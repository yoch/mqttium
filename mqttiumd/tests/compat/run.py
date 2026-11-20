#!/usr/bin/env python3
"""Play every compatibility scenario against the pinned Mosquitto and mqttiumd.

    python3 mqttiumd/tests/compat/run.py --reference DIR_OR_BINARY --broker BINARY
        [--out DIR] [--only GLOB ...] [--jobs N] [--list] [--verbose]
        [--update-fingerprints]

Each scenario is played against a fresh instance of each broker. The two
normalized transcripts are compared and the pair is joined with
`divergences.json`. The run writes `matrix.md`, `matrix.json` and the
transcripts in the output directory.

Exit status: 0 when every difference is classified and every classified
difference still reproduces as it was read; 1 otherwise; 2 for a wrong
command line or an unusable `divergences.json`. See README.md for what the
result does and does not establish.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import difflib
from fnmatch import fnmatchcase
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import traceback

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))

import classify  # noqa: E402
import dsl  # noqa: E402
import harness  # noqa: E402
import normalize  # noqa: E402
import scenarios  # noqa: E402,F401  (fills dsl.REGISTRY)

FORMAT = "mqttiumd-compat-matrix-v1"
RESULTS = (classify.SAME, *classify.CLASSES, classify.UNCLASSIFIED, classify.ERROR)
# What the broker is built from: the last commit that changed one of these
# names the content under test better than HEAD does.
BROKER_SOURCES = ("crates", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml")


def sha256(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def broker_commit() -> str:
    """The last commit that changed the broker's sources, as git reports it
    for the checkout this script is in; `unknown` outside a checkout."""
    root = HERE.parents[1]
    try:
        commit = subprocess.run(["git", "log", "-1", "--format=%H", "--", *BROKER_SOURCES], cwd=root, check=True,
                                capture_output=True, text=True, timeout=30).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--", *BROKER_SOURCES], cwd=root, check=True,
                               capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    if not commit:
        return "unknown"
    return commit + (" with uncommitted changes" if dirty else "")


def select(names, patterns):
    """Scenario names matching one of `patterns` (exact name or glob)."""
    if not patterns:
        return sorted(names)
    for pattern in patterns:
        if not any(fnmatchcase(name, pattern) for name in names):
            raise SystemExit(f"--only {pattern}: no such scenario")
    return sorted(name for name in names if any(fnmatchcase(name, pattern) for pattern in patterns))


def play(scenario, binaries: dict, scratch: Path) -> dict:
    """Both normalized transcripts of one scenario, or the error met."""
    out = {"name": scenario.name, "incidents": {}, "fingerprint": None}
    try:
        for backend in harness.BACKENDS:
            with tempfile.TemporaryDirectory(dir=scratch, prefix="compat-") as directory:
                raw, running = harness.run_scenario(scenario, backend, binaries[backend], Path(directory))
            out[backend] = normalize.normalize(raw, scenario)
            incident = running.incident()
            if incident:
                out["incidents"][backend] = {"what": incident, "log": running.log_tail,
                                             "events": running.events_tail}
        if normalize.same(out[harness.REFERENCE], out[harness.BROKER]):
            out["status"] = classify.SAME
        else:
            out["status"] = classify.DIFFERENT
            out["fingerprint"] = normalize.fingerprint(out[harness.REFERENCE], out[harness.BROKER])
    except Exception:  # noqa: BLE001 - recorded, and the run fails
        out["status"] = classify.ERROR
        out["error"] = traceback.format_exc()
    return out


def render_transcripts(result) -> str:
    """One step per line, so that two runs read well in a diff."""
    def block(key, transcript):
        rows = ",\n".join("    " + json.dumps(row, sort_keys=True, ensure_ascii=True) for row in transcript)
        return f'  {json.dumps(key)}: [\n{rows}\n  ]' if transcript else f'  {json.dumps(key)}: []'

    parts = [f'  "scenario": {json.dumps(result["name"])}', f'  "status": {json.dumps(result["status"])}']
    if result["status"] == classify.SAME:
        parts.append(block("both", result[harness.REFERENCE]))
    else:
        parts += [f'  "fingerprint": {json.dumps(result["fingerprint"])}',
                  block(harness.REFERENCE, result[harness.REFERENCE]), block(harness.BROKER, result[harness.BROKER])]
    return "{\n" + ",\n".join(parts) + "\n}\n"


def transcript_diff(result) -> str:
    def pretty(transcript):
        return [json.dumps(row, sort_keys=True) for row in transcript]

    return "\n".join(difflib.unified_diff(pretty(result[harness.REFERENCE]), pretty(result[harness.BROKER]),
                                          "mosquitto", "mqttiumd", lineterm="", n=2))


def summarize(rows) -> dict:
    by_result = {result: 0 for result in RESULTS}
    by_area = {area: {result: 0 for result in RESULTS} for area in dsl.AREAS}
    for row in rows:
        by_result[row["result"]] += 1
        by_area[row["area"]][row["result"]] += 1
    return {"by_result": by_result,
            "by_area": {area: counts for area, counts in by_area.items() if any(counts.values())}}


def cell(text: str) -> str:
    return text.replace("|", "\\|")


def summary_table(summary) -> list:
    columns = [result for result in RESULTS
               if summary["by_result"][result] or result in (classify.SAME, *classify.CLASSES)]
    lines = ["| Area | " + " | ".join(columns) + " | total |", "| - |" + " -: |" * (len(columns) + 1)]
    for area, counts in summary["by_area"].items():
        lines.append(f"| {area} | " + " | ".join(str(counts[column]) for column in columns)
                     + f" | {sum(counts.values())} |")
    lines.append("| **all** | " + " | ".join(str(summary["by_result"][column]) for column in columns)
                 + f" | {sum(summary['by_result'].values())} |")
    return lines


def markdown(matrix) -> str:
    rows = matrix["scenarios"]
    version = matrix["reference"]["version"]
    lines = [
        f"# Compatibility matrix: mqttiumd against Mosquitto {version}", "",
        "Written by `tests/compat/run.py`: regenerate it with the command in",
        "`tests/compat/README.md` and copy `matrix.md` here; do not edit it by hand.", "",
        f"- broker: `mqttiumd` built from commit `{matrix['broker']['commit']}`",
        f"- reference: Mosquitto {version}, the version `preview/build-mosquitto-reference.sh` builds",
        f"- scenarios played: {len(rows)}" + ("" if matrix["complete"] else " (a partial run)"), "",
        "`same` means equal normalized transcripts on the scenario played, nothing",
        "more. A difference is not a failure of mqttiumd by itself: Mosquitto is not",
        "the oracle, the MQTT specification is. Each difference is classified in",
        "`divergences.json`:", "",
    ]
    lines += [f"- `{name}`: {text};" for name, text in classify.CLASSES.items()]
    lines[-1] = lines[-1][:-1] + "."
    counts = matrix["summary"]["by_result"]
    totals = ", ".join(f"{counts[result]} `{result}`" for result in RESULTS if counts[result])
    lines += ["", "## Summary", "", f"{len(rows)} scenarios: {totals}.", "", *summary_table(matrix["summary"]), ""]
    if matrix["problems"]:
        lines += ["## Problems that fail the run", ""]
        lines += [f"- `{name}`: {problem}" for name, problem in matrix["problems"]]
        lines.append("")
    if matrix["incidents"]:
        lines += ["## Broker incidents (exit, hang or panic during a scenario)", ""]
        lines += [f"- `{item['scenario']}`, {item['backend']}: {item['what']}" for item in matrix["incidents"]]
        lines.append("")
    lines += ["## Scenarios", "", "| Scenario | MQTT | Result | Note |", "| - | - | - | - |"]
    for row in rows:
        note = row["difference"] or row["doc"]
        lines.append(f"| `{row['id']}` | {row['version']} | {row['result']} | {cell(note)} |")
    lines.append("")
    for name in (*classify.CLASSES, classify.UNCLASSIFIED):
        chosen = [row for row in rows if row["result"] == name]
        if not chosen:
            continue
        lines += [f"## {name} ({len(chosen)})", ""]
        for row in chosen:
            lines.append(f"- `{row['id']}` (MQTT {row['version']}): {row['difference'] or 'not classified'}")
            for label, key in (("Statement", "statement"), ("mqttiumd", "mqttiumd"), ("Lot", "lot")):
                if row.get(key):
                    lines.append(f"  - {label}: {row[key]}")
        lines.append("")
    return "\n".join(lines)


def update_fingerprints(path: Path, results) -> int:
    """Rewrite the fingerprint of every listed scenario that still differs."""
    entries = json.loads(path.read_text())
    seen = {result["name"]: result["fingerprint"] for result in results if result["status"] == classify.DIFFERENT}
    changed = 0
    for entry in entries:
        now = seen.get(entry.get("id"))
        if now and entry.get("fingerprint") != now:
            entry["fingerprint"] = now
            changed += 1
    if changed:
        path.write_text(json.dumps(entries, indent=2, ensure_ascii=False) + "\n")
    return changed


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--reference", type=Path,
                        help="the mosquitto executable, its build directory, or the output directory of "
                             "preview/build-mosquitto-reference.sh")
    parser.add_argument("--broker", type=Path, help="the mqttiumd executable")
    parser.add_argument("--out", type=Path, default=HERE.parents[1] / "target/compat",
                        help="directory for matrix.md, matrix.json and the transcripts")
    parser.add_argument("--only", nargs="+", action="extend", default=[], metavar="GLOB",
                        help="play the scenarios whose id matches one of these globs")
    parser.add_argument("--jobs", type=int, default=4, help="scenarios played at once (default 4)")
    parser.add_argument("--list", action="store_true", help="list the scenarios and exit")
    parser.add_argument("--verbose", "-v", action="store_true", help="print the diff of each differing pair")
    parser.add_argument("--update-fingerprints", action="store_true",
                        help="after reading the new differences, record them in divergences.json")
    args = parser.parse_args(argv)

    registry = dsl.REGISTRY
    names = select(registry, args.only)
    if args.list:
        for name in names:
            print(f"{name}  [MQTT {registry[name].version}]  {registry[name].doc}")
        return 0
    if args.reference is None or args.broker is None:
        parser.error("--reference and --broker are required")
    broker = args.broker.resolve()
    if not broker.is_file():
        parser.error(f"{broker} is not a file")
    try:
        reference = harness.reference_binary(args.reference)
        version = harness.reference_version(reference)
    except harness.BrokerError as error:
        parser.error(str(error))
    if version != harness.MOSQUITTO_VERSION:
        parser.error(f"the reference is Mosquitto {version}; divergences.json is written against "
                     f"{harness.MOSQUITTO_VERSION}")
    binaries = {harness.REFERENCE: reference, harness.BROKER: broker}
    divergences_path = HERE / "divergences.json"
    try:
        divergences = classify.load_divergences(divergences_path)
    except (classify.DivergenceError, json.JSONDecodeError) as error:
        print(f"divergences.json: {error}", file=sys.stderr)
        return 2

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="mqttiumd-compat-") as scratch:
        with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
            results = list(pool.map(lambda name: play(registry[name], binaries, Path(scratch)), names))
    elapsed = time.monotonic() - started

    if args.update_fingerprints:
        changed = update_fingerprints(divergences_path, results)
        print(f"divergences.json: {changed} fingerprint(s) rewritten")
        divergences = classify.load_divergences(divergences_path)

    observed = {result["name"]: (result["status"], result["fingerprint"]) for result in results}
    classes, problems = classify.judge(observed, divergences, set(registry))
    versions = {name: entry.version for name, entry in registry.items()}
    problems += classify.check_statements(divergences, versions, classify.known_statements(HERE.parents[2] / "docs/spec"))

    args.out.mkdir(parents=True, exist_ok=True)
    transcripts = args.out / "transcripts"
    rows, incidents = [], []
    for result in results:
        name = result["name"]
        scenario = registry[name]
        entry = divergences.get(name, {}) if result["status"] == classify.DIFFERENT else {}
        row = {"id": name, "area": scenario.area, "version": scenario.version, "doc": scenario.doc,
               "status": result["status"], "result": classes[name], "fingerprint": result["fingerprint"],
               "difference": entry.get("difference", ""), "statement": entry.get("statement", ""),
               "mqttiumd": entry.get("mqttiumd", ""), "lot": entry.get("lot", "")}
        rows.append(row)
        target = transcripts / (name + ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        if result["status"] == classify.ERROR:
            target.with_suffix(".error.txt").write_text(result["error"])
            print(f"ERROR {name}\n{result['error']}", file=sys.stderr)
            continue
        target.write_text(render_transcripts(result))
        if result["status"] == classify.DIFFERENT:
            diff = transcript_diff(result)
            target.with_suffix(".diff").write_text(diff + "\n")
            if args.verbose:
                print(f"--- {name} [{classes[name]}] fingerprint {result['fingerprint']}\n{diff}\n")
        for backend, incident in result["incidents"].items():
            incidents.append({"scenario": name, "backend": backend, "what": incident["what"]})
            target.with_suffix(f".{backend}.incident.txt").write_text(
                f"{incident['what']}\n\n-- output --\n{incident['log']}\n-- events --\n{incident['events']}\n")
            if classes[name] != "defect":
                problems.append((name, f"{backend} {incident['what']}, and the scenario is not listed as a defect"))

    problems.sort()
    summary = summarize(rows)
    matrix = {"format": FORMAT,
              "reference": {"version": version, "sha256": sha256(reference)},
              "broker": {"commit": broker_commit(), "sha256": sha256(broker)},
              "complete": not args.only, "scenario_count": len(rows), "summary": summary,
              "problems": [list(item) for item in problems], "incidents": incidents, "scenarios": rows}
    (args.out / "matrix.json").write_text(json.dumps(matrix, indent=1, sort_keys=True) + "\n")
    (args.out / "matrix.md").write_text(markdown(matrix))

    print("\n".join(summary_table(summary)))
    print(f"\n{len(rows)} scenarios in {elapsed:.1f} s; matrix in {args.out}")
    for item in incidents:
        print(f"INCIDENT {item['scenario']}: {item['backend']} {item['what']}")
    for name, problem in problems:
        print(f"PROBLEM {name}: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
