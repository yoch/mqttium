#!/usr/bin/env python3
"""Reception of R1: recompute refuses a qualifying result whose conditions no longer hold.

Run from the mqttiumd directory, on a qualifying campaign written by
tools/run_qualifying_campaign.sh:

    python3 tools/reproduce_recompute.py <campaign> <new empty directory> \
        [--modified <campaign of a modified tree>] [--result qualifying-run-result.json] \
        [--oracle <mqttiumd checkout whose oracle runs>]

Each case is an independent copy of a campaign under the new directory,
with one defect injected and nothing else changed. For each, the oracle of
this checkout runs `recompute` on the qualifying result, then `produce` on
the same campaign, and the script prints one JSON line:

- intact: nothing changed; recompute stands (0);
- source-missing: source.tar removed; recompute refuses (2);
- source-corrupt: the first byte of source.tar XOR 1; refuses (2);
- oracle-mismatch: the oracle that runs is a copy of this one with one
  byte appended to judge.py, so its files no longer are those the
  manifest names; refuses (2);
- kept-untracked-missing and kept-patch-missing: on the campaign of a
  modified tree (--modified), one kept file removed; refuses (2). Without
  --modified these cases are reported as not run.

A refused case must name its defect, and only it: every diagnostic is
`result-not-qualifying` and names the condition the case breaks. The
script exits 0 when every case run behaves so, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parents[1]
ORACLE = pathlib.Path("tests/oracle/mqttiumd_oracle")
REQUIREMENTS = pathlib.Path("tests/oracle/requirements.txt")


def oracle(oracle_root, campaign, *arguments):
    """Runs the oracle found under ``oracle_root`` on ``campaign``."""
    completed = subprocess.run(
        [sys.executable, "-m", "mqttiumd_oracle", "--root", str(campaign), *arguments],
        cwd=campaign,
        env={"PYTHONPATH": str(oracle_root / "tests/oracle"), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout, completed.stderr


def diagnostics(stderr):
    return [line[len("refused ") :] for line in stderr.splitlines() if line.startswith("refused ")]


def remove(path):
    def inject(campaign, _checkout):
        (campaign / path).unlink()

    return inject


def corrupt(campaign, _checkout):
    data = bytearray((campaign / "source.tar").read_bytes())
    data[0] ^= 1
    (campaign / "source.tar").write_bytes(bytes(data))


def other_oracle(base):
    def inject(_campaign, checkout):
        shutil.copytree(
            base / ORACLE, checkout / ORACLE, ignore=shutil.ignore_patterns("__pycache__")
        )
        shutil.copy(base / REQUIREMENTS, checkout / REQUIREMENTS)
        with (checkout / ORACLE / "judge.py").open("ab") as stream:
            stream.write(b"\n")

    return inject


def first_untracked(campaign):
    kept = campaign / "modified/untracked"
    files = sorted(path for path in kept.rglob("*") if path.is_file())
    return files[0].relative_to(campaign) if files else None


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("campaign", type=pathlib.Path)
    parser.add_argument("work", type=pathlib.Path)
    parser.add_argument("--modified", type=pathlib.Path)
    parser.add_argument("--result", default="qualifying-run-result.json")
    parser.add_argument(
        "--oracle",
        type=pathlib.Path,
        default=HERE,
        help="the mqttiumd checkout whose oracle runs (default: this one)",
    )
    options = parser.parse_args()
    if options.work.exists() and any(options.work.iterdir()):
        parser.error(f"{options.work} is not empty")

    # (case, campaign, defect, words its diagnostic names, expected recompute exit)
    cases = [
        ("intact", options.campaign, None, None, 0),
        ("source-missing", options.campaign, remove("source.tar"), "is not in the campaign", 2),
        ("source-corrupt", options.campaign, corrupt, "its digest or its size differs", 2),
        (
            "oracle-mismatch",
            options.campaign,
            other_oracle(options.oracle),
            "the oracle that judged is not the one the manifest identifies: "
            "tests/oracle/mqttiumd_oracle/judge.py",
            2,
        ),
    ]
    if options.modified is not None:
        untracked = first_untracked(options.modified)
        if untracked is not None:
            path = untracked.relative_to("modified/untracked").as_posix()
            cases.append(
                (
                    "kept-untracked-missing",
                    options.modified,
                    remove(untracked),
                    f"path {path} is not kept",
                    2,
                )
            )
        if (options.modified / "modified/tracked.patch").is_file():
            cases.append(
                (
                    "kept-patch-missing",
                    options.modified,
                    remove("modified/tracked.patch"),
                    "is not kept",
                    2,
                )
            )

    scenario = json.loads((options.campaign / "run-manifest.json").read_text())["scenario"]["path"]
    failures = 0
    for name, source, defect, words, expected in cases:
        campaign = options.work / name / "campaign"
        checkout = options.work / name / "checkout"
        shutil.copytree(source, campaign)
        oracle_root = options.oracle
        if defect is not None:
            defect(campaign, checkout)
            if (checkout / ORACLE).is_dir():
                oracle_root = checkout
        code, out, err = oracle(
            oracle_root,
            campaign,
            "--contracts",
            "contracts/v1",
            "recompute",
            "--result",
            options.result,
        )
        found = diagnostics(err)
        named = [
            diagnostic
            for diagnostic in found
            if diagnostic.startswith("result-not-qualifying at /manifest")
            and (words is None or words in diagnostic)
        ]
        produced, _, _ = oracle(
            oracle_root,
            campaign,
            "--contracts",
            "contracts/v1",
            "produce",
            "--scenario",
            source_scenario(source, scenario),
            "--trace",
            "trace.jsonl",
            "--manifest",
            "run-manifest.json",
            "--out",
            "reproduced-run-result.json",
            "--scope",
            f"reception of R1, case {name}",
        )
        qualification = json.loads((campaign / "reproduced-run-result.json").read_text())
        ok = code == expected and (
            expected == 0 and not found or found and len(named) == len(found)
        )
        failures += not ok
        print(
            json.dumps(
                {
                    "case": name,
                    "recompute": {"exit": code, "stdout": out.strip(), "diagnostics": found},
                    "produce": {
                        "exit": produced,
                        "qualification": qualification["qualification"],
                        "verdict": qualification["verdict"],
                        "unmet": [
                            r for r in qualification["reasons"] if r.startswith("not qualifying: ")
                        ],
                    },
                    "as_expected": ok,
                }
            )
        )
    for name in ("kept-untracked-missing", "kept-patch-missing"):
        if not any(case[0] == name for case in cases):
            print(
                json.dumps(
                    {"case": name, "run": False, "why": "no campaign of a modified tree keeps it"}
                )
            )
    return 1 if failures else 0


def source_scenario(campaign, default):
    manifest = json.loads((campaign / "run-manifest.json").read_text())
    return manifest.get("scenario", {}).get("path", default)


if __name__ == "__main__":
    sys.exit(main())
