"""Producing a result, qualifying only with its evidence, and recomputing one that is offered.

A qualifying result is evidence. It is produced here, from the observations
of a run, and one that is offered is found again from the files it cites: a
result whose statuses or injection counts differ from the recomputation is
refused. The verdict of a result is never taken from the result.
"""

from __future__ import annotations

import hashlib
import pathlib


from . import judge
from .semantic import verdict_of
from .strict import Diagnostic, Refused, parse

EXIT_CODES = {"PASS": 0, "FAIL": 1, "INCONCLUSIVE": 2, "OUT_OF_SCOPE": 3}
MAX_TEXT = 1024
MAX_REASONS = 256


def _cite(contracts, path):
    digest, size = contracts.hash_evidence(path)
    return {"path": path, "sha256": digest, "bytes": str(size)}


def _text(value: str) -> str:
    return value if len(value) <= MAX_TEXT else value[: MAX_TEXT - 1] + "…"


def _observed(run):
    """Assertion statuses and injection counts, as the observations determine them."""
    statuses = judge.judge(run)
    hits = judge.fault_hits(run)
    faults = [
        {
            "id": fault["id"],
            "expected_hits": fault["expected_hits"],
            "hits": str(hits.get(fault["id"], 0)),
        }
        for fault in run.scenario["faults"]
    ]
    return statuses, faults


def produce(contracts, scenario_path, trace_path, manifest_path, scope):
    """A qualifying result for the run a trace records."""
    run = judge.Run.load(contracts, scenario_path, trace_path)
    statuses, faults = _observed(run)
    reached = all(fault["hits"] == fault["expected_hits"] for fault in faults)
    verdict = verdict_of([status.status for status in statuses.values()], reached)
    reasons = [
        _text(f"{name}: {reason}")
        for name, status in statuses.items()
        if status.status != "PASS"
        for reason in status.reasons
    ]
    return {
        "format": "mqttiumd.run-result",
        "schema_version": 1,
        "qualification": "qualifying",
        "run_id": run.trace.run_id or "unreadable-trace",
        "verdict": verdict,
        "exit_code": EXIT_CODES[verdict],
        "scope": scope,
        "reasons": reasons[:MAX_REASONS],
        "assertions": [
            {"name": name, "status": status.status} for name, status in statuses.items()
        ],
        "faults": faults,
        "scenario": _cite(contracts, scenario_path),
        "manifest": _cite(contracts, manifest_path),
        "trace": _cite(contracts, trace_path),
        "contracts_digest": contracts.digest,
        "artifacts": [],
    }


# The condition of a qualifying run that no file given to the command can
# establish yet (docs/reception/open-conditions.md, lot L4).
UNTIED_BINARY = (
    "nothing establishes that the binary was built from the recorded content "
    "(no member of run-manifest v1 says so)"
)


def unmet(contracts, scenario_path, manifest_path):
    """The conditions of a qualifying run that the given files do not establish."""
    try:
        data = contracts.read(manifest_path)
    except Refused as refusal:
        return [f"the manifest cannot be read: {refusal.diagnostic}"]
    problems = contracts.check_bytes(data)
    if problems:
        return [f"the manifest is not a valid document: {problem}" for problem in problems]
    manifest = parse(data)
    if manifest["format"] != "mqttiumd.run-manifest":
        return ["the manifest is not a run manifest"]
    out = []
    if manifest["contracts_digest"] != contracts.digest:
        out.append("the manifest names other contracts than the ones in use")
    out += _cited(contracts, manifest.get("scenario"), scenario_path, "scenario")
    try:
        fixture = parse(contracts.read(scenario_path)).get("initial_fixture")
    except Refused as refusal:
        return [*out, f"the scenario cannot be read: {refusal.diagnostic}"]
    if fixture is not None:
        out += _cited(contracts, manifest.get("fixture"), fixture, "fixture")
    for member, words in (
        ("backend", "backend"),
        ("failure_model", "failure model"),
        ("scheduler", "scheduler"),
    ):
        if member not in manifest:
            out.append(f"the manifest records no {words}")
    if not manifest.get("commands"):
        out.append("the manifest records no command")
    for member in ("oracle", "generators"):
        if member not in manifest:
            out.append(f"the manifest does not identify the {member}")
    if "oracle" in manifest:
        out += _oracle_unmet(manifest["oracle"]["files"])
    if manifest["checkout"]["tree_state"] != "clean":
        out += _kept_unmet(contracts, manifest["checkout"]["modified_paths"])
    out.append(UNTIED_BINARY)
    return out


# The checkout directory the bench names files from (``mqttiumd``), and the
# package of the oracle that runs now, below it.
CHECKOUT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = pathlib.Path(__file__).resolve().parent


def _oracle_unmet(listed):
    """Whether the files the manifest lists are those of the oracle that runs.

    The oracle that judges is this one: its files are read from where it
    runs, not from the campaign, and each module of its package must be
    listed with its digest.
    """
    out = []
    named = {entry["path"] for entry in listed}
    for path in sorted(PACKAGE.glob("*.py")):
        relative = path.relative_to(CHECKOUT).as_posix()
        if relative not in named:
            out.append(f"the manifest does not list the oracle file {relative}")
    for entry in listed:
        path = CHECKOUT / entry["path"]
        try:
            inside = path.resolve(strict=True).is_relative_to(CHECKOUT)
            sha256 = hashlib.sha256(path.read_bytes()).hexdigest() if inside else None
        except OSError:
            sha256 = None
        if sha256 != entry["sha256"]:
            out.append(
                f"the oracle that judged is not the one the manifest identifies: {entry['path']}"
            )
    return out


# A header of a diff is short; any longer line is the content of a diff.
MAX_HEADER_BYTES = 8192


def _headers(stream):
    """The beginnings of the lines of ``stream``: a line longer than the bound is skipped whole."""
    while line := stream.readline(MAX_HEADER_BYTES + 1):
        if len(line) > MAX_HEADER_BYTES and not line.endswith(b"\n"):
            while (rest := stream.readline(MAX_HEADER_BYTES)) and not rest.endswith(b"\n"):
                pass
            continue
        yield line


def _kept_unmet(contracts, modified):
    """The modified paths the campaign does not keep, as the bench keeps them.

    An untracked path is kept as ``modified/untracked/<path>``; a tracked
    one, by its diff in ``modified/tracked.patch``.
    """
    diffed = set()
    patch = contracts.root / "modified/tracked.patch"
    if patch.is_file() and patch.resolve().is_relative_to(contracts.root):
        # A patch has no bound, a binary one has long lines: it is read by
        # bounded lines, and only the headers of its diffs are kept.
        with patch.open("rb") as stream:
            for line in _headers(stream):
                if line.startswith(b"diff --git a/"):
                    head = line.rstrip(b"\n")[len(b"diff --git a/") :]
                    diffed.add(head.split(b" b/", 1)[0].decode("utf-8", "replace"))
    out = []
    for path in modified:
        if path in diffed:
            continue
        try:
            contracts.hash_evidence(f"modified/untracked/{path}")
        except Refused:
            out.append(f"the modified path {path} is not kept in the run directory")
    return out


def _cited(contracts, cited, path, words):
    if cited is None:
        return [f"the manifest names no {words}"]
    try:
        sha256, _ = contracts.hash_evidence(path)
    except Refused as refusal:
        return [f"the {words} cannot be read: {refusal.diagnostic}"]
    if cited["sha256"] != sha256:
        return [f"the manifest names another {words} than {path}"]
    return []


def provisional(result, conditions):
    """``result`` as a development document: no evidence, so never a PASS."""
    out = dict(result, qualification="provisional")
    verdict = "INCONCLUSIVE" if result["verdict"] == "PASS" else result["verdict"]
    out.update(verdict=verdict, exit_code=EXIT_CODES[verdict])
    reasons = [_text(f"not qualifying: {condition}") for condition in conditions]
    out["reasons"] = (reasons + result["reasons"])[:MAX_REASONS]
    return out


def recompute(contracts, result_path):
    """The reasons a result is refused as evidence; empty when it stands."""
    try:
        data = contracts.read(result_path)
    except Refused as refusal:
        return [refusal.diagnostic]
    problems = contracts.check_bytes(data)
    if problems:
        return problems
    result = parse(data)
    if result["format"] != "mqttiumd.run-result":
        return [Diagnostic("reference-invalid", "/format", "not a run result")]
    if result["qualification"] != "qualifying":
        return [
            Diagnostic(
                "result-not-qualifying",
                "/qualification",
                "a provisional result names no evidence to recompute it from",
            )
        ]
    # The document was just checked against the digests it cites: the files
    # read now are the files it names, unless they changed in between, which
    # the digests of the run below would not hide either.
    try:
        run = judge.Run.load(contracts, result["scenario"]["path"], result["trace"]["path"])
    except Refused as refusal:
        return [refusal.diagnostic]
    try:
        sha256, _ = contracts.hash_evidence(result["trace"]["path"])
    except Refused as refusal:
        return [refusal.diagnostic]
    if sha256 != result["trace"]["sha256"]:
        return [Diagnostic("evidence-mismatch", "/trace", "the trace changed while it was read")]
    out = []
    if run.trace.run_id is not None and result["run_id"] != run.trace.run_id:
        out.append(
            Diagnostic(
                "evidence-mismatch",
                "/run_id",
                f"the cited trace is the run {run.trace.run_id}",
            )
        )
    statuses, faults = _observed(run)
    for index, entry in enumerate(result["assertions"]):
        found = statuses.get(entry["name"])
        if found is None or found.status != entry["status"]:
            out.append(
                Diagnostic(
                    "result-not-recomputed",
                    f"/assertions/{index}/status",
                    f"{entry['name']} is {entry['status']} in the result and "
                    f"{found.status if found else 'absent'} in the observations: "
                    f"{'; '.join(found.reasons) if found else ''}"[:MAX_TEXT],
                )
            )
    counted = {fault["id"]: fault["hits"] for fault in faults}
    for index, entry in enumerate(result["faults"]):
        if counted.get(entry["id"]) != entry["hits"]:
            out.append(
                Diagnostic(
                    "result-not-recomputed",
                    f"/faults/{index}/hits",
                    f"{entry['id']} was reached {counted.get(entry['id'])} times in the trace",
                )
            )
    return out
