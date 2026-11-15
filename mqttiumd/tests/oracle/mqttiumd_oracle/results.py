"""Producing a qualifying result, and recomputing one that is offered.

A qualifying result is evidence. It is produced here, from the observations
of a run, and one that is offered is found again from the files it cites: a
result whose statuses or injection counts differ from the recomputation is
refused. The verdict of a result is never taken from the result.
"""

from __future__ import annotations

import hashlib

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
    with (contracts.root / result["trace"]["path"]).open("rb") as stream:
        if hashlib.sha256(stream.read()).hexdigest() != result["trace"]["sha256"]:
            return [
                Diagnostic("evidence-mismatch", "/trace", "the trace changed while it was read")
            ]
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
