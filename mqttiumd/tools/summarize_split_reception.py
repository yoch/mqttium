#!/usr/bin/env python3
"""Recheck split-pilot traces and emit a small source-bound qualification record."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BROKER_SHA = "9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119"


def sha(path):
    with path.open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def read(path):
    return json.loads(path.read_text())


def records(path):
    with path.open() as lines:
        for line in lines:
            yield json.loads(line)


def trace(path, end_state, start=None, end=None, saturation=None):
    latest, seen = {}, {}
    gaps = 0
    prior = saturation
    gap_ns = 0
    times = []
    for row in records(path):
        if row["kind"] != "live_received":
            continue
        name, stream, sequence = row["event_id"].split("/")
        n = int(sequence)
        require(row["topic"] == f"atelier/{name}/telemetry/live" and row["qos"] == 0
                and not row["retain"], "live_observation_from_wrong_topic_or_qos")
        key = (name, stream)
        high = latest.get(name, (stream, 0, None))
        require(high[0] == stream, "fixture_stream_changed")
        require(row["display_updated"] == (n > high[1]), "current_live_value_regressed")
        if n > high[1]:
            latest[name] = (stream, n, row["digest"])
            gaps += n - high[1] - 1
        require((key, n) not in seen, "duplicate_live_fixture_identity")
        seen[key, n] = row["digest"]
        timestamp = row["monotonic_ns"]
        if start is not None and start <= timestamp <= end:
            times.append(row["received_ns"] - row["created_ns"])
        if prior is not None and saturation <= timestamp <= end:
            require(timestamp >= prior, "live_trace_reordered")
            gap_ns = max(gap_ns, timestamp - prior)
            prior = timestamp
    latest_decoded = {sensor: dict(stream=t, seq=n, digest=d) for sensor, (t, n, d) in latest.items()}
    require(latest_decoded == end_state, "display_and_authenticated_trace_differ")
    if prior is not None:
        gap_ns = max(gap_ns, end - prior)
    return seen, gaps, gap_ns, times


def recovery_freshness(path, boundary):
    delays = []
    previous = None
    spacing_ns = 0
    for row in records(path):
        if row["kind"] != "live_received" or row["monotonic_ns"] < boundary:
            continue
        arrival = row["monotonic_ns"]
        if previous is not None:
            spacing_ns = max(spacing_ns, arrival - previous)
        previous = arrival
        delays.append(row["received_ns"] - row["created_ns"])
    require(delays and min(delays) >= 0, "live_recovery_time_sample_unavailable")
    delays.sort()
    ranks = {"p50_ms": .50, "p95_ms": .95, "p99_ms": .99, "max_ms": 1.0}
    return {"samples": len(delays), "largest_interarrival_ms": spacing_ns / 1e6, **{
        label: delays[math.ceil(rank * len(delays)) - 1] / 1e6 for label, rank in ranks.items()
    }}


def evidence(args):
    out = args.qualification
    paths = {
        "short": out / args.short / "reception.json", "decisive": out / "decisive/reception.json",
        "archive": out / args.segments / "result.json", "incidents": out / args.incidents / "result.json",
        "controls": out / args.controls / "result.json", "old_ledger": out / args.legacy / "result.json",
    }
    inputs = {name: read(path) for name, path in paths.items()}
    require(all(r["status"] == "PASS" for r in inputs.values()), "qualification_input_not_passed")
    run = inputs["decisive"]
    package = ROOT / "preview/pilot"
    require(all(sha(package / name) == fingerprint for name, fingerprint in run["application_files"].items())
            and run["broker_build"]["binary_sha256"] == BROKER_SHA
            and len(run["application_source"]) == 40, "measured_implementation_does_not_match")
    schedule = run["schedule"]
    trace_path = out / "decisive/live-dashboard-1.jsonl"
    seen, exposed_holes, gap_ns, times = trace(trace_path, run["live_final"]["latest"], schedule["start_ns"],
                                              schedule["outage_boundary_ns"], schedule["history_saturation_observed_ns"])
    times.sort()
    require(len(times) == run["live_delivery"]["interval_arrivals"]
            and times[math.ceil(.99 * len(times)) - 1] / 1_000_000 == run["live_delivery"]["creation_to_live"]["p99_ms"]
            and gap_ns / 1e9 == run["live_delivery"]["max_gap_in_saturation_s"], "live_recomputation_differs")
    reconciled_path = out / "decisive/reconciliation.jsonl"
    event_count = 0
    missing = []
    for row in records(reconciled_path):
        sensor, stream, seq = row["id"].split("/")
        observed = seen.pop(((sensor, stream), int(seq)), None)
        require((observed == row["sha256"]) if row["live"] else observed is None, "reconciliation_trace_mismatch")
        if not row["live"]:
            missing.append(row["id"])
        event_count += 1
    require(event_count == run["expected_events"] and not seen
            and missing == run["live_delivery"]["missing"], "live_reconciliation_incomplete")
    case = inputs["incidents"]["reconciliation"]
    incident_seen, _, _, _ = trace(out / args.incidents / "live-dashboard-1.jsonl", case["latest_live_display"])
    require(len(incident_seen) + len(case["gaps_recovered"]) == case["all_events_on_each_history"],
            "incident_reconciliation_count_changed")
    state = run["final"]
    for lane, plane in run["planes"].items():
        require(plane["pid"] == run["broker_processes"][lane]["pid"]
                and plane["ready"]["max_connections"] == 16, "changed_process_or_connection_ceiling")
    sample_path = out / "decisive/samples.jsonl"
    live_peaks = Counter()
    history_peaks = Counter()
    frames = 0
    for row in records(sample_path):
        for lane, peak in (("live", live_peaks), ("history", history_peaks)):
            broker = row["brokers"][lane]
            require(broker["invariants_ok"] and broker["limits"]["references"] == 128, "sample_broker_quota_or_invariant")
            for key, value in broker["accounts"].items():
                peak[key] = max(peak[key], value)
        frames += 1
    historical = run["history_delivery"]
    require(run["outage"]["total"]["enqueued"] >= 90_000 and state["total"]["enqueued"] == event_count
            and historical["verified_on_each_dashboard"] == event_count, "final_application_counts")
    result = {
        "format": "mqttiumd.pilot.split-evidence.v1", "broker_source_sha": run["broker_build"]["source_sha"],
        "binary_sha256": BROKER_SHA, "application_source_sha": run["application_source"],
        "application_files": run["application_files"], "machine": run["machine"], "parameters": run["parameters"],
        "short": {"admission": inputs["short"]["outage"]["total"], "live": inputs["short"]["live_delivery"],
                  "reference_peak": inputs["short"]["history_reference_peak"]},
        "long": {"schedule": schedule, "last_sample_mono": state["monotonic_ns"],
                 "history_returned_mono": run["dashboard_return"]["monotonic_ns"],
                 "counts_at_outage_end": run["outage"]["total"], "final": state["total"],
                 "catchup": run["catchup"], "live": run["live_delivery"], "history": historical,
                 "live_after_outage": recovery_freshness(trace_path, schedule["outage_boundary_ns"]),
                 "sqlite": run["history_broker_events"]["runtime"]["sqlite_configuration"],
                 "history_commit_counts": {k: v for k, v in run["history_broker_events"]["counts"].items()
                                           if k in ("Admission_committed", "Finish_committed")},
                 "broker_resource_peaks": {"live": live_peaks, "history": history_peaks},
                 "plane_identity": run["planes"], "peaks": run["peaks"],
                 "recomputed_display_gaps": exposed_holes, "samples_checked": frames},
        "incidents": {"history_off_live_arrivals": inputs["incidents"]["history_restart"]["verified_live"],
                      "live_off_history_before": inputs["incidents"]["live_restart"]["history_counts_before"],
                      "live_off_history_after": inputs["incidents"]["live_restart"]["history_counts_after"],
                      "cut_event": case["fault_event_recovered"], "recovered_live_gaps": len(case["gaps_recovered"]),
                      "will_latency_ms": inputs["incidents"]["will_under_history_pressure"]["kill_to_receive_ms"],
                      "refusals": inputs["incidents"]["client_id_denied"],
                      "archived_repeat": inputs["incidents"]["archived_repeat"]["event_id"]},
        "archive": {"crashes": inputs["archive"]["crashes"], "budgets": inputs["archive"]["budgets"],
                    "ordering": inputs["archive"]["ordering"],
                    **{k: v for k, v in inputs["archive"]["rotations_and_restore"].items() if k != "rotations"},
                    "rotation_page_peak": max(r["page_count"] for r in inputs["archive"]["rotations_and_restore"]["rotations"]),
                    "rotation_freelist_peak": max(r["freelist_count"] for r in inputs["archive"]["rotations_and_restore"]["rotations"])},
        "old_ledger": inputs["old_ledger"],
        "mutations": inputs["controls"]["detected"],
        "evidence_sha256": {str(path.relative_to(out)): sha(path) for path in
                            [*paths.values(), trace_path, sample_path, reconciled_path]},
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("qualification", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--short", default="short")
    parser.add_argument("--segments", default="segments")
    parser.add_argument("--incidents", default="incidents")
    parser.add_argument("--controls", default="controls")
    parser.add_argument("--legacy", default="old-ledger")
    args = parser.parse_args()
    result = evidence(args)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"{args.output} {sha(args.output)}")


if __name__ == "__main__":
    main()
