#!/usr/bin/env python3
"""Bind the operated-site decision to observed artifacts and frozen files."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


PILOT = Path(__file__).resolve().parents[1] / "preview" / "pilot"
BINARY = "9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119"


def sha(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def need(condition, failure):
    if not condition:
        raise ValueError(failure)


def summarize(root):
    names = {"endurance": "received-endurance/result.json", "dense_archive": "archive-scale/result.json",
             "segment_ceiling": "catalog-full/result.json", "guards": "guard-green/result.json",
             "pressure": "pressure-green/result.json", "storage": "storage/result.json",
             "demo_certificate_horizon": "certificate-horizon.json", "sealed_reception": "package-rc-check/site/result.json"}
    files = {key: root / name for key, name in names.items()}
    inputs = {key: json.loads(path.read_text()) for key, path in files.items()}
    need(all(row["status"] == "PASS" for name, row in inputs.items() if name != "dense_archive"),
         "missing_positive_reception")
    e = inputs["endurance"]
    source = e["application_source"]
    need(len(source) == 40 and e["broker"]["binary_sha256"] == BINARY, "wrong_endurance_binary_or_source")
    need(e["component_sha256"] == inputs["sealed_reception"]["component_sha256"],
         "sealed_and_endurance_components_differ")
    for key in ("endurance", "dense_archive", "segment_ceiling"):
        for name, digest in inputs[key]["component_sha256"].items():
            need(sha(PILOT / name) == digest, "measured_component_changed:" + name)
    need(sha(PILOT / "storage_check.py") == inputs["storage"]["tool_sha256"], "storage_probe_changed")
    phases = {phase["phase"]: phase for phase in e["phases"]}
    saturated = phases["history_saturated"]
    catch = phases["history_caught_up"]
    old = e["local_backup"]
    need(saturated["sustained_s"] >= 1800 and catch["net_per_s"] > 0 and old["pending_at_backup"] > 200,
         "expected_saturation_and_resume_not_observed")
    for value in old["copy_result"]["counts"].values():
        need(value == old["final_events"], "incomplete_restored_dataset")
    trace = root / "received-endurance/samples.jsonl"
    count = 0
    peak = {"charged_archive_bytes": 0, "hot_rows": 0, "outbox_bytes": 0, "history_references": 0,
            "archive_segments_per_receiver": 0}
    free = None
    first_ns, last_ns = None, None
    with trace.open("r") as stream:
        for line in stream:
            row = json.loads(line)
            metrics = row["metrics"]
            count += 1
            first_ns = row["monotonic_ns"] if first_ns is None else first_ns
            last_ns = row["monotonic_ns"]
            available = metrics["disk"]["available_bytes"]
            free = available if free is None else min(free, available)
            for lane in metrics["brokers"].values():
                need(lane["invariants_ok"] and lane["limits"]["references"] == 128, "broker_shape_changed")
            peak["history_references"] = max(peak["history_references"], metrics["brokers"]["history"]["accounts"]["references"])
            for trend in metrics["history"]["ledgers"].values():
                need(trend["hot_count"] <= trend["hot_limit"] and trend["archive_charge"] <= trend["archive_budget"]
                     and trend["segment_count"] <= trend["segment_limit"] and not trend["conflicts"],
                     "archive_budget_or_identity_fault")
                peak["hot_rows"] = max(peak["hot_rows"], trend["hot_count"])
                peak["charged_archive_bytes"] = max(peak["charged_archive_bytes"], trend["archive_charge"])
                peak["archive_segments_per_receiver"] = max(peak["archive_segments_per_receiver"], trend["segment_count"])
            for trend in metrics["history"]["sources"].values():
                need(trend["occupied_bytes"] <= trend["budget"] and trend["attempts"] >= trend["confirmed"], "outbox_budget_or_receipt_counter")
                peak["outbox_bytes"] = max(peak["outbox_bytes"], trend["occupied_bytes"])
    need(count > 360 and peak["history_references"] >= 127 and first_ns < last_ns, "incomplete_sample_window")
    large = inputs["dense_archive"]
    need(large["dense"]["state"]["archived_count"] == 2_097_152
         and large["restored"]["manifest"]["unique_count"] == 2_097_152 and len(large["corruption"]) == 2
         and "archive_scratch_capacity_floor" in (root / "archive-scale.log").read_text(), "dense_volume_coverage_incomplete")
    limit = inputs["segment_ceiling"]["ceiling"]
    need(limit["state"]["segment_count"] == 32768 and limit["state"]["hot_count"] == 1,
         "catalog_limit_not_reached")
    evidence_paths = [*files.values(), trace, root / "received-endurance/site-backup-manifest.json",
                      root / "archive-scale.log", root / "pressure-red.log", root / "guard-red.log",
                      root / "python-gates.log", root / "rust-gates.log"]
    storage = inputs["storage"]
    met = e["normal_metrics"]
    expected_restarts = {"broker-live", "broker-history", "history-dashboard-1", "history-dashboard-2", "capteur-1"}
    unexpected = {role: counts["launches"] - 1 - int(role in expected_restarts)
                  for role, counts in phases["all_responsibilities_drained"]["actors"].items()
                  if counts["launches"] - 1 > int(role in expected_restarts)}
    outcome = {
        "format": "mqttiumd.private-site.qualification.v1",
        "broker_binary_sha256": BINARY, "broker_source_sha": e["broker"]["source_sha"],
        "application_source": source, "component_sha256": e["component_sha256"],
        "platform": e["platform"], "hardware_qualification": storage["physical_hardware_qualification"],
        "qualification": {
            "bounded_private_reference_pilot": "GO_WITH_RESTRICTIONS",
            "continuous_72_hours": "NOT_RECEIVED",
            "strong_real_time": "NO_GO",
            "recovery_without_original_authority": "NOT_RECEIVED",
        },
        "observed_generation_s": e["observed_generation_s"], "phases": e["phases"],
        "observations": count, "minimum_available_bytes": free, "peaks": peak,
        "application_budgets": e["application_budgets"], "final_primary_live": met["live"],
        "unknown_after_incidents": sum(row["unknown"] for row in met["history"]["sources"].values()),
        "extra_primary_restarts_by_role": unexpected,
        "process_peaks_by_pid": e["process_observations"],
        "authorized_role_guard": e["auth_refusal_both_lanes"], "charged_backup": old,
        "health_errors": inputs["guards"],
        "outbox_pressure": inputs["pressure"],
        "dense_archive": {**large["dense"], "restore_s": large["restore_s"], "export_s": large["export_s"],
                          "coverage": "dense/verify/dedup/export/restore/corruption completed; combined catalog build stopped at free-space floor",
                          "corruption": large["corruption"], "restored": large["restored"]},
        "segment_ceiling": limit, "storage_check": storage,
        "demo_certificate_horizon": inputs["demo_certificate_horizon"],
        "sha256": {str(path.relative_to(root)): sha(path) for path in evidence_paths},
    }
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    result = summarize(args.evidence)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
