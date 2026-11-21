#!/usr/bin/env python3
"""Recompute the original pilot-split/short LIVE measurement from its saved observations."""
import argparse
import hashlib
import json
from pathlib import Path
from zipfile import ZipFile


CASE = "pilot-split/short"
FILES = ("reception.json", "live-dashboard-1.jsonl")
LIMIT = 8 * 1024 * 1024
P99_LIMIT_NS = 250_000_000


def input_files(path, expected_digest):
    inputs = {}
    if expected_digest:
        with path.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != expected_digest:
                raise ValueError("short split evidence ZIP digest mismatch")
            stream.seek(0)
            with ZipFile(stream) as archive:
                for name in FILES:
                    entries = [info for info in archive.infolist() if info.filename == f"{CASE}/{name}"]
                    if entries:
                        if len(entries) != 1 or entries[0].file_size > LIMIT:
                            raise ValueError("ambiguous or oversized short split evidence")
                        inputs[name] = archive.read(entries[0])
    else:
        for name in FILES:
            try:
                with (path / name).open("rb") as source:
                    inputs[name] = source.read(LIMIT + 1)
            except FileNotFoundError:
                continue
            if len(inputs[name]) > LIMIT:
                raise ValueError("oversized short split evidence")
    return inputs


def event_times(trace, window, port):
    seen = set()
    for line in trace.splitlines():
        row = json.loads(line)
        if row["kind"] != "live_received":
            continue
        event = row["event_id"]
        if (event in seen or row["qos"] != 0 or row["retain"] is not False
                or not row["topic"].endswith("/telemetry/live") or (port is not None and row["port"] != port)):
            raise ValueError("duplicate LIVE observation or publication from the wrong plane")
        seen.add(event)
        created, received, tick = (row[key] for key in ("created_ns", "received_ns", "monotonic_ns"))
        yield dict(event_id=event, created_ns=created, received_ns=received,
                   monotonic_ns=tick, end_to_end_ns=received - created,
                   in_outage_window=window[0] <= tick <= window[1] if window else None)


def diagnose(inputs, archive_digest):
    receipt = json.loads(inputs.get("reception.json", b"{}"))
    build = receipt.get("broker_build", {})
    summary = {
        "case": CASE, "campaign": "original_sealed_reception", "source_artifact_sha256": archive_digest,
        "input_sha256": {name: hashlib.sha256(raw).hexdigest() for name, raw in inputs.items()},
        "broker": {key: build.get(key) for key in ("source_sha", "binary_sha256")},
        "application_source": receipt.get("application_source"), "reception_status": receipt.get("status"),
        "reception_failure": receipt.get("failure"), "p99_limit_ms": P99_LIMIT_NS / 1e6,
        "unavailable_per_event_stages": ["outbox_sqlite_commit", "wait_before_live_publish",
                                          "live_publish_call", "subscriber_callback_entry"],
        "missing_evidence": [name for name in FILES if name not in inputs],
    }
    window = None
    if receipt:
        configuration = receipt["parameters"]
        if (configuration["outage_s"], configuration["outage_events"], configuration["rate"]) != (10, 500, 50):
            raise ValueError("expected the original ten-second, 500-event split case")
        schedule = receipt.get("schedule", {})
        if {"start_ns", "outage_boundary_ns"} <= schedule.keys():
            window = [schedule["start_ns"], schedule["outage_boundary_ns"]]
            if window[1] - window[0] != 10_000_000_000:
                raise ValueError("unexpected short split outage window")
    rows = list(event_times(inputs.get("live-dashboard-1.jsonl", b""), window,
                            receipt.get("planes", {}).get("live", {}).get("port")))
    summary.update(window_monotonic_ns=window, observed_live_arrivals=len(rows))
    if window is None:
        summary["missing_evidence"].append("outage_schedule")
    recorded = receipt.get("live_delivery")
    if recorded is None:
        summary["missing_evidence"].append("reception_live_delivery")
    if summary["missing_evidence"]:
        summary["status"] = "incomplete"
        return summary, rows
    delays = sorted(row["end_to_end_ns"] for row in rows if row["in_outage_window"])
    p99 = delays[(99 * len(delays) + 99) // 100 - 1] if delays else None
    p99_ms = p99 / 1e6 if p99 is not None else None
    if (len(rows), len(delays), p99_ms) != (recorded["received"], recorded["interval_arrivals"],
                                             recorded["creation_to_live"].get("p99_ms")):
        raise ValueError("observed LIVE times do not reproduce the original reception")
    status = "no_arrivals" if not delays else "invalid_clock" if delays[0] < 0 else (
        "above_limit" if p99 > P99_LIMIT_NS else "within_limit")
    summary.update(status=status, arrivals_in_window=len(delays), p99_ms=p99_ms,
                   eventual_outage_events=recorded["outage_events_eventually_seen_live"],
                   expected_outage_events=recorded["outage_expected"])
    return summary, rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path, help="pilot-split/short directory or verified GitHub artifact ZIP")
    parser.add_argument("--artifact-sha256", help="independent digest required for ZIP input")
    parser.add_argument("--out", type=Path, required=True, help="new directory for the summary and per-event observations")
    args = parser.parse_args()
    if args.evidence.is_file() and not args.artifact_sha256:
        parser.error("ZIP input requires --artifact-sha256")
    summary, rows = diagnose(input_files(args.evidence, args.artifact_sha256), args.artifact_sha256)
    args.out.mkdir(mode=0o700)
    (args.out / "timing.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (args.out / "events.jsonl").open("x") as observations:
        for row in rows:
            observations.write(json.dumps(row) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
