#!/usr/bin/env python3
"""Finite live receipt-collection system tests, not fixture qualification.

The historical ceiling case is derived from the supplied review probe. Its
binary and source identity are explicit; an old run is never relabeled as a
new implementation. A missing response counts as the known receipt ceiling
only when the exact full receipt table, bounded live state, unknown result
and independent control progress are observed together.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from pubsub_probe import PubsubWire, decode_filter_ack, durable, subscribe_packet
from tcp_probe import ReceiptFailure, require, write_json
from tcp_reception import Broker, writer_available


HISTORICAL_RECEIPT_LIMIT = 8192


def table_counts(path: Path) -> dict:
    state = durable(path)
    return {"schema_version": state["schema_version"], "epoch": state["epoch"],
            "revision": state["revision"], "counts": state["counts"]}


def replace_one(wire: PubsubWire, attempt: int, timeout: float) -> dict:
    """MID 1 is reused only after the preceding request's terminal SUBACK."""
    wire.send(subscribe_packet(1, [("reception/receipt-live", 1)]))
    frame = wire.poll(timeout)
    if frame is None:
        return {"attempt": attempt, "result": "no_frame", "observation_s": timeout}
    if frame.kind == 0x90:
        mid, codes = decode_filter_ack(frame)
        return {"attempt": attempt, "result": "suback", "identifier": mid, "codes": list(codes)}
    return {"attempt": attempt, "result": "other_frame", "header": frame.header,
            "body": frame.body[:128].hex()}


def control_progress(wire: PubsubWire) -> None:
    wire.send(b"\xc0\x00")
    wire.wait(0xD0, timeout=2)


def bounded_empty_business(snapshot: dict) -> bool:
    return snapshot["counts"]["routes"] == 1 and all(snapshot["counts"][name] == 0
             for name in ("publications", "payloads", "obligations", "outbound", "inbound"))


def historical_ceiling(binary: Path, case: Path, response_timeout: float = 12) -> dict:
    """A regression that fails semantically on the former cumulative limit."""
    broker = Broker(binary, case / "first")
    restarted = None
    result = {"criterion": "A-ceiling", "historical_receipt_limit": HISTORICAL_RECEIPT_LIMIT,
              "sql_mutations_by_probe": False, "constant_live_filter": True,
              "packet_identifier": 1, "positive_subacks": 0, "samples": [],
              "parameters": {"attempt_budget": HISTORICAL_RECEIPT_LIMIT + 1,
                             "keep_alive": 0, "response_timeout_s": response_timeout}}
    started = time.monotonic_ns()
    try:
        ready = broker.ready()
        result["effective_limits"] = ready["details"]
        require(ready["details"]["receipt_slots"] == HISTORICAL_RECEIPT_LIMIT,
                "historical_budget_changed")
        with PubsubWire(broker.address, broker.observations, "constant-live-filter") as wire:
            wire.connect("receipt-ceiling-client", keep_alive=0, session_present=False)
            result["initial"] = table_counts(broker.db)
            answer = None
            for attempt in range(1, HISTORICAL_RECEIPT_LIMIT + 2):
                answer = replace_one(wire, attempt, response_timeout)
                if answer.get("codes") != [1]:
                    break
                require(answer["identifier"] == 1, "subscription_identifier_changed")
                result["positive_subacks"] += 1
                if attempt % 1024 == 0:
                    sample = {"attempt": attempt, "elapsed_ms": (time.monotonic_ns() - started) / 1e6,
                              "snapshot": table_counts(broker.db)}
                    result["samples"].append(sample)
                    print(json.dumps({"kind": "ceiling_sample", **sample}), flush=True)
            result["first_nonpositive"] = answer if answer.get("codes") != [1] else None
            result["before_restart"] = table_counts(broker.db)
            if result["first_nonpositive"] is not None:
                control_progress(wire)
                result["ping_progress_at_pressure"] = True
                events = broker.events()
                replies = [event for event in events if event["kind"] == "store_result"]
                result["last_store_result"] = replies[-1] if replies else None
                full = result["before_restart"]["counts"]["operations"] == HISTORICAL_RECEIPT_LIMIT
                exact = (result["positive_subacks"] == HISTORICAL_RECEIPT_LIMIT
                         and answer["result"] == "no_frame" and full
                         and bounded_empty_business(result["before_restart"])
                         and replies and replies[-1]["details"]["outcome"] == "PendingOrUnknown")
                require(exact, "unexpected_business_response_failure", json.dumps(answer))
        inode = broker.db.stat().st_ino
        result["stop_status"] = broker.stop()
        writer_available(broker.db)
        restarted = Broker(binary, case / "restart", db=broker.db, label="restarted")
        restarted.ready()
        require(restarted.db.stat().st_ino == inode, "ceiling_restart_replaced_database")
        with PubsubWire(restarted.address, restarted.observations, "same-filter-after-restart") as wire:
            result["session_present_after_restart"] = wire.connect("receipt-ceiling-client", keep_alive=0,
                                                                    session_present=True)
            result["after_restart"] = replace_one(wire, 1, response_timeout)
            if result["first_nonpositive"]:
                control_progress(wire)
                require(result["after_restart"]["result"] == "no_frame"
                        and table_counts(restarted.db)["counts"]["operations"] == HISTORICAL_RECEIPT_LIMIT,
                        "historical_ceiling_not_preserved_on_restart")
            else:
                require(result["after_restart"].get("codes") == [1], "receipt_progress_lost_on_restart")
            result["restart_snapshot"] = table_counts(restarted.db)
        result["restart_stop_status"] = restarted.stop()
        writer_available(broker.db)
        result["elapsed_ms"] = (time.monotonic_ns() - started) / 1e6
        result["same_file_inode"] = inode
        if result["first_nonpositive"]:
            result["status"] = "FAIL"
            result["failure"] = {"code": "receipt_capacity_ceiling",
                                 "detail": "8192 terminal replacements exhausted receipts while one live route and no publications remained; PING and same-file restart corroborated the ceiling"}
        else:
            result["status"] = "PASS"
        write_json(case / "observation.json", result)
        return result
    finally:
        broker.cleanup()
        if restarted:
            restarted.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--binary-source-head")
    parser.add_argument("--expected-binary-sha256")
    parser.add_argument("--case", choices=("A-ceiling",), default="A-ceiling")
    args = parser.parse_args()
    require(args.binary.is_file(), "binary_missing")
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    digest = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    if args.expected_binary_sha256:
        require(digest == args.expected_binary_sha256, "wrong_baseline_binary")
    harness = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    result = {"format": "mqttiumd.receipt-system-reception", "schema_version": 1,
              "qualification": "system-test-only", "source_head": args.binary_source_head or harness,
              "harness_source_head": harness, "binary": {"path": str(args.binary.resolve()), "sha256": digest},
              "cases": []}
    try:
        observed = historical_ceiling(args.binary.resolve(), args.out / args.case)
        result["cases"].append(observed)
        result["status"] = observed["status"]
        if observed.get("failure"):
            result["failure"] = observed["failure"]
    except Exception as error:
        result["status"] = "FAIL"
        result["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:1024]}
    write_json(args.out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "failure": result.get("failure"),
                      "receipt": str(args.out / "receipt.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
