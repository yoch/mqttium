#!/usr/bin/env python3
"""Small finite idle-backlog witness, not a throughput benchmark or qualifier."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from pubsub_probe import PubsubWire, durable
from tcp_probe import require, write_json
from tcp_reception import Broker, writer_available


def run(binary: Path, case: Path) -> dict:
    broker = Broker(binary, case)
    try:
        ready = broker.ready()
        with PubsubWire(broker.address, broker.observations, "offline-subscription") as subscriber:
            subscriber.connect("ready-backlog", session_present=False, receive_maximum=8)
            subscriber.subscribe(1, [("reception/ready", 1)])
        with PubsubWire(broker.address, broker.observations, "publisher") as publisher:
            publisher.connect("ready-source", session_present=False)
            for identifier in range(1, 13):
                publisher.publish(identifier, "reception/ready", f"item-{identifier}".encode(), qos=1)
        before = durable(broker.db)
        require(before["counts"]["obligations"] == 12 and before["counts"]["outbound"] == 0,
                "offline_backlog_not_established")
        write_json(case / "before.json", before)
        received = []
        with PubsubWire(broker.address, broker.observations, "resumed-backlog") as subscriber:
            subscriber.connect("ready-backlog", session_present=True, receive_maximum=8)
            connected = time.monotonic_ns()
            for identifier in range(1, 13):
                publication = subscriber.next_publication(timeout=3)
                observed = time.monotonic_ns()
                require(publication.payload == f"item-{identifier}".encode(), "backlog_order_or_content")
                received.append({"packet_id": publication.packet_id, "payload": publication.payload.decode(),
                                 "monotonic_ns": str(observed), "since_connack_ms": (observed-connected)/1e6})
                subscriber.acknowledge(publication)
            deadline = time.monotonic() + 5
            after = durable(broker.db)
            while after["counts"]["obligations"] and time.monotonic() < deadline:
                time.sleep(.001)
                after = durable(broker.db)
            require(after["counts"]["obligations"] == after["counts"]["outbound"] == 0,
                    "backlog_not_finished")
            drained = time.monotonic_ns()
        require(after["admissions"] == before["admissions"], "new_admission_during_backlog_drain")
        write_json(case / "after.json", after)
        status = broker.stop()
        writer_available(broker.db)
        return {"status": "PASS", "messages": 12, "receive_maximum": 8,
                "parameters": {"publisher_stopped_before_resume": True, "per_message_timeout_s": 3,
                               "finish_timeout_s": 5, "clock": "monotonic_ns"},
                "before": {"obligations": 12, "outbound": 0}, "received": received,
                "interarrival_ms": [(int(b["monotonic_ns"])-int(a["monotonic_ns"]))/1e6
                                    for a, b in zip(received, received[1:])],
                "drain_since_connack_ms": (drained-connected)/1e6,
                "new_admissions_during_drain": 0, "obligations_remaining_after_finish": 0,
                "exit_status": status, "effective_broker_limits": ready["details"],
                "scope": "finite timing observation; causal scan independence is a separate deterministic reactor test"}
    finally:
        broker.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    require(not any(args.out.iterdir()), "output_not_empty")
    result = {"format": "mqttiumd.ready-backlog-system-witness", "schema_version": 1,
              "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "binary": {"path": str(args.binary.resolve()),
                         "sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest()},
              "qualification": "system-test-only"}
    try:
        result.update(run(args.binary, args.out / "case"))
    except Exception as error:
        result.update(status="FAIL", failure={"code": getattr(error, "code", type(error).__name__),
                                              "detail": str(error)[:1024]})
    write_json(args.out / "result.json", result)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
