#!/usr/bin/env python3
"""Observe the configured persistence policies after a process SIGKILL."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import queue
import tempfile
import time

from common import (Broker, CAPTEURS, DASHBOARDS, PACKAGE, inventory, provision,
                    require, sha, topic, wait_for, write)
from load import Mosquitto
from restore import Client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--backend", choices=("mqttiumd", "mosquitto"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    args = parser.parse_args()
    os.umask(0o077)
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    result = {"status": "FAIL", "backend": args.backend, "binary_sha256": sha(args.binary),
              "parameters": {"seconds_before_burst": 31, "burst_publications": 20,
                             "burst_rate": 50, "payload_bytes": 256, "destinations": 2}}
    broker = None
    try:
        with tempfile.TemporaryDirectory(prefix="mqttiumd-persistence-compare-") as private:
            pki = Path(private) / "pki"
            provision(pki, args.package)

            def start(label):
                return (Broker(args.binary, out / "broker.sqlite", out, pki, label=label)
                        if args.backend == "mqttiumd" else Mosquitto(args.binary, out, pki))

            broker = start("before-crash")
            with ExitStack() as clients:
                dashboards = [clients.enter_context(Client(pki, broker.port, cid)) for cid in DASHBOARDS]
                for c in dashboards:
                    c.subscribe(["atelier/+/telemetry"])
                # Establish all ten configured persistent identities first.
                for cid in CAPTEURS[1:]:
                    with Client(pki, broker.port, cid):
                        pass
                with Client(pki, broker.port, CAPTEURS[0]) as sensor:
                    sensor.publish(topic(CAPTEURS[0]), b"checkpointed-seed", retain=True)
                    for c in dashboards:
                        require(c.messages.get(timeout=5)[1] == b"checkpointed-seed", "comparison_seed_not_received")
                    for c in dashboards:
                        c.close()
                    # Mosquitto uses its declared 30-second autosave policy.
                    # The same settling interval is applied to both brokers.
                    time.sleep(31)
                    if args.backend == "mosquitto":
                        require(broker.db.is_file(), "mosquitto_initial_periodic_snapshot_not_created")
                    burst_start = time.monotonic_ns()
                    for seq in range(20):
                        due = burst_start + seq * 20_000_000
                        if due > time.monotonic_ns():
                            time.sleep((due - time.monotonic_ns()) / 1e9)
                        sensor.publish(topic(CAPTEURS[0]), seq.to_bytes(8, "big") + b"x" * 248, retain=seq == 19)
                    result["burst_positive_pubacks"] = 20
                    result["burst_duration_ms"] = (time.monotonic_ns() - burst_start) / 1e6
                    if args.backend == "mqttiumd":
                        result["before_kill"] = inventory(broker.db)
                    result["kill"] = broker.stop(kill=True)
                    require(result["kill"]["wait_status"] == -9, "comparison_sigkill_not_observed")
            if args.backend == "mosquitto":
                (out / "mosquitto.log").rename(out / "mosquitto-before-crash.log")
            broker = start("after-crash")
            recovered = []
            with ExitStack() as clients:
                dashboards = [clients.enter_context(Client(pki, broker.port, cid)) for cid in DASHBOARDS]
                result["session_present"] = [c.connack[1] for c in dashboards]
                for c in dashboards:
                    rows = []
                    until = time.monotonic() + 2
                    while time.monotonic() < until and len(rows) < 20:
                        try:
                            rows.append(c.messages.get(timeout=.1))
                        except queue.Empty:
                            pass
                    recovered.append(rows)
                result["backlog_received"] = [len(rows) for rows in recovered]
                if args.backend == "mqttiumd":
                    require(all(c.connack == [0, True] for c in dashboards), "durable_comparison_sessions_lost")
                    require(all([int.from_bytes(row[1][:8], "big") for row in rows] == list(range(20))
                                for rows in recovered), "durable_comparison_backlog_lost")
                dashboards[0].subscribe([topic(CAPTEURS[0])])
                current = dashboards[0].messages.get(timeout=5)
                require(current[5], "comparison_current_retained_missing")
                result["retained_sha256"] = hashlib.sha256(current[1]).hexdigest()
                result["retained_is_latest_burst"] = current[1] == (19).to_bytes(8, "big") + b"x" * 248
                if args.backend == "mqttiumd":
                    require(result["retained_is_latest_burst"], "durable_comparison_retained_lost")
                    wait_for(lambda: inventory(broker.db)["counts"]["obligations"] == 0,
                             "durable_comparison_backlog_not_finished")
                    result["after_drain"] = inventory(broker.db)
            result["status"] = "PASS"
    except Exception as error:
        result["failure"] = str(error)
    finally:
        if broker:
            result["final_process"] = broker.stop()
        write(out / "crash-comparison.json", result)
    print(json.dumps({"status": result["status"], "failure": result.get("failure"), "receipt": str(out / "crash-comparison.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
