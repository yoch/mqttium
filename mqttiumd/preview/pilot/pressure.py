#!/usr/bin/env python3
"""Measure the existing durable reference limit during a dashboard outage."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import queue
import tempfile
import time

from common import Broker, PACKAGE, inventory, provision, require, sha, topic, wait_for, write
from restore import Client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    parser.add_argument("--binary", type=Path, default=PACKAGE / "mqttiumd")
    args = parser.parse_args()
    os.umask(0o077)
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    result = {"status": "FAIL", "binary_sha256": sha(args.binary),
              "parameters": {"rate": 50, "seconds": 10, "published": 500,
                             "payload_bytes": 256, "reference_limit": 128}}
    broker = None
    try:
        with tempfile.TemporaryDirectory(prefix="mqttiumd-pilot-pressure-") as private:
            pki = Path(private) / "pki"
            provision(pki, args.package)
            broker = Broker(args.binary, out / "broker.sqlite", out, pki)
            with Client(pki, broker.port, "dashboard-2") as offline:
                offline.subscribe(["atelier/+/telemetry"])
            with Client(pki, broker.port, "dashboard-1") as online, \
                    Client(pki, broker.port, "capteur-1") as sensor, \
                    Client(pki, broker.port, "capteur-8") as control:
                online.subscribe(["atelier/+/telemetry"])
                reasons = queue.Queue(maxsize=8)
                sensor.client.on_publish = lambda _c, _d, mid, reason, _p: reasons.put_nowait((mid, reason.value))
                success, refused, times = [], [], []
                start = time.monotonic()
                for seq in range(500):
                    deadline = start + seq / 50
                    if deadline > time.monotonic():
                        time.sleep(deadline - time.monotonic())
                    data = seq.to_bytes(8, "big") + b"x" * 248
                    begin = time.monotonic_ns()
                    receipt = sensor.client.publish(topic("capteur-1"), data, qos=1)
                    mid, reason = reasons.get(timeout=5)
                    require(receipt.rc == 0 and receipt.mid == mid, "pressure_puback_correlation")
                    times.append((time.monotonic_ns() - begin) / 1e6)
                    if reason < 128:
                        success.append(seq)
                    else:
                        require(reason == 0x97, "unexpected_pressure_rejection_reason")
                        refused.append(seq)
                require(success and refused, "reference_limit_not_exercised")
                live = [online.messages.get(timeout=5) for _ in success]
                require([int.from_bytes(m[1][:8], "big") for m in live] == success,
                        "refused_admission_was_partially_delivered")
                require(online.messages.empty(), "extra_live_delivery_under_pressure")
                control.publish(topic("capteur-8", "control"), b"control-without-destinations")
                loaded = wait_for(lambda: (lambda row: row if row["counts"]["obligations"] == len(success)
                    and row["counts"]["outbound"] == 0 else None)(inventory(broker.db)),
                    "online_work_not_finished_under_pressure")
                require(loaded["invariants_ok"], "pressure_resource_account_mismatch")
                with Client(pki, broker.port, "dashboard-2") as restored:
                    require(restored.connack == [0, True], "offline_pressure_session_not_resumed")
                    received = [restored.messages.get(timeout=8) for _ in success]
                    require([int.from_bytes(m[1][:8], "big") for m in received] == success,
                            "offline_pressure_backlog_incomplete")
                    wait_for(lambda: inventory(broker.db)["counts"]["obligations"] == 0,
                             "pressure_backlog_not_finished")
                    sensor.publish(topic("capteur-1"), b"after-capacity-returned")
                    require(online.messages.get(timeout=5)[1] == b"after-capacity-returned"
                            and restored.messages.get(timeout=5)[1] == b"after-capacity-returned",
                            "admissions_did_not_resume_after_capacity_return")
                    wait_for(lambda: inventory(broker.db)["counts"]["obligations"] == 0,
                             "post_pressure_delivery_not_finished")
                result.update(positive_pubacks=len(success), negative_pubacks=len(refused), reason=0x97,
                              first_rejection_s=refused[0] / 50, ordered_sequences=True,
                              independent_control_puback=True, at_pressure=loaded,
                              after_drain=inventory(broker.db), puback_max_ms=max(times))
            result["status"] = "PASS"
    except Exception as error:
        result["failure"] = str(error)
    finally:
        if broker:
            result["process"] = broker.stop()
        write(out / "pressure.json", result)
    print(json.dumps({"status": result["status"], "failure": result.get("failure"), "receipt": str(out / "pressure.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
