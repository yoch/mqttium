#!/usr/bin/env python3
"""Exercise eight device processes and two durable dashboards over mutual TLS."""
from __future__ import annotations

import argparse
import hashlib
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from common import (Broker, CAPTEURS, DASHBOARDS, PACKAGE, event_rows, inventory,
                    provision, sha, topic, wait_for, write)
from restore import Client


def worker(args):
    from common import mqtt_client
    import threading
    ready, admitted, closed = threading.Event(), threading.Event(), threading.Event()
    client, properties, keepalive = mqtt_client(args.pki, args.device, will=True)
    client.on_connect = lambda _c, _d, _f, reason, _p: ready.set() if reason.value == 0 else None
    client.on_publish = lambda *_: admitted.set()
    client.on_disconnect = lambda *_: closed.set()
    try:
        client.connect("127.0.0.1", args.port, keepalive, clean_start=False, properties=properties)
        client.loop_start()
        if not ready.wait(5):
            return 2
        client.publish(topic(args.device, "status"), b"online", qos=1, retain=True)
        if not admitted.wait(5):
            return 2
        print(json.dumps({"ready": True, "pid": os.getpid(), "device": args.device}), flush=True)
        if sys.stdin.readline().strip() == "normal":
            # A normal departure cancels the Will. The application updates its
            # retained presence itself before sending its normal DISCONNECT.
            message = client.publish(topic(args.device, "status"), b"offline", qos=1, retain=True)
            message.wait_for_publish(timeout=5)
            if not message.is_published():
                return 2
            client.disconnect()
            return 0 if closed.wait(5) else 2
        return 2
    finally:
        client.loop_stop()


def run(args):
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    result = {"status": "FAIL", "binary_sha256": sha(args.binary), "children": []}
    build_file = args.package / "BUILD.json"
    if build_file.is_file():
        build = json.loads(build_file.read_text())
        if build["binary_sha256"] != sha(args.binary):
            raise ValueError("packaged_binary_identity_mismatch")
        result["source_sha"] = build["source_sha"]
    broker = None
    children = []
    try:
        with tempfile.TemporaryDirectory(prefix="mqttiumd-pilot-faults-") as private:
            pki = Path(private) / "pki"
            provision(pki, args.package)
            db = out / "broker.sqlite"
            broker = Broker(args.binary, db, out, pki)
            result["initial_pid"] = broker.child.pid
            with ExitStack() as peers:
                dashboards = [peers.enter_context(Client(pki, broker.port, cid)) for cid in DASHBOARDS]
                for client in dashboards:
                    client.subscribe(["atelier/+/status"])
                dashboards[1].close()
                for device in CAPTEURS:
                    argv = [sys.executable, __file__, "--worker", "--pki", str(pki),
                            "--port", str(broker.port), "--device", device]
                    log = (out / f"{device}.log").open("wb")
                    child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log, text=True)
                    children.append((device, child, log))
                    # The child emits readiness only after real CONNACK and PUBACK.
                    import select
                    require_ready = select.select([child.stdout], [], [], 8)[0]
                    if not require_ready:
                        raise ValueError("device_process_did_not_receive_puback")
                    receipt = json.loads(child.stdout.readline())
                    if not receipt.get("ready") or receipt["pid"] != child.pid:
                        raise ValueError("device_readiness_identity_mismatch")
                    observed = dashboards[0].messages.get(timeout=5)
                    if observed[:3] != (topic(device, "status"), b"online", 1):
                        raise ValueError("live_dashboard_did_not_receive_online")
                result["connected"] = inventory(db)
                device, child, _ = children[-1]
                child.stdin.write("normal\n")
                child.stdin.flush()
                status = child.wait(timeout=8)
                result["children"].append({"device": device, "pid": child.pid, "event": "application_offline_and_disconnect", "wait": status})
                if status != 0 or dashboards[0].messages.get(timeout=5)[1] != b"offline":
                    raise ValueError("normal_application_departure_failed")
                for device, child, _ in children[:2]:
                    child.send_signal(signal.SIGKILL)
                    status = child.wait(timeout=5)
                    result["children"].append({"device": device, "pid": child.pid, "event": "SIGKILL", "wait": status})
                    if status != -9 or dashboards[0].messages.get(timeout=6)[:3] != (topic(device, "status"), b"offline", 1):
                        raise ValueError("detected_device_death_was_not_delivered")
                result["broker_crash"] = broker.stop(kill=True)
                if result["broker_crash"]["wait_status"] != -9:
                    raise ValueError("broker_sigkill_not_observed")
            for device, child, _ in children[2:-1]:
                child.kill()
                result["children"].append({"device": device, "pid": child.pid,
                    "event": "old_client_after_broker_crash", "wait": child.wait(timeout=5)})
            broker = Broker(args.binary, db, out, pki, label="restarted")
            with ExitStack() as peers:
                dashboards = [peers.enter_context(Client(pki, broker.port, cid)) for cid in DASHBOARDS]
                if any(c.connack != [0, True] for c in dashboards):
                    raise ValueError("persistent_dashboard_did_not_resume")
                wait_for(lambda: all(inventory(db)["counts"][name] == 0
                                     for name in ("wills", "obligations", "outbound")),
                         "recovered_wills_and_backlog_not_drained")
                backlog = []
                for client in dashboards:
                    rows = []
                    while not client.messages.empty():
                        rows.append(client.messages.get_nowait())
                    backlog.append(rows)
                # The offline dashboard must recover every acknowledged online
                # and every distinct offline responsibility admitted afterwards.
                expected = {(topic(c, "status"), value) for c in CAPTEURS for value in (b"online", b"offline")}
                result["recovered_counts"] = [len(rows) for rows in backlog]
                result["recovered_identity_hashes"] = [[hashlib.sha256(m[0].encode()+b"\0"+m[1]).hexdigest()
                    for m in rows] for rows in backlog]
                if {(m[0], m[1]) for m in backlog[1]} != expected:
                    raise ValueError("offline_dashboard_responsibility_missing")
                result["offline_dashboard_deliveries"] = len(backlog[1])
                with Client(pki, broker.port, CAPTEURS[0]) as sensor:
                    if sensor.connack != [0, True]:
                        raise ValueError("returning_sensor_did_not_resume")
                    sensor.publish(topic(CAPTEURS[0], "status"), b"online", retain=True)
                    for client in dashboards:
                        if client.messages.get(timeout=5)[:3] != (topic(CAPTEURS[0], "status"), b"online", 1):
                            raise ValueError("returning_sensor_online_not_observed")
                dashboards[0].subscribe(["atelier/+/status"])
                final = [dashboards[0].messages.get(timeout=5) for _ in CAPTEURS]
                if not all(m[5] and m[1] == (b"online" if m[0] == topic(CAPTEURS[0], "status") else b"offline") for m in final):
                    raise ValueError("current_presence_order_incorrect")
                wait_for(lambda: inventory(db)["counts"]["obligations"] == 0, "presence_replay_not_finished")
            result["final"] = inventory(db)
            wills = [e for name in ("broker.jsonl", "restarted.jsonl") for e in event_rows(out / name)
                     if e["kind"] == "will_admitted"]
            result["will_admissions"] = len(wills)
            if len(wills) != 7 or not result["final"]["invariants_ok"] or result["final"]["counts"]["wills"] != 0:
                raise ValueError("will_occurrences_or_resources_incorrect")
            result["status"] = "PASS"
    except Exception as error:
        result["failure"] = str(error)
    finally:
        for _device, child, log in children:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            log.close()
        if broker:
            result["final_process"] = broker.stop()
        write(out / "perturbations.json", result)
    print(json.dumps({"status": result["status"], "failure": result.get("failure"), "receipt": str(out / "perturbations.json")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, nargs="?")
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--pki", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--device")
    parser.add_argument("--package", type=Path, default=PACKAGE)
    parser.add_argument("--binary", type=Path, default=PACKAGE / "mqttiumd")
    args = parser.parse_args()
    raise SystemExit(worker(args) if args.worker else run(args))
