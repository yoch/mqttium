#!/usr/bin/env python3
"""Restore the served pilot file and a separately captured charged file."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from common import (Broker, CAPTEURS, DASHBOARDS, FOREVER, PACKAGE, inventory,
                    mqtt_client, pki_tools, principal, read_db, require, sha, topic, wait_for, write)


def demonstration(package):
    spec = importlib.util.spec_from_file_location("policy_demo", package / "policy-demo.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Client:
    def __init__(self, pki, port, name, *, receive_maximum=8):
        self.messages = queue.Queue(maxsize=256)
        self.connected = threading.Event()
        self.subscribed = threading.Event()
        self.closed = threading.Event()
        self.connack = None
        self.grants = None
        self.client, properties, keepalive = mqtt_client(pki, name, receive_maximum=receive_maximum)
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_disconnect = lambda *_: self.closed.set()
        self.client.connect("127.0.0.1", port, keepalive, clean_start=False, properties=properties)
        self.client.loop_start()
        require(self.connected.wait(5) and self.connack[0] == 0, "restore_connack_failed")

    def on_connect(self, _client, _data, flags, reason, _properties):
        self.connack = [reason.value, bool(flags.session_present)]
        self.connected.set()

    def on_subscribe(self, _client, _data, _mid, reasons, _properties):
        self.grants = [reason.value for reason in reasons]
        self.subscribed.set()

    def on_message(self, client, _data, message):
        self.messages.put_nowait((message.topic, message.payload, message.qos, message.mid,
                                  message.dup, message.retain))
        client.ack(message.mid, message.qos)

    def subscribe(self, filters):
        self.subscribed.clear()
        rc, _mid = self.client.subscribe([(f, 1) for f in filters])
        require(rc == 0 and self.subscribed.wait(5) and self.grants == [1] * len(filters),
                "restore_suback_failed")

    def publish(self, name, value, retain=False):
        receipt = self.client.publish(name, value, qos=1, retain=retain)
        receipt.wait_for_publish(timeout=5)
        require(receipt.is_published(), "restore_publish_not_acknowledged")

    def close(self):
        if self.client.is_connected():
            self.client.disconnect()
            require(self.closed.wait(5), "local_disconnect_did_not_finish")
        self.client.loop_stop()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def policy_command(binary, verb, db, old, new, plan, backup=None):
    argv = [str(binary), "policy", verb, "--db", str(db), "--old-identities", str(old),
            "--new-identities", str(new)]
    argv += ["--out", str(plan)] if verb == "plan" else ["--plan", str(plan), "--backup", str(backup)]
    result = subprocess.run(argv, capture_output=True, timeout=30)
    value = json.loads(result.stdout)
    return {"argv": argv, "exit": result.returncode, "result": value}


def recover_quiet(binary, db, out, pki, policy, expected, package):
    out.mkdir(mode=0o700)
    demo = demonstration(package)
    with read_db(db) as snapshot:
        expected_values = {bytes(name).decode(): bytes(value) for name, value in snapshot.execute(
            "SELECT r.topic,b.bytes FROM retained r JOIN publications p ON r.publication=p.id "
            "JOIN payloads b ON b.id=p.payload")}
    require(len(expected_values) == expected, "quiet_retained_inventory_mismatch")
    broker = Broker(binary, db, out, pki, policy=policy)
    result = {}
    try:
        with ExitStack() as clients:
            dashboards = [clients.enter_context(Client(pki, broker.port, cid)) for cid in DASHBOARDS]
            require(all(c.connack == [0, True] for c in dashboards), "quiet_sessions_not_resumed")
            wait_for(lambda: inventory(db)["counts"]["obligations"] == 0, "quiet_backlog_not_finished")
            before_replay = demo.inspect(db)
            require(before_replay["tables"]["sessions"] != "", "quiet_sessions_missing")
            for c in dashboards:
                while not c.messages.empty():
                    c.messages.get_nowait()
            dashboards[0].subscribe(["atelier/#"])
            replays = [dashboards[0].messages.get(timeout=6) for _ in range(expected)]
            require({m[0]: m[1] for m in replays} == expected_values and all(m[5] for m in replays),
                    "quiet_retained_replay_incorrect")
            with Client(pki, broker.port, CAPTEURS[0]) as sensor:
                require(sensor.connack == [0, True], "quiet_sensor_session_lost")
                sensor.publish(topic(CAPTEURS[0]), b"restored-application-probe")
                for c in dashboards:
                    message = c.messages.get(timeout=5)
                    require(message[:3] == (topic(CAPTEURS[0]), b"restored-application-probe", 1),
                            "restored_application_not_delivered")
            wait_for(lambda: inventory(db)["counts"]["obligations"] == 0, "quiet_probe_not_finished")
            result = {"connacks": [c.connack for c in dashboards], "retained_replays": expected,
                      "application_deliveries": 2, "before_stop": inventory(db)}
    finally:
        result["process"] = broker.stop()
    result["after_stop"] = inventory(db)
    require(result["process"]["wait_status"] == 0 and result["after_stop"]["invariants_ok"],
            "quiet_restored_broker_failed")
    return result


def onboard(args, out, policy):
    out.mkdir(mode=0o700)
    demo = demonstration(args.package)
    broker = Broker(args.binary, args.db, out, args.pki, policy=policy)
    result = {}
    try:
        with Client(args.pki, broker.port, DASHBOARDS[0]) as first, Client(args.pki, broker.port, DASHBOARDS[1]) as second:
            with Client(args.pki, broker.port, "capteur-9") as sensor:
                require(sensor.connack == [0, False], "new_principal_did_not_open_new_session")
                sensor.publish(topic("capteur-9", "status"), b"new-device", retain=True)
                for dashboard in (first, second):
                    require(dashboard.messages.get(timeout=5)[:3] == (topic("capteur-9", "status"), b"new-device", 1),
                            "new_device_not_received_by_old_dashboard")
            foreign = demo.mqtt_session(broker.port, args.pki, "capteur-9", "capteur-1")
            require(foreign == {"present": False, "reason": 135, "transport": "TLSv1.3"},
                    "new_device_rebound_old_client_id")
            wait_for(lambda: inventory(args.db)["counts"]["obligations"] == 0, "new_device_work_not_finished")
            result = {"new_device_deliveries": 2, "foreign_connack": foreign, "after": inventory(args.db)}
    finally:
        result["process"] = broker.stop()
    return result


def quiet(args, out):
    demo = demonstration(args.package)
    before = demo.inspect(args.db)
    old = args.pki / "identities.json"
    new = deepcopy(json.loads(old.read_text()))
    pin = pki_tools(args.package).issue(args.pki, "capteur-9", "clientAuth")
    new["principals"].append(principal("capteur-9", pin))
    new_file, plan, backup = out / "new-identities.json", out / "plan.json", out / "before.sqlite"
    write(new_file, new)
    planned = policy_command(args.binary, "plan", args.db, old, new_file, plan)
    require(planned["exit"] == 0 and planned["result"]["status"] == "Planned", "loaded_policy_plan_failed")
    require(demo.inspect(args.db) == before, "plan_changed_loaded_database")
    applied = policy_command(args.binary, "apply", args.db, old, new_file, plan, backup)
    require(applied["exit"] == 0 and applied["result"]["status"] == "Applied", "loaded_policy_apply_failed")
    require(demo.inspect(args.db)["tables"] == before["tables"], "transition_changed_business")
    require(demo.inspect(backup) == before, "backup_is_not_full_committed_image")
    restore_dir = out / "private-restored"
    restore_dir.mkdir(mode=0o700)
    restored = restore_dir / "broker.sqlite"
    shutil.copyfile(backup, restored)
    restored.chmod(0o600)
    require(sha(restored) == applied["result"]["backup_sha256"], "restore_digest_mismatch")
    result = {"before": before, "plan": planned, "apply": applied,
              "restored": recover_quiet(args.binary, restored, out / "restore-run", args.pki, old,
                                         before["counts"]["retained"], args.package)}
    result["same_file_after_apply"] = recover_quiet(args.binary, args.db, out / "original-run", args.pki,
        new_file, before["counts"]["retained"], args.package)
    result["new_device"] = onboard(args, out / "new-device", new_file)
    return result


def charged(args, out):
    sys.path.insert(0, str(args.package / "tests/system"))
    import policy_transition_reception as receiver
    from secure_reception import Peer, broker_with, client_context, observed
    from tcp_probe import packet
    demo = receiver.demo
    with tempfile.TemporaryDirectory(prefix="mqttiumd-pilot-restore-") as private:
        pki = Path(private) / "pki"
        receiver.credentials(pki)
        db, seed = receiver.seed(args.binary, out / "seed", pki, old_retained_reference=True)
        control = receiver.Case(args.binary, out / "offline", db, pki / "identities.json", pki / "next.json")
        control.expect("plan", "charged", "Planned")
        before = demo.inspect(db)
        applied = control.expect("apply", "charged", "Applied")
        backup = control.out / "charged.sqlite"
        require(demo.inspect(backup) == before and sha(backup) == applied["backup_sha256"],
                "charged_backup_did_not_include_wal")
        directory = out / "private-restored"
        directory.mkdir(mode=0o700)
        restored = directory / "broker.sqlite"
        shutil.copyfile(backup, restored)
        restored.chmod(0o600)
        broker = broker_with(args.binary, out / "restore-run", pki, db=restored)
        result = {"seed": seed, "applied": applied, "backup_sha256": sha(backup)}
        try:
            with Peer(broker, "restored-dashboard", client_context(pki, "dashboard")) as dashboard:
                require(dashboard.open("dashboard"), "charged_dashboard_not_resumed")
                first = dashboard.message(b"seed-retained")
                require(first.packet_id == seed["claim_packet_id"] and first.dup,
                        "charged_claim_identity_not_recovered")
                dashboard.message(b"current-retained")
                dashboard.message(b"seed-inbound")
                dashboard.message(b"armed-will")
                dashboard.subscribe(22, [("atelier/capteur-1/status", 1)])
                replay = dashboard.message(b"current-retained")
                require(replay.retain, "charged_current_retained_not_replayed")
                with Peer(broker, "restored-source", client_context(pki)) as sensor:
                    require(sensor.open("capteur-1"), "charged_source_not_resumed")
                    sensor.send(packet(0x62, b"\x00\x11"))
                    sensor.expect(0x70, 17)
                    sensor.send(b"\xe0\x00")
                dashboard.send(b"\xe0\x00")
            drained = observed(broker, lambda: (lambda state: state if all(state["counts"][key] == 0
                for key in ("obligations", "inbound", "outbound", "wills")) else None)(demo.inspect(restored)),
                "charged_restore_not_finished")
            result.update(original_claim_pi=first.packet_id, retransmitted=first.dup,
                          application_deliveries=5, inbound_qos2_completed=17,
                          after=drained, schema=13)
        finally:
            result["process"] = {"pid": broker.process.pid, "wait_status": broker.stop()}
            broker.cleanup()
        require(result["process"]["wait_status"] == 0, "charged_restored_process_failure")
        result["commands"] = control.calls
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    parser.add_argument("--binary", type=Path, default=PACKAGE / "mqttiumd")
    parser.add_argument("--db", type=Path)
    parser.add_argument("--pki", type=Path)
    parser.add_argument("--mode", choices=("quiet", "charged"), required=True)
    args = parser.parse_args()
    for name in ("package", "binary", "db", "pki"):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, value.resolve())
    os.umask(0o077)
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    report = {"status": "FAIL", "mode": args.mode, "binary_sha256": sha(args.binary)}
    build_file = args.package / "BUILD.json"
    if build_file.is_file():
        build = json.loads(build_file.read_text())
        if build["binary_sha256"] != sha(args.binary):
            raise ValueError("packaged_binary_identity_mismatch")
        report["source_sha"] = build["source_sha"]
    try:
        report["result"] = quiet(args, out) if args.mode == "quiet" else charged(args, out)
        report["status"] = "PASS"
    except Exception as error:
        report["failure"] = str(error)
    write(out / "restore.json", report)
    print(json.dumps({"status": report["status"], "failure": report.get("failure"), "receipt": str(out / "restore.json")}))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
