#!/usr/bin/env python3
"""Exercise the offline authority transaction and its public MQTT recovery."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import sqlite3
import subprocess
import tempfile
import threading
import time

from external_mqttium import client_identity
from secure_reception import INFINITE, STATUS, Peer, broker_with, client_context, observed, pki_module
from tcp_probe import Observations, ReceiptFailure, packet, require, write_json
from message_expiry_probe import publish_packet

DEMO = Path(__file__).resolve().parents[2] / "preview/policy-demo.py"
spec = importlib.util.spec_from_file_location("policy_demo", DEMO)
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)
NEW_TOPIC = "atelier/capteur-2/status"


class WireMetadata(Observations):
    def record_bytes(self, kind, data, **fields):
        self.record(kind, bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), **fields)


def image(db, target):
    with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as src, sqlite3.connect(target) as dst:
        src.backup(dst)
        dst.execute("PRAGMA journal_mode=DELETE")
    return target


class Case:
    def __init__(self, binary, out, db, old, new):
        self.binary, self.out, self.db, self.old, self.new = binary, out, db, old, new
        self.calls = []
        self.out.mkdir(mode=0o700)

    def argv(self, verb, name, *, old=None, new=None):
        plan = self.out / f"{name}.json"
        backup = self.out / f"{name}.sqlite"
        return [self.binary, "policy", verb, "--db", self.db, "--old-identities", old or self.old,
                "--new-identities", new or self.new,
                *(["--out", plan] if verb == "plan" else ["--plan", plan, "--backup", backup])]

    def invoke(self, argv):
        result = subprocess.run(argv, capture_output=True, timeout=12, check=False)
        try:
            outcome = json.loads(result.stdout)
        except (ValueError, UnicodeError):
            raise ReceiptFailure("unstructured_policy_result", str(result.returncode)) from None
        self.calls.append({"command": [str(arg) for arg in argv], "exit": result.returncode, "outcome": outcome})
        return result.returncode, outcome

    def expect(self, verb, name, value, *, old=None, new=None, diagnosis="wrong_policy_result"):
        status, outcome = self.invoke(self.argv(verb, name, old=old, new=new))
        require((status, outcome.get("code", outcome.get("status"))) == (
            0 if value in ("Planned", "Applied", "AlreadyApplied", "Unchanged") else 2, value),
            diagnosis, str((status, outcome.get("code", outcome.get("status")))))
        return outcome


def credentials(root):
    old = pki_module.generate(root)
    old["principals"][0]["client_ids"].append("capteur-pending")
    write_json(root / "identities.json", old)
    pin = pki_module.issue(root, "sensor-2", "clientAuth")
    new = deepcopy(old)
    new["principals"].append({"name": "sensor-2", "certificates": [pin], "client_ids": ["capteur-2"],
                              "publish": ["atelier/capteur-2"], "subscribe": []})
    write_json(root / "next.json", new)
    return old, new


def seed(binary, out, pki, *, pending_will=False):
    broker = broker_with(binary, out, pki, pauses=("will.admission.prepared",) if pending_will else ())
    wire = WireMetadata(out / "wire-metadata.jsonl")
    try:
        with ExitStack() as stack:
            def peer(label, device):
                return stack.enter_context(Peer(broker, label, client_context(pki, device), log=wire))
            dashboard = peer("offline-dashboard", "dashboard")
            dashboard.open("dashboard", qos_window=1)
            dashboard.subscribe(1, [("atelier/+/status", 1), (STATUS, 1)])
            sensor = peer("old-sensor", "sensor")
            sensor.open("capteur-1", will=(STATUS, b"armed-will"))
            sensor.publish(STATUS, b"seed-retained", retain=True)
            first = dashboard.message(b"seed-retained", ack=False)
            require(first.qos == 1 and first.packet_id is not None, "old_claim_not_transmitted")
            sensor.send(publish_packet(STATUS, b"seed-inbound", 17, qos=2))
            sensor.expect(0x50, 17)
            if pending_will:
                before = int(broker.events()[-1]["seq"])
                pending = peer("pending-sensor", "sensor")
                pending.open("capteur-pending", will=(STATUS, b"pending-will"))
                pending.close()
                broker.wait_event("barrier_hit", point="will.admission.prepared", after=before)
            killed = broker.process.pid
            require(broker.stop(killed=True) == -9, "seed_sigkill_not_observed")
        snapshot = demo.inspect(broker.db)
        require(all(snapshot["counts"][key] > 0 for key in
                    ("inbound", "outbound", "wills", "obligations", "retained", "payloads", "operations")),
                "seed_responsibilities_missing")
        # The child was killed while it still had the WAL open. Only this
        # path, including committed WAL pages, is used to obtain the plan.
        wal = Path(str(broker.db) + "-wal")
        require(wal.stat().st_size > 32, "seed_wal_not_present")
        return broker.db, {"pid": killed, "wait": -9, "wal_bytes": wal.stat().st_size,
                           "claim_packet_id": first.packet_id, "before": snapshot}
    finally:
        broker.cleanup()


def refused_changes(case, pki, old, new, which=None):
    variants = {}
    reduced = deepcopy(new)
    reduced["principals"][1]["subscribe"] = []
    variants["reduction"] = (reduced, "subscribe_grant_reduced", "non_monotone_plan_accepted")
    transfer = deepcopy(new)
    transfer["principals"][0]["client_ids"].remove("capteur-1")
    transfer["principals"][2]["client_ids"].append("capteur-1")
    variants["transfer"] = (transfer, "client_id_owner_changed", "foreign_owner_transfer_planned")
    removed = deepcopy(new)
    removed["principals"][1]["client_ids"].remove("dashboard-check")
    variants["remove-client"] = (removed, "client_id_removed", "client_removal_planned")
    removed_principal = deepcopy(new)
    removed_principal["principals"].pop(1)
    variants["remove-principal"] = (removed_principal, "principal_removed", "principal_removal_planned")
    results = {}
    for label, (value, reason, diagnosis) in variants.items():
        if which and label != which:
            continue
        changed = pki / f"{label}.json"
        write_json(changed, value)
        before = demo.inspect(case.db)
        refusal = case.expect("plan", label, "non_monotone_policy_change", new=changed, diagnosis=diagnosis)
        require(any(row["kind"] == reason for row in refusal["changes"]["refusals"]), "wrong_policy_refusal_cause")
        case.expect("apply", label, "non_monotone_policy_change", new=changed)
        require(demo.inspect(case.db) == before and not (case.out / f"{label}.sqlite").exists(),
                "denial_changed_state")
        results[label] = reason
    return results


def normal_refusals(binary, case, root, pki, old, new):
    result = refused_changes(case, pki, old, new)
    invalid = pki / "invalid.json"
    invalid.write_text("{")
    case.expect("plan", "invalid", "invalid_new_policy", new=invalid)
    case.expect("plan", "wrong-old", "old_policy_mismatch", old=pki / "next.json")
    for label, value in (
        ("duplicate-principal", {**new, "principals": [*new["principals"], new["principals"][0]]}),
        ("duplicate-pin", {**new, "principals": [*new["principals"][:2],
            {**new["principals"][2], "certificates": old["principals"][0]["certificates"]}]}),
    ):
        invalid_policy = pki / f"{label}.json"
        write_json(invalid_policy, value)
        case.expect("plan", label, "invalid_new_policy", new=invalid_policy)
    too_many = deepcopy(new)
    too_many["principals"][0]["client_ids"].extend(f"extra-{i}" for i in range(129))
    write_json(pki / "oversized.json", too_many)
    case.expect("plan", "over-capacity", "capacity_exceeded", new=pki / "oversized.json")

    corrupt = image(case.db, root / "corrupt.sqlite")
    with sqlite3.connect(corrupt) as sql:
        sql.execute("DELETE FROM security_session_owners WHERE session=(SELECT min(session) FROM security_session_owners)")
        require(sql.execute("PRAGMA quick_check").fetchone() == ("ok",), "corruption_control_not_semantic")
    corrupt_case = Case(binary, root / "corrupt", corrupt, case.old, case.new)
    invalid_row = corrupt_case.expect("plan", "corrupt", "invalid_durable_state")
    require(invalid_row["cause"] == "InvalidData", "registry_corruption_not_typed")
    result["corrupt_registry"] = corrupt_case.calls

    clear = broker_with(binary, root / "clear", None)
    try:
        clear.stop()
        before = hashlib.sha256(clear.db.read_bytes()).hexdigest()
        bad_schema = Case(binary, root / "legacy", clear.db, case.old, case.new)
        wrong = bad_schema.expect("plan", "legacy", "wrong_schema")
        require(wrong["schema_found"] == 12 and hashlib.sha256(clear.db.read_bytes()).hexdigest() == before,
                "wrong_schema_touched_file")
        result["legacy"] = bad_schema.calls
    finally:
        clear.cleanup()
    return result


def validate_public_recovery(binary, root, db, pki, claim):
    from importlib.metadata import version
    from mqttium import MQTTProtocolVersion
    from mqttium.api import AsyncClient, Properties
    import paho.mqtt.client as mqtt
    from paho.mqtt.properties import Properties as PahoProperties
    from paho.mqtt.packettypes import PacketTypes

    broker = broker_with(binary, root, pki, db=db, policy=pki / "next.json")
    receipts = queue.Queue(maxsize=8)
    connack = []
    ready = threading.Event()
    dashboard = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dashboard",
                            protocol=mqtt.MQTTv5, reconnect_on_failure=False)
    dashboard.tls_set_context(client_context(pki, "dashboard"))
    def on_connect(_client, _userdata, flags, reason, _props):
        connack.append((reason.value, flags.session_present))
        ready.set()
    dashboard.on_connect = on_connect
    dashboard.on_message = lambda _c, _d, msg: receipts.put_nowait((msg.topic, msg.payload, msg.qos, msg.mid, msg.dup))

    async def publish_new():
        async with AsyncClient("capteur-2", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False,
                               connect_properties=Properties({"session_expiry_interval": INFINITE})) as sensor:
            await sensor.connect(*broker.address, ssl=client_context(pki, "sensor-2"), timeout=4)
            sent = await sensor.publish(NEW_TOPIC, b"new-authorized", qos=1, timeout=4)
            await sent.wait(timeout=4)
            require(sent.is_done() and sent.exception() is None, "mqttium_new_sensor_not_admitted")

    try:
        properties = PahoProperties(PacketTypes.CONNECT)
        properties.SessionExpiryInterval = INFINITE
        dashboard.connect(*broker.address, clean_start=False, properties=properties)
        dashboard.loop_start()
        require(ready.wait(5) and connack == [(0, True)], "paho_durable_dashboard_not_recovered")
        first = receipts.get(timeout=6)
        require(first[:4] == (STATUS, b"seed-retained", 1, claim) and first[4],
                "outgoing_claim_identity_not_recovered")
        old = {receipts.get(timeout=6)[1] for _ in range(2)}
        require(old == {b"seed-inbound", b"armed-will"}, "old_responsibilities_not_recovered")
        wire = WireMetadata(root / "wire-metadata.jsonl")
        for clean in (False, True):
            with Peer(broker, "new-certificate-foreign-client", client_context(pki, "sensor-2"), log=wire) as foreign:
                foreign.open("capteur-1", clean=clean, reason=0x87)
        with Peer(broker, "legitimate-inbound-resume", client_context(pki), log=wire) as sensor:
            require(sensor.open("capteur-1"), "old_sensor_session_lost")
            sensor.send(packet(0x62, b"\x00\x11"))
            sensor.expect(0x70, 17)
            sensor.send(b"\xe0\x00")
        asyncio.run(publish_new())
        require(receipts.get(timeout=6)[:3] == (NEW_TOPIC, b"new-authorized", 1),
                "new_sensor_outside_existing_wildcard")
        drained = observed(broker, lambda: (lambda snapshot: snapshot if all(snapshot["counts"][key] == 0
                    for key in ("obligations", "inbound", "outbound", "wills")) else None)(demo.inspect(db)),
                    "migration_backlog_not_drained")
        return {"mqttium": client_identity(), "paho": version("paho-mqtt"), "session_present": True,
                "old_messages": 3, "resumed_claim_id": claim, "finished_inbound_id": 17,
                "new_sensor_received": True, "refused_client_id": [135, 135], "after": drained}
    finally:
        dashboard.disconnect()
        dashboard.loop_stop()
        broker.cleanup()


def apply_and_recover(binary, root, pki, case, seeded):
    old_plan = case.expect("plan", "main", "Planned")
    counts = old_plan["business"]["counts"]
    require(counts["exact_routes"] == counts["wildcard_routes"] == counts["claims"] == counts["inbound"] == 1
            and counts["will_armed"] == 1, "plan_missed_live_durable_state")
    pre = demo.inspect(case.db)
    try:
        result = case.expect("apply", "main", "Applied", diagnosis="transition_touched_mqtt_business_rows")
    except ReceiptFailure:
        require(demo.inspect(case.db) == pre, "business_mutation_escaped_rollback")
        write_json(case.out / "rollback.json", {"business_unchanged": True, "before": pre})
        raise
    after = demo.inspect(case.db)
    require(pre["tables"] == after["tables"], "mqtt_business_state_changed")
    restored = case.out / "main.sqlite"
    require(demo.inspect(restored) == pre and result["backup_sha256"] == hashlib.sha256(restored.read_bytes()).hexdigest(),
            "backup_missing_committed_wal")
    case.expect("apply", "main", "AlreadyApplied")
    recovered = validate_public_recovery(binary, root / "restarted", case.db, pki, seeded["claim_packet_id"])
    case.expect("apply", "main", "stale_plan")
    return {"applied": result, "backup_same_as_source_with_wal": True, "previous": pre,
            "after_transition": after, "recovered": recovered}


def crash_case(binary, root, db, pki, point):
    root.mkdir()
    case = Case(binary, root / "control", image(db, root / "database.sqlite"),
                pki / "identities.json", pki / "next.json")
    plan = case.expect("plan", "change", "Planned")
    before = demo.inspect(case.db)
    paused = case.argv("apply", "change") + ["--test-control", root, "--test-pause", point]
    with subprocess.Popen(paused, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:
        try:
            ready = root / f"ready-{point}"
            until = time.monotonic() + 10
            while not ready.is_file() and child.poll() is None and time.monotonic() < until:
                time.sleep(.005)
            require(ready.is_file() and child.poll() is None, "policy_crash_point_not_reached")
            child.kill()
            stdout, _ = child.communicate(timeout=5)
            require(child.returncode == -9 and not stdout, "policy_commit_result_not_lost")
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
    after = demo.inspect(case.db)
    cut = {"pid": child.pid, "signal": child.returncode, "point": point, "before": before,
           "after": after, "paused_command": [str(arg) for arg in paused]}
    write_json(root / "cut.json", cut)
    require(after["tables"] == before["tables"], "crash_changed_mqtt_responsibilities")
    expect_new = point == "policy.committed"
    require(after["owners"] == (sorted([*before["owners"], ("capteur-2", "sensor-2")]) if expect_new else before["owners"]),
            "partially_committed_authority")
    require(after["policy_sha256"] == plan["new_policy_sha256" if expect_new else "old_policy_sha256"],
            "partially_committed_authority")
    require(demo.inspect(case.out / "change.sqlite") == before, "crash_backup_did_not_preserve_wal")
    if expect_new:
        case.expect("apply", "change", "AlreadyApplied")
    else:
        argv = case.argv("apply", "change")
        argv[-1] = case.out / "retry.sqlite"
        require(case.invoke(argv)[1]["status"] == "Applied", "rolled_back_transition_did_not_retry")
    return {**cut, "commands": case.calls}


def maintenance_edges(binary, root, db, pki, old, new):
    root.mkdir()
    copied = image(db, root / "state.sqlite")
    case = Case(binary, root / "control", copied, pki / "identities.json", pki / "next.json")
    case.expect("plan", "initial", "Planned")
    original = demo.inspect(copied)
    invalid_path = case.argv("apply", "initial")
    invalid_path[-1] = Path(str(copied) + "-wal")
    require(case.invoke(invalid_path)[1]["code"] == "backup_failed" and demo.inspect(copied) == original,
            "failed_backup_changed_authority")
    public = root / "public"
    public.mkdir(mode=0o755)
    public.chmod(0o755)
    invalid_path[-1] = public / "backup.sqlite"
    require(case.invoke(invalid_path)[1]["code"] == "backup_failed" and not invalid_path[-1].exists(),
            "backup_directory_not_private")

    tampered = case.out / "initial.json"
    raw = tampered.read_text()
    document = json.loads(raw)
    document["body"]["business"]["counts"]["payloads"] = 0
    write_json(tampered, document)
    case.expect("apply", "initial", "invalid_plan")
    tampered.write_text(raw)
    rotated = deepcopy(old)
    rotated["principals"][0]["certificates"] = [pki_module.issue(pki, "rotated", "clientAuth")]
    rotation = pki / "rotate.json"
    write_json(rotation, rotated)
    case.expect("plan", "rotate", "Planned", new=rotation)
    case.expect("apply", "rotate", "Unchanged", new=rotation)
    require(demo.inspect(copied) == original and not (case.out / "rotate.sqlite").exists(),
            "credential_rotation_rewrote_database")

    active = broker_with(binary, root / "writer", pki, db=copied)
    try:
        case.expect("apply", "initial", "writer_alive")
    finally:
        active.stop()
    case.expect("apply", "initial", "stale_plan")

    wider = deepcopy(new)
    sensor = wider["principals"][0]
    sensor["client_ids"].append("capteur-extra")
    sensor["publish"] = ["atelier"]
    sensor["subscribe"] = ["atelier/capteur-1"]
    extension = pki / "extension.json"
    write_json(extension, wider)
    proposal = case.expect("plan", "extension", "Planned", new=extension)
    require({"client_id_added", "publish_prefix_added", "subscribe_prefix_added"} <=
            {row["kind"] for row in proposal["changes"]["additions"]}, "scope_expansion_not_reported")
    case.expect("plan", "other-authority", "Planned")
    case.expect("apply", "extension", "Applied", new=extension)
    case.expect("apply", "other-authority", "unknown_durable_authority")
    return case.calls


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--hooks", type=Path)
    parser.add_argument("--fixture-binary", type=Path)
    parser.add_argument("--mode", choices=("normal", "crash", "reduction", "transfer", "atomicity", "business"),
                        default="normal")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "crash" and args.hooks is None:
        parser.error("crash mode requires --hooks")
    os.umask(0o077)
    out = args.out.resolve()
    out.mkdir()
    scenario = args.mode
    binary = args.binary.resolve()
    result = {"status": "FAIL", "scenario": scenario, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    try:
        with tempfile.TemporaryDirectory(prefix="mqttiumd-policy-receive-") as temp:
            pki = Path(temp) / "pki"
            old, new = credentials(pki)
            seed_binary = args.hooks if scenario == "crash" else args.fixture_binary or binary
            db, witnessed = seed(seed_binary.resolve(), out / "seed", pki, pending_will=scenario == "crash")
            result["seed"] = witnessed
            case = Case(binary, out / "control", db, pki / "identities.json", pki / "next.json")
            if scenario in ("reduction", "transfer"):
                result["refusals"] = refused_changes(case, pki, old, new, scenario)
            elif scenario in ("crash", "atomicity"):
                plan = case.expect("plan", "will-states", "Planned")
                if scenario == "crash":
                    require(plan["business"]["counts"]["will_armed"] == plan["business"]["counts"]["will_pending"] == 1,
                            "plan_did_not_inventory_both_will_states")
                check = args.hooks.resolve() if scenario == "crash" else binary
                result["crashes"] = [crash_case(check, out / "before-commit", db, pki, "policy.fingerprint_staged")]
                if scenario == "crash":
                    result["crashes"].append(crash_case(check, out / "after-commit", db, pki, "policy.committed"))
            else:
                if scenario == "normal":
                    result["refusals"] = normal_refusals(binary, case, out, pki, old, new)
                    result["maintenance"] = maintenance_edges(binary, out / "maintenance", db, pki, old, new)
                result["transition"] = apply_and_recover(binary, out, pki, case, witnessed)
            result["commands"] = case.calls
        result["status"] = "PASS"
    except ReceiptFailure as error:
        result["failure"] = {"code": error.code}
        result["commands"] = case.calls if "case" in locals() else []
        raise
    finally:
        write_json(out / "receipt.json", result)
    print(json.dumps({"status": result["status"], "scenario": scenario, "receipt": str(out / "receipt.json")}))


if __name__ == "__main__":
    main()
