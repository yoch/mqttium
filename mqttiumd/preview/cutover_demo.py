"""Wire and storage observations across the same-address Mosquitto cutover."""
import asyncio
from contextlib import closing, contextmanager
import json
from pathlib import Path
import socket
import subprocess
import sys

from mosquitto_example import prepare


ROOT = "atelier/capteur-1"
FENCE = ROOT + "/fence"
STATE = ROOT + "/state"


def subscribe(d, wire, filters):
    wire.send(0x82, b"\0\1" + b"".join(d.field(topic) + b"\1" for topic in filters))
    frame = wire.receive()
    d.require(frame and frame[0] == 0x90 and frame[1][:2] == b"\0\1"
              and len(frame[1]) == 2 + len(filters), "cutover_suback_shape")
    return list(frame[1][2:])


def through_fence(d, wire):
    received = []
    # A subscribed marker follows the trial publications from the same sender.
    # It bounds the observation even when the tested filter was refused.
    for _ in range(6):
        frame = wire.receive()
        d.require(frame and frame[0] in (0x32, 0x33), "cutover_expected_delivery")
        data = frame[1]
        end = 2 + int.from_bytes(data[:2], "big")
        topic, pid, payload = data[2:end].decode(), data[end:end + 2], data[end + 2:].decode()
        d.require(len(pid) == 2 and int.from_bytes(pid, "big") > 0, "cutover_delivery_id")
        wire.send(0x40, pid)
        received.append([topic, payload, frame[0] & 1])
        if (topic, payload) == (FENCE, "end-of-trial"):
            return received
    raise ValueError("cutover_unexpected_delivery_count")


def publish_sequence(d, port, pki, messages):
    with closing(d.Wire311(pki, port, "sensor")) as pub:
        d.acl_connect(pub, 4, client_id="capteur-1")
        for mid, (topic, payload, retain) in enumerate(messages, 1):
            packet_id = mid.to_bytes(2, "big")
            pub.send(0x33 if retain else 0x32, d.field(topic) + packet_id + payload.encode())
            pub.expect(0x40, packet_id)
        pub.send(0xe0)


def observation(d, port, pki, filters, messages):
    with closing(d.Wire311(pki, port, "dashboard")) as sub:
        d.acl_connect(sub, 4)
        grants = subscribe(d, sub, [*filters, FENCE])
        publish_sequence(d, port, pki, [*messages, (FENCE, "end-of-trial", False)])
        frames = through_fence(d, sub)
        sub.send(0xe0)
    d.require(grants[-1] == 1 and frames[-1] == [FENCE, "end-of-trial", 0], "cutover_fence_missing")
    return dict(filters=filters, suback=grants[:-1], received=frames[:-1], fence=frames[-1])


def compare_acl(d, pki, port, mosquitto):
    offered = [("atelier/a", "allowed", False), ("atelier/a/enfant", "child", False)]
    exact = observation(d, port, pki, ["atelier/a", "atelier/a/enfant"], offered)
    broad = observation(d, port, pki, ["atelier/#"], offered)
    allowed = [["atelier/a", "allowed", 0]]
    d.require(exact["suback"] == ([1, 1] if mosquitto else [1, 128])
              and exact["received"] == allowed, "cutover_literal_read_widened")
    d.require(broad["suback"] == ([1] if mosquitto else [128])
              and broad["received"] == (allowed if mosquitto else []), "cutover_broad_grant_or_delivery")
    # Both servers check the leaf for mTLS. The Mosquitto CN identifies an
    # ACL username. mqttiumd additionally checks ownership of the ClientID.
    with closing(d.Wire311(pki, port, "sensor")) as wire:
        wire.send(0x10, d.field("MQTT") + b"\5\2\0\x1e\0" + d.field("dashboard"))
        connack = wire.receive()
        d.require(connack and connack[0] == 0x20 and connack[1][1] == (0 if mosquitto else 0x87),
                  "cutover_unexpected_clientid_policy")
        if mosquitto:
            wire.send(0xe0)
    reasons = []
    with closing(d.Wire311(pki, port, "dashboard")) as writer:
        d.acl_connect(writer, 5)
        for index, topic in enumerate(("commande/point", "commande/point/enfant"), 1):
            mid = index.to_bytes(2, "big")
            writer.send(0x32, d.field(topic) + mid + b"\0command")
            ack = writer.receive()
            if index == 2 and not mosquitto:
                d.require(ack and ack[0] == 0xe0 and ack[1][:1] == b"\x87", "cutover_expected_acl_disconnect")
                reasons.append(["DISCONNECT", ack[1][0]])
            else:
                d.require(ack and ack[0] == 0x40 and ack[1][:2] == mid, "cutover_expected_publish_ack")
                reasons.append(["PUBACK", ack[1][2] if len(ack[1]) > 2 else 0])
        if mosquitto:
            writer.send(0xe0)
    d.require(reasons[0][1] in (0, 0x10) and reasons[1][1] == 0x87, "cutover_literal_write_widened")
    return {"exact_and_child": exact, "broad": broad, "exact_write_responses": reasons,
            "sensor_cn_dashboard_clientid_connack": connack[1][1]}


def qos2(d, clients, port, service):
    topic, payload = ROOT + "/handoff", "same listener qos2"
    with clients.subscribe(port, topic, service + "-handoff", qos=2) as sub:
        publisher = clients.publish(port, topic, payload, qos=2)
        subscriber = d.mosquitto_result(sub, topic, payload, qos=2, granted=2)
    return {"publisher": publisher, "subscriber": subscriber}


@contextmanager
def checked_service(d, out, plan, data, name, record):
    before = set(data.glob("run.*/events.jsonl"))
    with d.process([sys.executable, d.PACKAGE / "mosquitto-config.py", "start", plan],
                   out / (name + ".log"), broker=True) as child:
        def ready():
            d.require(child.poll() is None, "checked_start_exited")
            for log in set(data.glob("run.*/events.jsonl")) - before:
                for row in d.events(log):
                    if row["kind"] == "ready":
                        return log, row["details"]["address"]
        log, address = d.wait_for(ready, "checked_start_not_ready")
        binary = d.sha(Path(f"/proc/{child.pid}/exe"))
        d.require(binary == record["binary_sha256"], "cutover_binary_mismatch")
        db = (data / "broker.sqlite").stat()
        record["starts"].append(dict(name=name, pid=child.pid, port=int(address.rsplit(":", 1)[1]),
                                     binary_sha256=binary, sqlite=[db.st_dev, db.st_ino]))
        yield log
    (out / (name + ".jsonl")).write_bytes(log.read_bytes())


def verify_config(d, tool, conf, bindings, data):
    command = [sys.executable, tool, "check", conf, bindings, data]
    result = subprocess.run(command, capture_output=True, timeout=d.TIMEOUT)
    d.require(result.stdout, "checker did not produce a plan: " + result.stderr.decode(errors="replace"))
    return result.returncode, json.loads(result.stdout)


def check_destinations(d, tool, conf, bindings, site, pki, port, result):
    occupied = site / "occupied"
    occupied.mkdir(mode=0o700)
    sentinel = occupied / "operator-file"
    sentinel.write_bytes(b"leave this file alone")
    absent = site / "absent-parent"
    tests = [(occupied, "destination_occupied"),
             (absent / "candidate", "destination_parent_missing")]
    trials = result["destination_preflight"] = {}
    source = site / "mosquitto-data/mosquitto.db"
    original = d.sha(source) if source.exists() else None
    with closing(d.Wire311(pki, port, "dashboard")) as wire:
        d.acl_connect(wire, 4)
        for destination, fault in tests:
            rc, plan = verify_config(d, tool, conf, bindings, destination)
            wire.send(0xc0)
            wire.expect(0xd0, b"")
            trials[fault] = {
                "exit_status": rc,
                "status": plan["status"],
                "diagnostics": [row for row in plan["diagnostics"] if row["status"] == "blocking"],
                "original_service_pingresp": True,
            }
        wire.send(0xe0)
    d.require((d.sha(source) if source.exists() else None) == original,
              "destination_checks_changed_source_store")
    d.require(list(occupied.iterdir()) == [sentinel] and sentinel.read_bytes() == b"leave this file alone"
              and not absent.exists(), "destination_checks_changed_candidate_storage")
    bad = [fault for fault, observed in trials.items() if observed["exit_status"] != 2
           or not any(row.get("code") == fault for row in observed["diagnostics"])]
    d.require(not bad, "destination_preflight_expected_refusals:" + ",".join(bad))


def check_sealing(d, tool, conf, bindings, data):
    controls = {
        "python_memfd_absent": "del os.memfd_create",
        "kernel_memfd_unavailable": "os.memfd_create = unavailable",
    }
    findings = {}
    for name, control in controls.items():
        driver = ("import errno, os, runpy, sys\n"
                  "def unavailable(*args):\n    raise OSError(errno.ENOSYS, 'memfd')\n"
                  + control + "\n"
                  "sys.argv = sys.argv[1:]\n"
                  "sys.path.insert(0, os.path.dirname(sys.argv[0]))\n"
                  "runpy.run_path(sys.argv[0], run_name='__main__')\n")
        checked = subprocess.run([sys.executable, "-c", driver, tool, "check", conf, bindings, data],
                                 capture_output=True, timeout=d.TIMEOUT)
        report = json.loads(checked.stdout)
        refused = [row for row in report["diagnostics"] if row.get("code") == "sealing_unavailable"]
        d.require(checked.returncode == 2 and report["status"] == "blocked"
                  and len(refused) == 1 and not data.exists(), "sealing_fallback_or_missing_diagnostic:" + name)
        findings[name] = refused[0]
    return findings


def exercise_cutover(d, out, pki, reference, record, clients):
    site = out / "switch"
    with socket.socket() as reserved:
        reserved.bind((d.HOST, 0))
        port = reserved.getsockname()[1]
    conf, bindings = prepare(site, pki, port)
    data, plan = site / "candidate", out / "checked-mosquitto.json"
    tool = d.PACKAGE / "mosquitto-config.py"
    rc, effective = verify_config(d, tool, conf, bindings, data)
    d.require(rc == 0 and effective["status"] == "compatible" and not data.exists(), "offline_configuration_check")
    plan.write_text(json.dumps(effective, indent=2) + "\n")
    result = record["cutover"] = {"address": effective["effective"]["source"]["listener"],
                                  "plan_sha256": effective["plan_sha256"]}
    result["sealing_controls"] = check_sealing(d, tool, conf, bindings, data)
    data.mkdir(mode=0o700)
    sentinel = data / "operator-file"
    sentinel.write_bytes(b"created after check")
    refused = subprocess.run([sys.executable, tool, "start", plan], capture_output=True, timeout=d.TIMEOUT)
    d.require(refused.returncode == 2 and list(data.iterdir()) == [sentinel]
              and sentinel.read_bytes() == b"created after check", "start_changed_destination_after_check")
    diagnostics = json.loads(refused.stderr)["diagnostics"]
    d.require(any(row.get("code") == "destination_occupied" for row in diagnostics),
              "start_did_not_recheck_destination")
    result["destination_changed_after_check"] = diagnostics
    sentinel.unlink()
    data.rmdir()
    wrong = site / "wrong-bindings.txt"
    wrong.write_text(bindings.read_text().replace("sensor.crt", "dashboard.crt"))
    rc, mismatch = verify_config(d, tool, conf, wrong, data)
    rejects = [r for r in mismatch["diagnostics"] if r["status"] == "blocking"]
    d.require(rc == 2 and len(rejects) == 1 and rejects[0]["file"] == str(wrong)
              and rejects[0]["line"] == 2, "CN_binding_mismatch_lost_provenance")
    result["wrong_certificate_binding"] = rejects[0]
    conf_text = conf.read_text()
    bad = site / "unsupported.conf"
    bad.write_text(conf_text + "include_dir /etc/mosquitto/conf.d\n")
    rc, problem = verify_config(d, tool, bad, bindings, data)
    rejects = [row for row in problem["diagnostics"] if row["status"] == "blocking"]
    d.require(rc == 2 and len(rejects) == 1 and rejects[0]["file"] == str(bad)
              and rejects[0]["line"] == len(conf_text.splitlines()) + 1, "unknown_directive_provenance")
    result["unknown_directive"] = rejects[0]
    acl = site / "access.acl"
    original_acl = acl.read_text()
    acl.write_text(original_acl + "pattern read %u/#\n")
    refused = subprocess.run([sys.executable, tool, "start", plan], capture_output=True, timeout=d.TIMEOUT)
    d.require(refused.returncode == 2 and not data.exists(), "changed_acl_opened_candidate")
    blocked = [r for r in json.loads(refused.stderr)["diagnostics"] if r["status"] == "blocking"]
    d.require(len(blocked) == 1 and blocked[0]["file"] == str(acl)
              and blocked[0]["line"] == len(original_acl.splitlines()) + 1, "ambiguous_acl_provenance")
    result["changed_acl"] = blocked[0]
    acl.write_text(original_acl)
    # A compatible edit also requires the operator to issue a new plan.
    conf.write_text(conf_text + "# changed since verification\n")
    stale = subprocess.run([sys.executable, tool, "start", plan], capture_output=True, timeout=d.TIMEOUT)
    d.require(stale.returncode == 2 and not data.exists(), "stale_compatible_plan_started")
    conf.write_text(conf_text)
    d.require(not data.exists(), "offline_diagnostics_touched_data")
    with d.reference_service(conf, reference, out / "mosquitto-original.log"):
        check_destinations(d, tool, conf, bindings, site, pki, port, result)
        result["mosquitto_qos2"] = qos2(d, clients, port, "original")
        result["mosquitto_acl"] = compare_acl(d, pki, port, True)
        publish_sequence(d, port, pki, [(STATE, "original-state", True)])
        d.require(observation(d, port, pki, [STATE], [])["received"] == [[STATE, "original-state", 1]],
                  "original_state_not_committed")
    source_db = site / "mosquitto-data/mosquitto.db"
    original_digest = d.sha(source_db)
    with checked_service(d, out, plan, data, "checked-start", record) as log:
        d.require(record["starts"][-1]["port"] == port, "cutover_changed_listener")
        result["mqttiumd_qos2"] = qos2(d, clients, port, "candidate")
        for role in ("publisher", "subscriber"):
            d.require(result["mosquitto_qos2"][role]["argv"] == result["mqttiumd_qos2"][role]["argv"],
                      "cutover_changed_client_arguments")
        result["mqttiumd_acl"] = compare_acl(d, pki, port, False)
        result["cross_protocol"] = asyncio.run(d.mosquitto_to_mqttium(
            port, log, clients, pki, data / "broker.sqlite", 2, granted=2))
        d.require(observation(d, port, pki, [STATE], [])["received"] == [], "candidate_imported_source_state")
        publish_sequence(d, port, pki, [(STATE, "candidate-state", True)])
    d.require(d.sha(source_db) == original_digest, "cutover_modified_original_store")
    candidate_digest = d.sha(data / "broker.sqlite")
    with d.reference_service(conf, reference, out / "mosquitto-rollback.log"):
        result["rollback_qos2"] = qos2(d, clients, port, "rollback")
        result["original_after_rollback"] = observation(d, port, pki, [STATE], [])
        d.require(result["original_after_rollback"]["received"] == [[STATE, "original-state", 1]],
                  "rollback_did_not_recover_original_state")
    d.require(d.sha(data / "broker.sqlite") == candidate_digest, "rollback_modified_candidate_store")
    with checked_service(d, out, plan, data, "checked-restart", record):
        result["candidate_after_rollback"] = observation(d, port, pki, [STATE], [])
        d.require(result["candidate_after_rollback"]["received"] == [[STATE, "candidate-state", 1]],
                  "candidate_lost_trial_state")
    result.update(original_database_sha256_during_trial=original_digest,
                  candidate_database_sha256_during_rollback=candidate_digest, status="PASS")
