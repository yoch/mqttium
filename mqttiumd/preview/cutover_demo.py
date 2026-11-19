"""Wire and storage observations across the same-address Mosquitto cutover."""
import asyncio
from contextlib import closing, contextmanager
import json
from pathlib import Path
import signal
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


def verify_config(d, tool, conf, bindings, data, *, after_stop=False):
    command = [sys.executable, tool, "check", conf, bindings, data]
    if after_stop:
        command.append("--after-stop")
    result = subprocess.run(command, capture_output=True, timeout=d.TIMEOUT)
    d.require(result.stdout, "checker did not produce a plan: " + result.stderr.decode(errors="replace"))
    return result.returncode, json.loads(result.stdout)


def save_plan(out, name, plan):
    path = out / (name + ".json")
    path.write_text(json.dumps(plan, indent=2) + "\n")
    return path


def blocked_start(d, tool, plan):
    refused = subprocess.run([sys.executable, tool, "start", plan], capture_output=True, timeout=d.TIMEOUT)
    d.require(refused.returncode == 2, "unexpected_start_result:" + refused.stderr.decode(errors="replace"))
    return json.loads(refused.stderr)


def save_source(d, process, source):
    before = source.stat().st_ino if source.exists() else None
    process.send_signal(signal.SIGUSR1)
    d.wait_for(lambda: source.exists() and source.stat().st_ino != before,
               "mosquitto_did_not_publish_manual_save")


def inventory_plan(d, tool, conf, bindings, data, source, *, decision, after_stop=False):
    rc, plan = verify_config(d, tool, conf, bindings, data, after_stop=after_stop)
    expected_rc = 0 if decision in ("ready", "recheck_after_stop") else 2
    d.require(rc == expected_rc and plan["status"] == "compatible"
              and plan["decision"]["status"] == decision, "unexpected_cutover_decision:" + repr(plan["decision"]))
    inventory = plan["inventory"]
    d.require(inventory["status"] == "captured" and inventory["database_version"] == 6
              and inventory["sha256"] == d.sha(source) and inventory["captured_at"],
              "source_snapshot_identity")
    return plan


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


def inspect_source_refusals(d, tool, conf, bindings, site, source, result):
    contents = source.read_bytes()
    outcomes = result["source_refusals"] = {}
    for expected in ("unknown_format", "unreadable", "invalid_format"):
        directory = site / expected
        directory.mkdir()
        copy = directory / source.name
        if expected == "unreadable":
            copy.mkdir()
        elif expected == "unknown_format":
            copy.write_bytes(contents[:19] + (7).to_bytes(4, "big") + contents[23:])
        else:
            copy.write_bytes(contents[:-1])
        fixture = site / (expected + ".conf")
        fixture.write_text(conf.read_text().replace(str(source.parent) + "/", str(directory) + "/"))
        data = site / (expected + "-candidate")
        rc, plan = verify_config(d, tool, fixture, bindings, data, after_stop=True)
        inventory = plan["inventory"]
        d.require(rc == 2 and inventory["status"] == expected and "counts" not in inventory
                  and plan["decision"]["reason"] == "source_inventory_unavailable"
                  and not data.exists(), "source_refusal_did_not_block:" + expected)
        outcomes[expected] = inventory
    d.require(source.read_bytes() == contents, "source_refusal_changed_original")


def demonstrate_drain(d, out, pki, reference, tool, port, record):
    site = out / "drain-site"
    conf, bindings = prepare(site, pki, port)
    db, data = site / "mosquitto-data/mosquitto.db", site / "candidate"
    payload = "retained-and-owed"
    counts = {
        "persistent_sessions": 1, "subscriptions": 1, "stored_messages": 1,
        "stored_payload_bytes": len(payload.encode()), "retained_values": 1,
        "incoming_queued": 0, "incoming_inflight": 0,
    }
    with d.reference_service(conf, reference, out / "mosquitto-drain.log") as original:
        with closing(d.Wire311(pki, port, "dashboard")) as sub:
            d.acl_connect(sub, 4, persistent=True)
            d.require(subscribe(d, sub, [STATE]) == [1], "drain_site_suback")
            sub.send(0xe0)
        # Prove the disconnect reached Mosquitto before the publisher sends.
        log = out / "mosquitto-drain.log"
        d.wait_for(lambda: "Client dashboard disconnected" in log.read_text(), "drain_site_dashboard_still_online")
        publish_sequence(d, port, pki, [(STATE, payload, True)])
        save_source(d, original, db)
        queued = inventory_plan(d, tool, conf, bindings, data, db, decision="drain_required")
        d.require(queued["inventory"]["counts"] == dict(counts, outgoing_queued=1, outgoing_inflight=0)
                  and queued["inventory"]["pending_by_declared_client"] == {"dashboard": {"outgoing_queued": 1}}
                  and not data.exists(), "source_queue_or_shared_store_count")
        pending = save_plan(out, "mosquitto-drain-required", queued)
        d.require(blocked_start(d, tool, pending)["decision"]["status"] == "drain_required"
                  and not data.exists(), "start_ignored_source_queue")
        with closing(d.Wire311(pki, port, "dashboard")) as sub:
            d.acl_connect(sub, 4, persistent=True, resume=True)
            packet = d.decode_delivery(sub.receive(), 4, STATE, payload.encode(), 1)
            save_source(d, original, db)
            inflight = inventory_plan(d, tool, conf, bindings, data, db, decision="drain_required")
            d.require(inflight["inventory"]["counts"] == dict(counts, outgoing_queued=0, outgoing_inflight=1),
                      "source_inflight_not_distinguished_from_queue")
            save_plan(out, "mosquitto-inflight", inflight)
            sub.send(0x40, packet)
            sub.send(0xc0)
            sub.expect(0xd0, b"")
            sub.send(0xe0)
        save_source(d, original, db)
        drained = inventory_plan(d, tool, conf, bindings, data, db, decision="recheck_after_stop")
        d.require(drained["inventory"]["counts"] == dict(counts, outgoing_queued=0, outgoing_inflight=0),
                  "source_ack_did_not_drain_exactly_one_reference")
        save_plan(out, "mosquitto-drained-in-service", drained)
    ready = inventory_plan(d, tool, conf, bindings, data, db, decision="ready", after_stop=True)
    d.require(ready["inventory"]["counts"] == drained["inventory"]["counts"], "source_stop_changed_drained_totals")
    plan = save_plan(out, "mosquitto-drained-after-stop", ready)
    original_sha = d.sha(db)
    with checked_service(d, out, plan, data, "drained-site-start", record):
        d.require(observation(d, port, pki, [STATE], [])["received"] == [], "drained_site_imported_retained")
    d.require(d.sha(db) == original_sha, "drained_site_candidate_modified_source")
    with d.reference_service(conf, reference, out / "mosquitto-drained-rollback.log"):
        replay = observation(d, port, pki, [STATE], [])["received"]
        d.require(replay == [[STATE, payload, 1]], "drained_retained_missing_on_return")
    record["cutover"]["drain_site"] = {
        "queued": queued["decision"], "inflight": inflight["decision"], "after_ack": drained["decision"],
        "after_stop": ready["decision"], "original_db_sha256_during_trial": original_sha,
        "delivery": {"topic": STATE, "payload": payload, "packet_id": int.from_bytes(packet, "big"),
                     "session_present": True, "puback": True},
        "retained_after_return": replay,
    }


def exercise_cutover(d, out, pki, reference, record, clients):
    site = out / "switch"
    with socket.socket() as reserved:
        reserved.bind((d.HOST, 0))
        port = reserved.getsockname()[1]
    conf, bindings = prepare(site, pki, port)
    data = site / "candidate"
    source_db = site / "mosquitto-data/mosquitto.db"
    tool = d.PACKAGE / "mosquitto-config.py"
    rc, absent = verify_config(d, tool, conf, bindings, data)
    d.require(rc == 2 and absent["status"] == "compatible" and absent["inventory"]["status"] == "absent"
              and absent["decision"]["reason"] == "source_inventory_unavailable"
              and not data.exists() and not source_db.exists(), "missing_source_was_treated_as_empty")
    result = record["cutover"] = {"address": absent["effective"]["source"]["listener"],
                                  "source_absent": absent["inventory"]}
    result["sealing_controls"] = check_sealing(d, tool, conf, bindings, data)
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
    with d.reference_service(conf, reference, out / "mosquitto-original.log") as original:
        result["mosquitto_qos2"] = qos2(d, clients, port, "original")
        result["mosquitto_acl"] = compare_acl(d, pki, port, True)
        publish_sequence(d, port, pki, [(STATE, "original-state", True)])
        d.require(observation(d, port, pki, [STATE], [])["received"] == [[STATE, "original-state", 1]],
                  "original_state_not_committed")
        save_source(d, original, source_db)
        check_destinations(d, tool, conf, bindings, site, pki, port, result)
        before = inventory_plan(d, tool, conf, bindings, data, source_db, decision="recheck_after_stop")
        d.require(before["inventory"]["saved_on"] == "autosave_or_manual_save", "wrong_running_capture_boundary")
        provisional = save_plan(out, "provisional-mosquitto", before)
        d.require(blocked_start(d, tool, provisional)["decision"]["status"] == "recheck_after_stop"
                  and not data.exists(), "provisional_plan_started_candidate")
        rc, stale_save = verify_config(d, tool, conf, bindings, data, after_stop=True)
        d.require(rc == 2 and stale_save["decision"]["status"] == "recheck_after_stop", "autosave_authorized_start")
    prepared = inventory_plan(d, tool, conf, bindings, data, source_db, decision="ready", after_stop=True)
    original_digest = d.sha(source_db)
    d.require(prepared["inventory"]["saved_on"] == "clean_shutdown" and original_digest != before["inventory"]["sha256"]
              and prepared["inventory"]["counts"]["retained_values"] == 1, "final_save_did_not_capture_source")
    plan = save_plan(out, "checked-mosquitto", prepared)
    result["plan_sha256"] = prepared["plan_sha256"]
    result["final_decision"] = prepared["decision"]
    result["running_source"] = before["inventory"]
    result["stopped_source"] = prepared["inventory"]

    data.mkdir(mode=0o700)
    sentinel = data / "operator-file"
    sentinel.write_bytes(b"created after check")
    diagnostics = blocked_start(d, tool, plan)["diagnostics"]
    d.require(list(data.iterdir()) == [sentinel] and sentinel.read_bytes() == b"created after check",
              "start_changed_destination_after_check")
    d.require(any(row.get("code") == "destination_occupied" for row in diagnostics),
              "start_did_not_recheck_destination")
    result["destination_changed_after_check"] = diagnostics
    sentinel.unlink()
    data.rmdir()
    acl = site / "access.acl"
    original_acl = acl.read_text()
    acl.write_text(original_acl + "pattern read %u/#\n")
    blocked = [r for r in blocked_start(d, tool, plan)["diagnostics"] if r["status"] == "blocking"]
    d.require(len(blocked) == 1 and blocked[0]["file"] == str(acl)
              and blocked[0]["line"] == len(original_acl.splitlines()) + 1, "ambiguous_acl_provenance")
    result["changed_acl"] = blocked[0]
    acl.write_text(original_acl)
    # A compatible edit also requires the operator to issue a new plan.
    conf.write_text(conf_text + "# changed since verification\n")
    d.require(blocked_start(d, tool, plan)["decision"]["status"] == "ready"
              and not data.exists(), "stale_compatible_plan_started")
    conf.write_text(conf_text)
    d.require(not data.exists(), "offline_diagnostics_touched_data")
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
        publish_sequence(d, port, pki, [(ROOT + "/rollback", "source-only", True)])
    d.require(d.sha(data / "broker.sqlite") == candidate_digest, "rollback_modified_candidate_store")
    refused = blocked_start(d, tool, plan)
    d.require(refused["error"] == "source_snapshot_changed" and d.sha(data / "broker.sqlite") == candidate_digest,
              "old_source_snapshot_allowed_restart")
    result["stale_source_snapshot"] = refused["error"]
    updated = inventory_plan(d, tool, conf, bindings, data, source_db, decision="ready", after_stop=True)
    reopen = save_plan(out, "checked-mosquitto-reopen", updated)
    with checked_service(d, out, reopen, data, "checked-restart", record):
        d.require(record["starts"][-2]["sqlite"] == record["starts"][-1]["sqlite"],
                  "candidate_restart_switched_database")
        result["candidate_after_rollback"] = observation(d, port, pki, [STATE], [])
        d.require(result["candidate_after_rollback"]["received"] == [[STATE, "candidate-state", 1]],
                  "candidate_lost_trial_state")
    result.update(original_database_sha256_during_trial=original_digest,
                  candidate_database_sha256_during_rollback=candidate_digest, status="PASS")
    inspect_source_refusals(d, tool, conf, bindings, site, source_db, result)
    demonstrate_drain(d, out, pki, reference, tool, port, record)
