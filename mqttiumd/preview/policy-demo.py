#!/usr/bin/env python3
"""Onboard a second mTLS sensor and receive the old backlog on the same file."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("demo_pki", HERE / "demo-pki.py")
pki = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pki)


def expect(ok, code):
    if not ok:
        raise ValueError(code)


def inspect(db):
    # Independent read of the tiny demonstration data set. Keep lengths and
    # hashes in the receipt, never message bytes or certificate contents.
    business = {
        "meta": "one", "sessions": "id", "routes": "topic,position",
        "retained": "topic", "publications": "id", "payloads": "id",
        "obligations": "publication,idx", "outbound": "publication,idx",
        "inbound": "session,packet_id", "wills": "id", "session_clock": "one",
        "operations": "id", "fences": "id", "receipt_origins": "epoch,owner",
        "security_session_owners": "session",
    }
    with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as sql:
        sql.execute("BEGIN")
        hashes, counts = {}, {}
        for name, order in business.items():
            digest = hashlib.sha256()
            count = 0
            for row in sql.execute(f"SELECT * FROM {name} ORDER BY {order}"):
                data = repr(row).encode()
                digest.update(len(data).to_bytes(8, "big"))
                digest.update(data)
                count += 1
            hashes[name] = digest.hexdigest()
            counts[name] = count
        return {
            "schema": sql.execute("PRAGMA user_version").fetchone()[0],
            "policy_sha256": sql.execute("SELECT hex(fingerprint) FROM security_policy").fetchone()[0].lower(),
            "owners": sql.execute("SELECT CAST(client_id AS TEXT),principal FROM security_client_owners ORDER BY client_id").fetchall(),
            "sessions": sql.execute("SELECT CAST(client_id AS TEXT),hex(id) FROM sessions ORDER BY client_id").fetchall(),
            "counts": counts, "tables": hashes,
        }


def command(argv, calls, *, data=None):
    result = subprocess.run(argv, input=data, capture_output=True, timeout=12, check=False)
    calls.append({"argv": [str(arg) for arg in argv], "exit": result.returncode})
    return result


@contextmanager
def broker(binary, db, out, credentials, policy, label, calls, *, crash=False):
    events = out / f"{label}.events.jsonl"
    argv = [binary, "--listen", "127.0.0.1:0", "--db", db, "--events", events,
            "--tls-cert", credentials / "server.crt", "--tls-key", credentials / "server.key",
            "--client-ca", credentials / "ca.crt", "--identities", policy]
    record = {"argv": [str(arg) for arg in argv]}
    calls.append(record)
    with subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as child:
        record["pid"] = child.pid
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                expect(child.poll() is None, "broker_exited_before_ready")
                if events.is_file():
                    for line in events.read_text().splitlines():
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if event["kind"] == "ready":
                            expect(event["details"]["profile"] == "mtls-will-pubsub-v1",
                                   "secure_profile_missing")
                            yield int(event["details"]["address"].rsplit(":", 1)[1])
                            return
                time.sleep(.01)
            raise ValueError("broker_not_ready")
        finally:
            if child.poll() is None:
                if crash:
                    child.kill()
                else:
                    child.terminate()
            try:
                record["exit"] = child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                record["exit"] = child.wait(timeout=3)
            expect(record["exit"] == (-9 if crash else 0), "broker_shutdown_failed")


def mqtt_session(port, credentials, device, client_id):
    """Check Session Present independently of the Mosquitto console format."""
    context = ssl.create_default_context(cafile=credentials / "ca.crt")
    context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(credentials / f"{device}.crt", credentials / f"{device}.key")
    ident = client_id.encode()
    body = b"\x00\x04MQTT\x05\x00\x00\x1e\x05\x11\xff\xff\xff\xff"
    body += len(ident).to_bytes(2, "big") + ident
    expect(len(body) < 128, "demo_connect_length")
    with socket.create_connection(("127.0.0.1", port), timeout=4) as tcp:
        with context.wrap_socket(tcp, server_hostname="127.0.0.1") as tls:
            tls.sendall(bytes((0x10, len(body))) + body)
            with tls.makefile("rb") as wire:
                expect(wire.read(1) == b"\x20", "expected_connack")
                length = 0
                for power in range(4):
                    byte, = wire.read(1)
                    length += (byte & 127) << (7 * power)
                    if byte < 128:
                        break
                expect(3 <= length <= 512, "demo_connack_length")
                flags, reason, *_ = wire.read(length)
                if reason == 0:
                    tls.sendall(b"\xe0\x00")
                return {"present": bool(flags & 1), "reason": reason, "transport": tls.version()}


def run(binary: Path, out: Path):
    calls = []
    old_topic = "atelier/capteur-1/status"
    new_topic = "atelier/capteur-2/status"
    db = out / "broker.sqlite"
    out.mkdir(mode=0o700)
    backup_dir = out / "private-backups"
    backup_dir.mkdir(mode=0o700)
    backup = backup_dir / "before-onboarding.sqlite"
    with tempfile.TemporaryDirectory(prefix="mqttiumd-policy-demo-") as temp:
        credentials = Path(temp) / "pki"
        old = pki.generate(credentials)
        old_file = out / "old-identities.json"
        old_file.write_text(json.dumps(old, indent=2) + "\n")
        next_pin = pki.issue(credentials, "sensor-2", "clientAuth")
        new = json.loads(json.dumps(old))
        new["principals"].append({
            "name": "sensor-2", "certificates": [next_pin], "client_ids": ["capteur-2"],
            "publish": ["atelier/capteur-2"], "subscribe": ["atelier/capteur-2/command"],
        })
        new_file = out / "new-identities.json"
        new_file.write_text(json.dumps(new, indent=2) + "\n")

        def mosq(program, port, device, client_id, *args, data=None):
            return command([program, "-V", "mqttv5", "-h", "127.0.0.1", "-p", str(port),
                "--tls-version", "tlsv1.3", "--cafile", credentials / "ca.crt",
                "--cert", credentials / f"{device}.crt", "--key", credentials / f"{device}.key",
                "-i", client_id, *args], calls, data=data)

        def publish(port, device, client_id, topic, payload, retain=False):
            args = ["-d", "-c", "-x", "4294967295", "-q", "1", "-t", topic, "-s"]
            if retain:
                args.append("-r")
            return mosq("mosquitto_pub", port, device, client_id, *args, data=payload)

        def policy(verb, before, after, plan, *, target=db, name=backup):
            flags = ["--out", plan] if verb == "plan" else ["--plan", plan, "--backup", name]
            process = command([binary, "policy", verb, "--db", target, "--old-identities", before,
                               "--new-identities", after, *flags], calls)
            return process.returncode, json.loads(process.stdout)

        with broker(binary, db, out, credentials, old_file, "original", calls) as port:
            sub = mosq("mosquitto_sub", port, "dashboard", "dashboard", "-c", "-x", "4294967295",
                       "-d", "-E", "-W", "6", "-t", "atelier/+/status", "-q", "1")
            expect(sub.returncode == 0 and b"received SUBACK" in sub.stdout,
                   "persistent_suback_missing")
            expect(publish(port, "sensor", "capteur-1", old_topic, b"online", True).returncode == 0,
                   "initial_retained_refused")
            expect(publish(port, "sensor", "capteur-1", old_topic, b"awaiting-dashboard").returncode == 0,
                   "offline_publication_refused")
        before = inspect(db)
        inode = db.stat().st_dev, db.stat().st_ino
        expect(before["schema"] == 11 and before["counts"]["obligations"] == 2
               and before["counts"]["retained"] == 1 and before["counts"]["routes"] == 1,
               "original_persistent_responsibilities_missing")

        path = out / "onboard.json"
        status, planned = policy("plan", old_file, new_file, path)
        expect(status == 0 and planned["status"] == "Planned"
               and any(x["kind"] == "principal_added" and x["principal"] == "sensor-2"
                       for x in planned["changes"]["additions"]), "onboarding_plan_refused")
        status, applied = policy("apply", old_file, new_file, path)
        expect(status == 0 and applied["status"] == "Applied", "onboarding_apply_failed")
        after = inspect(db)
        expect(after["tables"] == before["tables"] and before["sessions"] == after["sessions"],
               "mqtt_business_state_changed")
        expect(inode == (db.stat().st_dev, db.stat().st_ino) and inspect(backup) == before,
               "backup_or_original_image_changed")
        expect(backup.stat().st_mode & 0o777 == 0o600
               and hashlib.sha256(backup.read_bytes()).hexdigest() == applied["backup_sha256"],
               "private_backup_digest_mismatch")
        status, repeated = policy("apply", old_file, new_file, path)
        expect(status == 0 and repeated["status"] == "AlreadyApplied", "repeat_apply_not_confirmed")

        unsafe = json.loads(json.dumps(new))
        next(row for row in unsafe["principals"] if row["name"] == "dashboard")["subscribe"] = []
        unsafe_file = out / "unsafe-identities.json"
        unsafe_file.write_text(json.dumps(unsafe, indent=2) + "\n")
        unsafe_plan = out / "refused.json"
        plan_exit, refused_plan = policy("plan", new_file, unsafe_file, unsafe_plan)
        apply_exit, refused_apply = policy("apply", new_file, unsafe_file, unsafe_plan)
        expect(plan_exit == apply_exit == 2 and refused_plan["code"] == refused_apply["code"]
               == "non_monotone_policy_change" and inspect(db) == after,
               "dangerous_change_was_not_refused")

        with broker(binary, db, out, credentials, new_file, "updated", calls) as port:
            sensor = mqtt_session(port, credentials, "sensor", "capteur-1")
            dashboard = mqtt_session(port, credentials, "dashboard", "dashboard")
            foreign = mqtt_session(port, credentials, "sensor-2", "capteur-1")
            expect(sensor == dashboard == {"present": True, "reason": 0, "transport": "TLSv1.3"},
                   "old_session_did_not_resume")
            expect(foreign == {"present": False, "reason": 135, "transport": "TLSv1.3"},
                   "new_sensor_impersonated_old_sensor")
            backlog = mosq("mosquitto_sub", port, "dashboard", "dashboard", "-c", "-x", "4294967295",
                           "-t", "atelier/+/status", "-q", "1", "-R", "-C", "2", "-W", "6",
                           "-F", "%t|%p|%q|%r")
            expect(backlog.returncode == 0 and set(backlog.stdout.splitlines()) == {
                f"{old_topic}|online|1|0".encode(), f"{old_topic}|awaiting-dashboard|1|0".encode()},
                "old_backlog_not_received")
            retained = mosq("mosquitto_sub", port, "dashboard", "dashboard-check", "-t", old_topic,
                            "-q", "1", "-C", "1", "-W", "6", "-F", "%t|%p|%q|%r")
            expect(retained.returncode == 0 and retained.stdout.strip() == f"{old_topic}|online|1|1".encode(),
                   "original_retained_not_recovered")
            expect(publish(port, "sensor-2", "capteur-2", new_topic, b"new-sensor").returncode == 0,
                   "new_sensor_refused")
            received = mosq("mosquitto_sub", port, "dashboard", "dashboard", "-c", "-x", "4294967295",
                            "-t", "atelier/+/status", "-q", "1", "-R", "-C", "1", "-W", "6",
                            "-F", "%t|%p|%q|%r")
            expect(received.returncode == 0 and received.stdout.strip() == f"{new_topic}|new-sensor|1|0".encode(),
                   "new_sensor_message_not_delivered")
            denied = publish(port, "sensor-2", "capteur-2", old_topic, b"outside-grant", True)
            denied_sub = mosq("mosquitto_sub", port, "dashboard", "dashboard-check", "-d", "-E", "-W", "2", "-t", "#", "-q", "1")
            expect(b"DISCONNECT (135)" in denied.stdout + denied.stderr and b"received PUBACK" not in denied.stdout,
                   "forbidden_publish_not_refused")
            expect(b"DISCONNECT" in denied_sub.stdout and b"135" in denied_sub.stdout + denied_sub.stderr,
                   "forbidden_filter_not_refused")
        final = inspect(db)
        expect(final["counts"]["obligations"] == final["counts"]["outbound"] == 0,
               "mqtt_deliveries_not_finished")

        # Restore to a separate path; the live data set and its WAL are never replaced.
        restored = out / "restore.sqlite"
        shutil.copyfile(backup, restored)
        restored.chmod(0o600)
        expect(inspect(restored) == before, "standalone_backup_did_not_restore")
        with broker(binary, restored, out, credentials, old_file, "restored", calls) as port:
            expect(mqtt_session(port, credentials, "sensor", "capteur-1")["present"],
                   "old_policy_could_not_open_restored_backup")
            recovered = mosq("mosquitto_sub", port, "dashboard", "dashboard", "-c", "-x", "4294967295",
                             "-t", "atelier/+/status", "-q", "1", "-R", "-C", "2", "-W", "6",
                             "-F", "%t|%p|%q|%r")
            expect(recovered.returncode == 0 and recovered.stdout == backlog.stdout,
                   "restored_backlog_not_received")

        versions = {program: command([program, "--help"], calls).stdout.decode().splitlines()[1]
                    for program in ("mosquitto_pub", "mosquitto_sub")}
        return {"status": "PASS", "source_sha": json.loads((HERE / "BUILD.json").read_text())["source_sha"]
                if (HERE / "BUILD.json").is_file() else None, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "same_file": inode == (db.stat().st_dev, db.stat().st_ino), "before": before,
                "after": after, "drained_counts": final["counts"], "planned": planned, "applied": applied,
                "repeated": repeated, "refused_plan": refused_plan, "refused_apply": refused_apply,
                "backlog_received_and_finished": 2, "backup_backlog_received": 2, "retained_recovered": 1,
                "new_sensor_qos1": True, "old_sensor_connack": sensor, "dashboard_connack": dashboard,
                "foreign_connack": foreign, "mosquitto": versions, "commands": calls}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--binary", type=Path, default=HERE / "mqttiumd")
    args = parser.parse_args()
    os.umask(0o077)
    result = run(args.binary.resolve(), args.out.resolve())
    (args.out / "policy-demo.json").write_text(json.dumps(result, indent=2) + "\n")
    print(f"PASS: capteur-2, same SQLite file, old and restored backlog. Receipt {args.out / 'policy-demo.json'}")
