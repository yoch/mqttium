#!/usr/bin/env python3
"""Observe durable MQTT presence with two authenticated Mosquitto clients."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import pty
import select
import signal
import sqlite3
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("policy_demo", HERE / "policy-demo.py")
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)
TOPIC = "atelier/capteur-1/status"


def until(observe, code):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        result = observe()
        if result:
            return result
        time.sleep(.01)
    raise ValueError(code)


def inspect(db):
    with sqlite3.connect(db.as_uri() + "?mode=ro", uri=True) as sql:
        sql.execute("BEGIN")
        counts = {table: sql.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("wills", "sessions", "retained", "publications", "payloads", "obligations", "outbound")}
        retained = sql.execute("SELECT hex(r.publication),p.qos,b.bytes,b.refs FROM retained r "
                               "JOIN publications p ON p.id=r.publication JOIN payloads b ON b.id=p.payload "
                               "WHERE r.topic=?", (TOPIC.encode(),)).fetchone()
        return {"counts": counts, "retained": [retained[0], retained[1], retained[2].decode(),
                                              int.from_bytes(retained[3], 'big')] if retained else None}


@contextmanager
def sensor(argv, label, out, receipt):
    master, slave = pty.openpty()
    try:
        child = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=slave, stderr=slave)
    finally:
        os.close(slave)
    record = {"case": label, "argv": list(map(str, argv)), "pid": child.pid}
    receipt["calls"].append(record)
    output = bytearray()

    def pump():
        while select.select([master], [], [], 0)[0]:
            try:
                part = os.read(master, 4096)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                break
            if not part:
                break
            output.extend(part)
            demo.expect(len(output) < 65536, "sensor_output_budget")
        return output.decode().lower()

    def admitted():
        log = pump()
        if "received connack (131)" in log:
            raise ValueError("will_retain_rejected_by_connack_0x83")
        demo.expect(child.poll() is None, "sensor_exited_before_publication")
        return "received connack (0)" in log and "received puback" in log

    try:
        child.stdin.write(b"online\n")
        child.stdin.flush()
        until(admitted, "sensor_connack_or_puback_missing")
        yield child, record
    finally:
        if child.poll() is None:
            child.kill()
        record["exit"] = child.wait(timeout=5)
        child.stdin.close()
        pump()
        os.close(master)
        record["output"] = output.decode()
        (out / f"{label}.log").write_bytes(output)


def run(binary, out):
    out.mkdir(mode=0o700)
    db = out / "broker.sqlite"
    build_file = binary.with_name("BUILD.json")
    build = json.loads(build_file.read_text()) if build_file.is_file() else None
    receipt = {"format": "mqttiumd.retained-will-demo.v1", "status": "RUNNING", "calls": [],
               "observations": {}, "binary": str(binary), "build": build,
               "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
               "execution_platform": platform.platform(), "clients": {}}
    observations = receipt["observations"]
    try:
        for client in ("mosquitto_pub", "mosquitto_sub"):
            version = subprocess.run([client, "--help"], capture_output=True, text=True, timeout=5)
            receipt["clients"][client] = version.stdout.splitlines()[0]
        with tempfile.TemporaryDirectory(prefix="mqttiumd-retained-will-") as temp:
            pki = Path(temp) / "pki"
            demo.pki.generate(pki)

            def client(program, port, device, client_id, *args):
                return [program, "-V", "mqttv5", "-h", "127.0.0.1", "-p", str(port),
                        "--tls-version", "tlsv1.3", "--cafile", pki / "ca.crt",
                        "--cert", pki / f"{device}.crt", "--key", pki / f"{device}.key",
                        "-i", client_id, *args]

            def source(port, label):
                argv = client("mosquitto_pub", port, "sensor", "capteur-1", "-d", "-l", "-q", "1",
                              "-r", "-t", TOPIC, "--will-topic", TOPIC, "--will-payload", "offline",
                              "--will-retain", "--will-qos", "1", "-D", "will", "will-delay-interval", "0")
                return sensor(argv, label, out, receipt)

            def observe(port, label, value, client_id):
                argv = client("mosquitto_sub", port, "dashboard", client_id, "-q", "1", "-t", "atelier/+/status",
                              "-C", "1", "-W", "5", "-F", "%t|%p|%q|%r")
                started = time.monotonic_ns()
                result = demo.command(argv, receipt["calls"])
                observations[label] = {"message": result.stdout.decode().strip(),
                                       "duration_ns": time.monotonic_ns() - started, "exit": result.returncode}
                demo.expect(result.returncode == 0 and result.stdout.strip() == f"{TOPIC}|{value}|1|1".encode(),
                            "late_dashboard_retained_" + value)

            def image(value):
                state = inspect(db)
                return state if state["retained"] and state["retained"][1:] == [1, value, 1] else None

            policy = pki / "identities.json"
            with demo.broker(binary, db, out, pki, policy, "first", receipt["calls"], crash=True) as port:
                inode = db.stat().st_ino
                with source(port, "sensor-killed") as (child, record):
                    online = until(lambda: image("online"), "online_retained_missing")
                    demo.expect(online["counts"]["wills"] == 1 and online["counts"]["sessions"] == 1,
                                "armed_will_or_absent_dashboard_missing")
                    observations["online"] = online
                    record["sigkill_monotonic_ns"] = time.monotonic_ns()
                    child.kill()
                    demo.expect(child.wait(timeout=5) == -signal.SIGKILL, "sensor_sigkill_missing")
                offline = until(lambda: image("offline"), "retained_will_missing_without_subscribers")
                demo.expect(offline["retained"][0] != online["retained"][0] and offline["counts"]["wills"] == 0
                            and offline["counts"]["sessions"] == 0, "retained_will_occurrence_or_cleanup")
                observations["offline"] = offline
                observe(port, "late-dashboard", "offline", "dashboard")

            with demo.broker(binary, db, out, pki, policy, "recovered", receipt["calls"]) as port:
                demo.expect(db.stat().st_ino == inode and inspect(db)["retained"] == offline["retained"],
                            "offline_retained_identity_changed_after_sigkill")
                observe(port, "recovered-dashboard", "offline", "dashboard-check")
                with source(port, "sensor-normal") as (child, record):
                    restored = until(lambda: image("online"), "restored_online_retained_missing")
                    child.stdin.close()
                    demo.expect(child.wait(timeout=5) == 0, "sensor_normal_disconnect_failed")
                demo.expect("sending disconnect" in record["output"].lower(), "sensor_disconnected_without_mqtt")
                final = until(lambda: (lambda current: current if current["counts"]["wills"] == 0 and
                              current["counts"]["sessions"] == 0 and current["counts"]["obligations"] == 0 else None)
                              (inspect(db)), "retained_will_responsibilities_not_drained")
                demo.expect(final["retained"] == restored["retained"] and
                            final["counts"]["retained"] == final["counts"]["payloads"] == 1,
                            "normal_disconnect_published_will_or_leaked_references")
                observe(port, "restored-dashboard", "online", "dashboard")
                observations["final"] = final
        receipt["status"] = "PASS"
    except BaseException as error:
        receipt.update(status="FAIL", failure=str(error))
        raise
    finally:
        (out / "retained-will-demo.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("PASS: online → sensor SIGKILL → offline retained → late dashboard → broker SIGKILL → offline retained → online")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--binary", type=Path, default=HERE / "mqttiumd")
    args = parser.parse_args()
    run(args.binary.resolve(), args.out.resolve())
