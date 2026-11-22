"""Observe one non-retained QoS 1 obligation across a packaged broker restart."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
from queue import Queue
import re
import signal
import subprocess
from tempfile import TemporaryDirectory

import mqttium
from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient

from paho.mqtt import client as mqtt

from common import (PACKAGE, event_rows, inventory, mqtt_client, pki_tools, read_db, require, sha, tls_context,
                    wait_for, write)


TOPIC = "atelier/capteur-1/durable-restart"
PAYLOAD = b"mqttiumd durable restart 01"
LOOPBACK = "127.0.0.1"


@contextmanager
def launch(out, pki, record):
    data = out / "data"
    logs = data / "logs"
    previous = set(logs.glob("run.*/events.jsonl"))
    argv = [str(PACKAGE / "start-tls.sh"), str(data), str(pki)]
    with (out / f"broker-{len(record['starts']) + 1}.log").open("wb") as log:
        process = subprocess.Popen(argv, stdout=log, stderr=log,
                                   env=dict(os.environ, MQTTIUMD_LISTEN=f"{LOOPBACK}:0"))
    started = {"pid": process.pid, "requested_listener": f"{LOOPBACK}:0"}
    record["starts"].append(started)
    try:
        def ready():
            require(process.poll() is None, f"broker_exited_{process.returncode}")
            for path in set(logs.glob("run.*/events.jsonl")) - previous:
                for row in event_rows(path):
                    if row["kind"] == "ready":
                        return path, row["details"]["address"]
            return None

        events, address = wait_for(ready, "packaged_tls_broker_not_ready", duration=12)
        host, port = address.rsplit(":", 1)
        require(host == LOOPBACK, "probe_requires_loopback")
        running_binary = Path(f"/proc/{process.pid}/exe")
        measured = sha(running_binary)
        require(measured == record["binary_sha256"], "running_broker_digest_mismatch")
        identity = (data / "broker.sqlite").stat()
        started.update(address=address, events=str(events.relative_to(out)), binary_sha256=measured,
                       database_device=identity.st_dev, database_inode=identity.st_ino)
        yield int(port), events
    finally:
        if process.poll() is None:
            started["requested_signal"] = "SIGINT"
            process.send_signal(signal.SIGINT)
        try:
            started["exit"] = process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            process.kill()
            started["exit"] = process.wait(timeout=5)
            raise ValueError("packaged_broker_stop_timeout") from None
    require(started["exit"] == 0 and started.get("requested_signal") == "SIGINT", "packaged_broker_stop_failed")


@contextmanager
def paho_dashboard(pki, port, *, clean, cert="dashboard"):
    client, props, keepalive = mqtt_client(pki, "dashboard", cert=cert)
    connack = Queue(maxsize=1)
    suback = Queue(maxsize=1)
    messages = Queue(maxsize=8)
    client.on_connect = lambda _c, _u, f, r, _p: connack.put_nowait((r.value, bool(f.session_present)))
    client.on_subscribe = lambda _c, _u, mid, reasons, _p: suback.put_nowait((mid, [r.value for r in reasons]))
    client.on_message = lambda _c, _u, message: messages.put_nowait(message)
    require(client.connect(LOOPBACK, port, keepalive, clean_start=clean, properties=props) == mqtt.MQTT_ERR_SUCCESS,
            "paho_connect_call_failed")
    require(client.loop_start() == mqtt.MQTT_ERR_SUCCESS, "paho_loop_start_failed")
    try:
        yield client, connack, suback, messages
    finally:
        client.disconnect()
        client.loop_stop()


async def publish(pki, port):
    async with AsyncClient("capteur-1", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as sensor:
        await sensor.connect(LOOPBACK, port, ssl=tls_context(pki, "sensor"), timeout=6)
        sent = await sensor.publish(TOPIC, PAYLOAD, qos=1, retain=False, timeout=6)
        await sent.wait(timeout=6)
        require(sent.is_done() and sent.exception() is None, "mqttium_puback_not_received")
        return {"packet": "PUBACK", "mid": sent.mid, "qos": int(sent.qos), "accepted": True}


def pending_message(db):
    with read_db(db) as reader:
        rows = reader.execute("""
            SELECT p.topic,b.bytes,p.qos,p.retain,o.target,o.phase
              FROM obligations o JOIN publications p ON p.id=o.publication
              JOIN payloads b ON b.id=p.payload
            """).fetchall()
    require(len(rows) == 1 and rows[0][:4] == (TOPIC.encode(), PAYLOAD, 1, 0), "offline_obligation_missing")
    topic, payload, qos, retain, target, phase = rows[0]
    return {"topic": topic.decode(), "payload": payload.decode(), "qos": qos, "retain": bool(retain),
            "target": int.from_bytes(target), "phase": phase}


def exercise(out, pki, record):
    db = out / "data/broker.sqlite"
    record["database"] = str(db.relative_to(out))
    record["demo_policy_sha256"] = sha(pki / "identities.json")
    with launch(out, pki, record) as (port, events):
        with paho_dashboard(pki, port, clean=True) as (client, connack, suback, messages):
            hello = connack.get(timeout=8)
            require(hello == (0, False), "initial_dashboard_session_not_fresh")
            tls = client.socket().version()
            result, mid = client.subscribe(TOPIC, qos=1)
            require(result == mqtt.MQTT_ERR_SUCCESS, "paho_subscribe_call_failed")
            ack = suback.get(timeout=8)
            require(ack == (mid, [1]) and tls == "TLSv1.3" and messages.empty(), "paho_suback_not_granted")
            record["paho_subscription"] = {"connack": list(hello), "suback": list(ack), "tls": tls}
        closed = wait_for(lambda: next((r for r in event_rows(events) if r["kind"] == "connection_closed"), None),
                          "dashboard_disconnect_not_observed")
        record["dashboard_disconnect"] = {"connection": closed["connection"], "seq": closed["seq"]}
        record["mqttium_puback"] = asyncio.run(publish(pki, port))
        record["pending_before_stop"] = pending_message(db)
        stored = inventory(db)
        require(stored["invariants_ok"] and stored["counts"]["outbound"] == stored["counts"]["retained"] == 0,
                "offline_obligation_unexpectedly_claimed")
        record["obligations_before_stop"] = stored["counts"]["obligations"]
    record["pending_while_stopped"] = pending_message(db)

    with launch(out, pki, record) as (port, _events):
        first, second = record["starts"]
        require((first["database_device"], first["database_inode"]) ==
                (second["database_device"], second["database_inode"]) and first["events"] != second["events"],
                "restart_database_identity_mismatch")
        with paho_dashboard(pki, port, clean=True, cert="sensor") as (_client, connack, _suback, _messages):
            refusal = connack.get(timeout=8)
            require(refusal == (0x87, False), "foreign_dashboard_identity_not_refused")
            record["foreign_connack"] = {"reason": refusal[0], "session_present": refusal[1],
                                         "presented_certificate": "sensor", "requested_client_id": "dashboard"}
        with paho_dashboard(pki, port, clean=False) as (client, connack, _suback, messages):
            resumed = connack.get(timeout=8)
            require(resumed == (0, True), "dashboard_session_not_present_after_restart")
            message = messages.get(timeout=8)
            require((message.topic, message.payload, message.qos, message.retain) == (TOPIC, PAYLOAD, 1, False),
                    "resumed_message_does_not_match")
            require(client.ack(message.mid, message.qos) == mqtt.MQTT_ERR_SUCCESS, "paho_manual_puback_failed")
            record["paho_recovery"] = {"session_present": resumed[1], "connack_reason": resumed[0],
                                       "topic": message.topic, "payload": message.payload.decode(),
                                       "qos": message.qos, "retain": message.retain, "dup": message.dup,
                                       "mid": message.mid, "puback_sent": True}
            end = wait_for(lambda: (value if (value := inventory(db))["counts"]["obligations"] == 0 else None),
                           "delivery_obligation_not_finished")
            require(end["invariants_ok"] and end["counts"]["outbound"] == end["counts"]["retained"] == 0,
                    "post_delivery_store_invariant")
            record["obligations_after_puback"] = end["counts"]["obligations"]


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("out", type=Path)
    cli.add_argument("archive_sha256", help="digest bound by the external archive verifier")
    args = cli.parse_args()
    out = args.out.absolute()
    out.mkdir(mode=0o700)
    versions = {name: version(name) for name in ("mqttium", "paho-mqtt")}
    seal = json.loads((PACKAGE / "PACKAGE.json").read_text())
    build = json.loads((PACKAGE / "BUILD.json").read_text())
    binary = sha(PACKAGE / "mqttiumd")
    board_file = Path("/proc/device-tree/model")
    record = {
        "format": "mqttiumd.durable-restart.receipt.v1", "archive_sha256": args.archive_sha256,
        "broker_source_sha": build["source_sha"], "package_source_sha": seal["package_source_sha"],
        "binary_sha256": binary, "build_elf": build["elf"], "clients": versions,
        "client_module_sha256": {"mqttium": sha(Path(mqttium.__file__)), "paho": sha(Path(mqtt.__file__))},
        "machine": platform.machine(), "platform": platform.platform(), "glibc": platform.libc_ver(),
        "board": board_file.read_bytes().rstrip(b"\0").decode("utf-8", errors="replace") if board_file.is_file() else None,
        "reproduce": ["python3", "preview_archive.py", f"{PACKAGE.name}.tar.gz", "--sha256", args.archive_sha256,
                      "--out", "./restart-observation", "--restart"],
        "scope": "one loopback mTLS restart with a persistent MQTT 5 subscriber", "starts": [],
    }
    if os.environ.get("GITHUB_ACTIONS") == "true":
        record["github"] = {name.lower(): os.environ.get(name)
                            for name in ("GITHUB_RUN_ID", "GITHUB_JOB", "GITHUB_RUN_ATTEMPT", "RUNNER_ARCH")}
    try:
        require(re.fullmatch(r"[0-9a-f]{64}", args.archive_sha256), "invalid_archive_sha256")
        require(versions == {"mqttium": "1.3.0", "paho-mqtt": "2.1.0"}, "wrong_client_versions")
        require(platform.machine() == build["machine"] and binary == build["binary_sha256"] ==
                seal["broker_binary_sha256"], "packaged_broker_identity_mismatch")
        with TemporaryDirectory(prefix="durable-restart-pki-", dir=out) as tmp:
            pki = Path(tmp) / "pki"
            pki_tools().generate(pki)
            exercise(out, pki, record)
    except Exception as error:
        write(out / "restart.json", {**record, "status": "FAIL", "error": f"{type(error).__name__}: {error}"})
        raise
    record["status"] = "PASS"
    write(out / "restart.json", record)
    print(out / "restart.json")


if __name__ == "__main__":
    main()
