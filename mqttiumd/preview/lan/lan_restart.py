#!/usr/bin/env python3
"""Run one durable MQTT 5 restart with separate broker and client roles."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack, closing, contextmanager
import hashlib
from importlib.metadata import version
from ipaddress import IPv4Address, IPv4Network
import json
import os
from pathlib import Path
import platform
from queue import Queue
import re
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from zipfile import ZipFile


SCRIPT = Path(__file__).resolve()
ZIP_SHA256 = "0c25d0b4a2c6252aaac01dfdfa3defa84f87c8d51470d0f224e80666f9907d45"
TAR_SHA256 = "90335c2ea30fdff16491c784cec018ccbe196a2be82b07a6a9334376e3d4c4c7"
PACKAGE = "mqttiumd-restart-preview-51be4079-569a870c-aarch64-linux"
BROKER = {
    "binary_sha256": "f915e784ff09e2af874798b4ede4b6235ebcc459f009b39f902ea176a73ac479",
    "broker_source_sha": "51be4079f7ee42f46500e993315eddc971f0a8d4",
    "package_source_sha": "569a870c80d15a67133e540c6882d706ae6294b9",
}
CLIENTS = {"mqttium": "1.3.0", "paho-mqtt": "2.1.0"}
TIMEOUT = 30


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write(path, data):
    with path.open("x") as output:
        json.dump(data, output, indent=2, sort_keys=True)
        output.write("\n")


def prepare(site, host, campaign, mqtt_port, control_port):
    address = IPv4Address(host)
    require(any(address in IPv4Network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")),
            "choose_an_explicit_private_IPv4_address")
    require(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,47}", campaign), "invalid_campaign_id")
    require(1024 <= mqtt_port <= 65535 and 1024 <= control_port <= 65535 and mqtt_port != control_port,
            "choose_two_distinct_unprivileged_ports")
    require(shutil.disk_usage(site.parent).free >= 128 * 1024**2, "need_128_MiB_free_for_the_short_demo")
    with (SCRIPT.parent / "broker-aarch64.zip").open("rb") as source:
        # In particular, authenticate preview_archive.py before importing or launching it.
        require(hashlib.file_digest(source, "sha256").hexdigest() == ZIP_SHA256, "broker_zip_sha256_mismatch")
        source.seek(0)
        site.mkdir(mode=0o700)
        broker, peer = site / "broker", site / "peer"
        for role in (broker, peer):
            role.mkdir(mode=0o700)
            (role / "pki").mkdir(mode=0o700)
        with TemporaryDirectory(dir=broker) as temporary, ZipFile(source) as archive:
            staging = Path(temporary)
            tar = f"{PACKAGE}.tar.gz"
            members = {tar, f"{tar}.sha256", "preview_archive.py"}
            require(len(archive.namelist()) == len(members) and set(archive.namelist()) == members,
                    "unexpected_broker_zip_entries")
            archive.extractall(staging)
            subprocess.run([sys.executable, staging / "preview_archive.py", staging / tar,
                            "--sha256", TAR_SHA256, "--out", broker / "preview"], check=True, timeout=TIMEOUT)
            preflight = json.loads((broker / "preview/preflight.json").read_text())
            require(all(preflight[k] == v for k, v in BROKER.items()), "unexpected_frozen_broker")
            subprocess.run([sys.executable, broker / "preview" / PACKAGE / "demo-pki.py",
                            staging / "pki", "--host", host], check=True, timeout=TIMEOUT)
            pki = staging / "pki"
            dashboard_der = ssl.PEM_cert_to_DER_cert((pki / "dashboard.crt").read_text())
            config = {"host": host, "mqtt_port": mqtt_port, "control_port": control_port, "campaign": campaign,
                      "topic": f"atelier/capteur-1/lan-restart/{campaign}", "payload": f"mqttiumd LAN restart {campaign}",
                      "control_client_sha256": hashlib.sha256(dashboard_der).hexdigest(), "broker": BROKER,
                      "broker_zip_sha256": ZIP_SHA256, "broker_tar_sha256": TAR_SHA256,
                      "kit_source": (SCRIPT.parent / "KIT-SOURCE").read_text().strip()}
            shutil.copyfile(SCRIPT.parent / "requirements.txt", peer / "requirements.txt")
            for role, credentials in ((broker, ("ca.crt", "server.crt", "server.key", "identities.json")),
                                      (peer, ("ca.crt", "sensor.crt", "sensor.key", "dashboard.crt", "dashboard.key"))):
                for item in credentials:
                    shutil.copyfile(pki / item, role / "pki" / item)
                shutil.copyfile(SCRIPT, role / SCRIPT.name)
                write(role / "campaign.json", config)
    print(f"Broker ready to run from {broker}. Transfer {peer} privately to the peer.")


def tls(pki, identity):
    ctx = ssl.create_default_context(
        ssl.Purpose.CLIENT_AUTH if identity == "server" else ssl.Purpose.SERVER_AUTH,
        cafile=str(pki / "ca.crt"),
    )
    ctx.minimum_version = ctx.maximum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(pki / f"{identity}.crt", pki / f"{identity}.key")
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


class Rendezvous:
    """Ordered phase signals over a separate, campaign-bound mTLS connection."""

    def __init__(self, conn, reader, config, receipt):
        self.conn = conn
        self.reader = reader
        self.campaign = config["campaign"]
        self.trace = receipt["control"]["signals"]

    def send(self, step, **fields):
        message = {"campaign": self.campaign, "step": step, **fields}
        self.conn.sendall(json.dumps(message, separators=(",", ":")).encode() + b"\n")
        self.trace.append({"sent": step, **fields})

    def expect(self, step, *, with_mid=False):
        line = self.reader.readline(513)
        require(0 < len(line) <= 512 and line.endswith(b"\n"), f"missing_or_invalid_signal_{step}")
        message = json.loads(line)
        expected = {"campaign", "step", "mid"} if with_mid else {"campaign", "step"}
        require(isinstance(message, dict) and message.keys() == expected
                and message["campaign"] == self.campaign and message["step"] == step,
                f"unexpected_control_signal_at_{step}")
        mid = message.get("mid")
        require(not with_mid or type(mid) is int and 1 <= mid <= 65535, "invalid_delivery_packet_identifier")
        self.trace.append({"received": step, **({"mid": mid} if with_mid else {})})
        return mid


@contextmanager
def control(sock, pki, config, receipt, *, server):
    kind = "server" if server else "dashboard"
    sock.settimeout(TIMEOUT)
    ctx = tls(pki, kind)
    with ctx.wrap_socket(sock, server_side=server, server_hostname=None if server else config["host"]) as conn:
        if server:
            presented = conn.getpeercert(binary_form=True)
            require(hashlib.sha256(presented).hexdigest() == config["control_client_sha256"],
                    "control_requires_the_dashboard_certificate")
        receipt["control"] = {"tls": conn.version(), "local": conn.getsockname(), "remote": conn.getpeername(),
                              "signals": []}
        with conn.makefile("rb") as reader:
            yield Rendezvous(conn, reader, config, receipt)


def wait_for(probe, reason):
    deadline = monotonic() + TIMEOUT
    while monotonic() < deadline:
        if value := probe():
            return value
        sleep(0.02)
    raise TimeoutError(reason)


def connect_control(address):
    def attempt():
        try:
            return socket.create_connection(address, timeout=1)
        except (ConnectionRefusedError, TimeoutError):
            return None

    return wait_for(attempt, "broker_control_listener_unreachable")


def event(path, kind):
    # The broker may be appending the last record while the demo is reading.
    for line in path.read_bytes().split(b"\n")[:-1]:
        record = json.loads(line)
        if record["kind"] == kind:
            return record
    return None


@contextmanager
def launch(root, config, receipt):
    data = root / "run/data"
    logs = data / "logs"
    previous = set(logs.glob("run.*/events.jsonl"))
    listener = f"{config['host']}:{config['mqtt_port']}"
    name = f"broker-{len(receipt['starts']) + 1}"
    argv = [root / "preview" / PACKAGE / "start-tls.sh", data, root / "pki"]
    with (root / "run" / f"{name}.log").open("xb") as log:
        process = subprocess.Popen(argv, env=dict(os.environ, MQTTIUMD_LISTEN=listener), stdout=log, stderr=log)
    start = {"pid": process.pid, "listener": listener, "launcher": str(argv[0].relative_to(root))}
    receipt["starts"].append(start)
    try:
        def ready():
            require(process.poll() is None, f"broker_exited_{process.returncode}")
            for stream in set(logs.glob("run.*/events.jsonl")) - previous:
                if ready_event := event(stream, "ready"):
                    return stream, ready_event
            return None

        stream, ready_event = wait_for(ready, f"{name}_not_ready")
        require(ready_event["details"]["address"] == listener, "mqtt_listener_mismatch")
        binary = digest(Path(f"/proc/{process.pid}/exe"))
        require(binary == config["broker"]["binary_sha256"], "running_broker_sha256_mismatch")
        db = (data / "broker.sqlite").stat()
        start.update(binary_sha256=binary, sqlite_device=db.st_dev, sqlite_inode=db.st_ino,
                     events=str(stream.relative_to(root)), ready_event_seq=ready_event["seq"])
        yield stream
    finally:
        # Save just the product event stream for this start, outside the private DB tree.
        if process.poll() is None:
            start["stop_signal"] = "SIGINT"
            process.send_signal(signal.SIGINT)
        try:
            start["exit"] = process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            process.kill()
            start["exit"] = process.wait(timeout=5)
            raise
        finally:
            streams = set(logs.glob("run.*/events.jsonl")) - previous
            if len(streams) == 1:
                shutil.copyfile(streams.pop(), root / "run" / f"{name}.events.jsonl")
    require(start.get("stop_signal") == "SIGINT" and start["exit"] == 0, "broker_stop_failed")


def delivery_snapshot(db):
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=2)) as reader:
        reader.execute("BEGIN")
        obligations = reader.execute("SELECT count(*) FROM obligations").fetchone()[0]
        claims = reader.execute("SELECT count(*) FROM outbound").fetchone()[0]
        require(obligations <= 1 and claims <= 1, "unexpected_parallel_durable_deliveries")
        row = reader.execute("""
            SELECT o.publication, o.idx, o.target, o.phase, s.client_id,
                   p.topic, b.bytes, o.qos, o.retain, c.packet_id, c.claim
              FROM obligations o JOIN publications p ON p.id = o.publication
              JOIN payloads b ON b.id = p.payload JOIN sessions s ON s.id = o.target
              LEFT JOIN outbound c ON (c.publication, c.idx, c.target, c.target_revision)
                                   = (o.publication, o.idx, o.target, o.target_revision)
        """).fetchone()
    require(obligations == int(row is not None), "obligation_missing_dependents")
    delivery = None
    if row:
        pub, idx, target, phase, client_id, topic, payload, qos, retain, mid, claim = row
        delivery = {"publication": pub.hex(), "index": idx, "target_session": str(int.from_bytes(target, "big")),
                    "phase": phase, "client_id": client_id.decode(), "topic": topic.decode(),
                    "payload": payload.decode(), "qos": qos, "retain": bool(retain),
                    "mid": mid, "claim": claim.hex() if claim else None}
    require(claims == int(delivery is not None and delivery["claim"] is not None), "uncorrelated_outbound_claim")
    return {"obligations": obligations, "claims": claims, "delivery": delivery}


def identity_bytes(event_id):
    return b"".join(int(event_id[k]).to_bytes(8, "big") for k in ("incarnation", "epoch", "owner", "sequence")).hex()


def broker_run(root, config, receipt):
    db = root / "run/data/broker.sqlite"
    pki = root / "pki"
    receipt["preflight"] = json.loads((root / "preview/preflight.json").read_text())
    require(platform.machine() == receipt["preflight"]["machine"] == "aarch64", "frozen_broker_requires_AArch64")
    require(digest(root / "preview" / PACKAGE / "mqttiumd") == config["broker"]["binary_sha256"],
            "installed_broker_sha256_mismatch")
    with ExitStack() as sockets:
        listener = sockets.enter_context(socket.create_server((config["host"], config["control_port"])))
        listener.settimeout(2 * TIMEOUT)
        with launch(root, config, receipt) as first:
            print(f"Ready on {config['host']}:{config['mqtt_port']}; start the peer now.", flush=True)
            sock, _ = listener.accept()
            sockets.enter_context(sock)
            sync = sockets.enter_context(control(sock, pki, config, receipt, server=True))
            sync.send("broker_ready")
            sync.expect("subscriber_disconnected")
            receipt["subscriber_closed_event"] = wait_for(
                lambda: event(first, "connection_closed"), "subscriber_disconnect_not_observed")
            sync.send("subscriber_offline")
            sync.expect("published")
            before = delivery_snapshot(db)
            require(before["obligations"] == 1 and before["claims"] == 0, "offline_obligation_missing")
            pending = before["delivery"]
            require((pending["topic"], pending["payload"], pending["client_id"], pending["qos"], pending["retain"])
                    == (config["topic"], config["payload"], "dashboard", 1, False), "wrong_offline_obligation")
            receipt["before_stop"] = before
        receipt["while_stopped"] = delivery_snapshot(db)
        require(receipt["while_stopped"] == before, "offline_obligation_changed_during_stop")

        with launch(root, config, receipt) as second:
            first_start, second_start = receipt["starts"]
            db_identity = ("sqlite_device", "sqlite_inode")
            require(all(first_start[k] == second_start[k] for k in db_identity) and first != second,
                    "restart_did_not_keep_the_same_database")
            sync.send("restarted")
            mid = sync.expect("delivery_held", with_mid=True)
            held = delivery_snapshot(db)
            receipt["held_before_puback"] = held
            require(held["obligations"] == held["claims"] == 1, "expected_one_obligation_and_one_claim_before_puback")
            delivery = held["delivery"]
            require(all(delivery[k] == pending[k] for k in ("publication", "index", "target_session"))
                    and delivery["mid"] == mid, "held_claim_does_not_match_the_replayed_delivery")
            require(event(second, "puback_observed") is None, "puback_reached_the_broker_before_release")
            sync.send("release_puback")
            sync.expect("puback_sent")
            ack = wait_for(lambda: event(second, "puback_observed"), "released_puback_not_observed_by_broker")
            receipt["puback_observed_event"] = ack
            wire = ack["puback"]
            target = wire["target"]
            require(wire["packet_id"] == mid and wire["reason_code"] == 0 and target
                    and identity_bytes(target["claim_operation_id"]) == delivery["claim"]
                    and identity_bytes(target["delivery_id"]["publication_id"]) == delivery["publication"]
                    and target["delivery_id"]["index"] == delivery["index"]
                    and wire["connection"]["session"] == delivery["target_session"], "puback_did_not_bind_to_held_claim")

            def drained():
                snapshot = delivery_snapshot(db)
                if snapshot["obligations"] == snapshot["claims"] == 0:
                    return snapshot
                return None

            receipt["after_puback"] = wait_for(drained, "delivery_and_claim_not_finished")
            sync.send("drained")
            sync.expect("mosquitto_done")
            sync.send("finished")


@contextmanager
def dashboard(root, config, *, clean, cert="dashboard"):
    from paho.mqtt import client as mqtt

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="dashboard", protocol=mqtt.MQTTv5,
                         reconnect_on_failure=False, manual_ack=True)
    client.tls_set_context(tls(root / "pki", cert))
    connack, suback, messages = Queue(maxsize=1), Queue(maxsize=1), Queue(maxsize=4)

    def connected(client, _userdata, flags, reason, _properties):
        connection = client.socket()
        connack.put_nowait({"reason": reason.value, "session_present": bool(flags.session_present),
                            "tls": connection.version(), "remote": connection.getpeername()})

    client.on_connect = connected
    client.on_subscribe = lambda _c, _u, mid, reasons, _p: suback.put_nowait((mid, [r.value for r in reasons]))
    client.on_message = lambda _c, _u, message: messages.put_nowait(message)
    properties = mqtt.Properties(mqtt.PacketTypes.CONNECT)
    properties.SessionExpiryInterval = 4294967295
    require(client.connect(config["host"], config["mqtt_port"], keepalive=15, clean_start=clean,
                           properties=properties) == mqtt.MQTT_ERR_SUCCESS, "paho_connect_call_failed")
    require(client.loop_start() == mqtt.MQTT_ERR_SUCCESS, "paho_loop_start_failed")
    try:
        yield client, connack, suback, messages
    finally:
        client.disconnect()
        client.loop_stop()


async def publish(root, config):
    from mqttium import MQTTProtocolVersion
    from mqttium.api import AsyncClient

    async with AsyncClient("capteur-1", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as sensor:
        await sensor.connect(config["host"], config["mqtt_port"], ssl=tls(root / "pki", "sensor"), timeout=8)
        sent = await sensor.publish(config["topic"], config["payload"].encode(), qos=1, retain=False, timeout=8)
        await sent.wait(timeout=8)
        require(sent.is_done() and sent.exception() is None, "mqttium_puback_not_received")
        return {"packet": "PUBACK", "mid": sent.mid, "qos": int(sent.qos), "accepted": True}


def mosquitto_exchange(root, config):
    # Retained data lets these one-shot clients run in order without a readiness sleep.
    topic = config["topic"] + "/mosquitto"
    payload = f"mosquitto {config['campaign']}"
    address = ["-V", "mqttv5", "-h", config["host"], "-p", str(config["mqtt_port"]), "--tls-version", "tlsv1.3",
               "--cafile", "pki/ca.crt"]
    pub = ["mosquitto_pub", *address, "--cert", "pki/sensor.crt", "--key", "pki/sensor.key",
           "-i", "capteur-1", "-q", "1", "-r", "-t", topic, "-m", payload]
    sub = ["mosquitto_sub", *address, "--cert", "pki/dashboard.crt", "--key", "pki/dashboard.key",
           "-i", "dashboard-check", "-q", "1", "-C", "1", "-W", "15", "-v", "-t", topic]
    versions = {}
    for tool in (pub[0], sub[0]):
        result = subprocess.run([tool, "--help"], cwd=root, text=True, capture_output=True, timeout=5)
        line = next((line for line in result.stdout.splitlines() if line.startswith(f"{tool} version ")), None)
        require(result.returncode in (0, 1) and line, f"{tool}_version_not_reported")
        versions[tool] = line
    subprocess.run(pub, cwd=root, check=True, timeout=17)
    received = subprocess.check_output(sub, cwd=root, text=True, timeout=17)
    require(received == f"{topic} {payload}\n", "mosquitto_received_unexpected_message")
    return {"clients": versions, "commands": [pub, sub], "topic": topic, "payload": payload, "output": received}


def peer_run(root, config, receipt):
    from paho.mqtt import client as mqtt

    receipt["clients"] = {name: version(name) for name in CLIENTS}
    require(receipt["clients"] == CLIENTS, "use_the_client_versions_from_requirements.txt")
    with connect_control((config["host"], config["control_port"])) as sock, \
            control(sock, root / "pki", config, receipt, server=False) as sync:
        sync.expect("broker_ready")
        with dashboard(root, config, clean=True) as (client, connack, suback, messages):
            hello = connack.get(timeout=TIMEOUT)
            require((hello["reason"], hello["session_present"], hello["tls"]) == (0, False, "TLSv1.3"),
                    "initial_dashboard_session_not_fresh")
            result, mid = client.subscribe(config["topic"], qos=1)
            require(result == mqtt.MQTT_ERR_SUCCESS, "paho_subscribe_call_failed")
            ack = suback.get(timeout=TIMEOUT)
            require(ack == (mid, [1]) and messages.empty(), "paho_suback_not_granted")
            receipt["subscription"] = {"connack": hello, "suback_mid": mid, "suback_codes": ack[1]}
        sync.send("subscriber_disconnected")
        sync.expect("subscriber_offline")
        receipt["mqttium_puback"] = asyncio.run(publish(root, config))
        sync.send("published")
        sync.expect("restarted")
        with dashboard(root, config, clean=True, cert="sensor") as (_client, connack, _suback, _messages):
            refused = connack.get(timeout=TIMEOUT)
            receipt["hostile_connack"] = {"certificate": "sensor", "requested_client_id": "dashboard", **refused}
            require((refused["reason"], refused["session_present"], refused["tls"]) == (0x87, False, "TLSv1.3"),
                    "foreign_dashboard_identity_not_refused")
        with dashboard(root, config, clean=False) as (client, connack, _suback, messages):
            resumed = connack.get(timeout=TIMEOUT)
            require((resumed["reason"], resumed["session_present"], resumed["tls"]) == (0, True, "TLSv1.3"),
                    "dashboard_session_not_present_after_restart")
            message = messages.get(timeout=TIMEOUT)
            require((message.topic, message.payload, message.qos, message.retain)
                    == (config["topic"], config["payload"].encode(), 1, False), "resumed_message_does_not_match")
            receipt["recovered"] = {"connack": resumed, "topic": message.topic, "payload": message.payload.decode(),
                                    "mid": message.mid, "qos": message.qos, "retain": message.retain, "dup": message.dup}
            sync.send("delivery_held", mid=message.mid)
            sync.expect("release_puback")
            require(client.ack(message.mid, message.qos) == mqtt.MQTT_ERR_SUCCESS, "paho_manual_puback_failed")
            sync.send("puback_sent")
            sync.expect("drained")
        receipt["mosquitto"] = mosquitto_exchange(root, config)
        sync.send("mosquitto_done")
        sync.expect("finished")


def run(role):
    root = SCRIPT.parent
    config = json.loads((root / "campaign.json").read_text())
    output = root / "run"
    output.mkdir(mode=0o700)
    receipt = {"format": "mqttiumd.lan-restart.receipt.v1", "role": role, "starts": [], "config": config,
               "launcher_sha256": digest(SCRIPT), "reproduce": [sys.executable, SCRIPT.name, role],
               "host": socket.gethostname(), "machine": platform.machine(), "kernel": platform.release(),
               "glibc": platform.libc_ver(), "python": platform.python_version(),
               "netns": os.readlink("/proc/self/ns/net"), "mntns": os.readlink("/proc/self/ns/mnt")}
    board = Path("/proc/device-tree/model")
    if board.is_file():
        receipt["board"] = board.read_bytes().rstrip(b"\0").decode(errors="replace")
    path = output / f"lan-{role}.json"
    try:
        (broker_run if role == "broker" else peer_run)(root, config, receipt)
    except Exception as error:
        write(path, {**receipt, "status": "FAIL", "error": f"{type(error).__name__}: {error}"})
        raise
    write(path, {**receipt, "status": "PASS"})
    print(path, flush=True)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("prepare", help="verify the ARM preview and provision two private role directories")
    setup.add_argument("--out", type=Path, required=True, help="new directory with an existing parent")
    setup.add_argument("--host", required=True, help="explicit RFC 1918 IPv4 of the broker")
    setup.add_argument("--campaign", required=True)
    setup.add_argument("--mqtt-port", type=int, default=28883)
    setup.add_argument("--control-port", type=int, default=28884)
    commands.add_parser("broker", help="start, restart and inspect the local MQTT broker")
    commands.add_parser("peer", help="run the MQTT clients and send explicit phase acknowledgements")
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args.out.resolve(), args.host, args.campaign, args.mqtt_port, args.control_port)
    else:
        run(args.command)


if __name__ == "__main__":
    main()
