#!/usr/bin/env python3
"""Observe MQTT 3.1.1 and MQTT 5 on the extracted broker's mTLS listener."""
from __future__ import annotations

import argparse
import asyncio
from contextlib import closing, contextmanager
import hashlib
from importlib.metadata import version
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

from mosquitto_example import reference_config

from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, SubscribeOptions
from paho.mqtt import client as paho


HOST = "127.0.0.1"
PACKAGE = Path(__file__).resolve().parent
TOPIC = "atelier/capteur-1/mqtt311"
REFERENCE = "2.0.21"
REFERENCE_SHA256 = "7ad5e84caeb8d2bb6ed0c04614b2a7042def961af82d87f688ba33db857b899d"
TIMEOUT = 10


def require(condition, failure):
    if not condition:
        raise ValueError(failure)


def sha(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def wait_for(probe, failure, *, timeout=TIMEOUT):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if value := probe():
            return value
        sleep(0.01)
    raise TimeoutError(failure)


def events(path):
    return [json.loads(line) for line in path.read_bytes().split(b"\n")[:-1]]


@contextmanager
def process(command, output, *, broker=False, env=None):
    with output.open("wb") as stdout:
        child = subprocess.Popen(command, stdout=stdout, stderr=stdout, env=env)
    try:
        yield child
    finally:
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=TIMEOUT)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=TIMEOUT)
                raise
    if broker:
        require(child.returncode == 0, "broker_did_not_stop_cleanly")


@contextmanager
def launch_broker(out, pki, name, data, record, *, killed=False, package=PACKAGE):
    logs = data / "logs"
    previous = set(logs.glob("run.*/events.jsonl"))
    command = [package / "start-tls.sh", data, pki]
    stream = None
    try:
        with process(command, out / f"{name}.log", broker=not killed,
                     env=dict(os.environ, MQTTIUMD_LISTEN=f"{HOST}:0")) as child:
            def ready():
                require(child.poll() is None, "packaged_broker_exited_from_startup")
                for path in set(logs.glob("run.*/events.jsonl")) - previous:
                    for row in events(path):
                        if row["kind"] == "ready":
                            return path, row
                return None

            stream, ready_event = wait_for(ready, "packaged_broker_not_ready")
            host, port = ready_event["details"]["address"].split(":")
            require(host == HOST, "reception_requires_loopback")
            binary = sha(Path(f"/proc/{child.pid}/exe"))
            require(binary == record["binary_sha256"], "executed_binary_digest_mismatch")
            db = (data / "broker.sqlite").stat()
            record["starts"].append({"name": name, "pid": child.pid, "port": int(port),
                                     "binary_sha256": binary, "sqlite": [db.st_dev, db.st_ino]})
            yield int(port), stream
            if killed:
                status = child.wait(timeout=TIMEOUT)
                require(status == -signal.SIGKILL, "broker_kill_not_verified")
                record["starts"][-1]["exit_status"] = status
    finally:
        if stream is not None:
            (out / f"{name}.jsonl").write_bytes(stream.read_bytes())


@contextmanager
def reference_service(config, source, log):
    with process([source / "src/mosquitto", "-c", config], log, broker=True) as child:
        def ready():
            require(child.poll() is None, "reference_exited_from_startup")
            return f"mosquitto version {REFERENCE} running" in log.read_text()

        wait_for(ready, "reference_not_ready")
        yield child


@contextmanager
def launch_reference(out, pki, source):
    with socket.socket() as reservation:
        reservation.bind((HOST, 0))
        port = reservation.getsockname()[1]
    acl = out / "reference.acl"
    acl.write_text("user sensor\ntopic write atelier/capteur-1/#\nuser dashboard\ntopic read atelier/#\n")
    config, log = out / "reference.conf", out / "reference.log"
    reference_config(config, acl, pki, port)
    with reference_service(config, source, log):
        yield port, log


def tls(pki, identity):
    context = ssl.create_default_context(cafile=str(pki / "ca.crt"))
    context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(pki / f"{identity}.crt", pki / f"{identity}.key")
    return context


class Mosquitto:
    def __init__(self, out, pki, source):
        self.out = out
        self.pki = pki
        self.source = source
        home = out / "empty-client-config"
        home.mkdir(mode=0o700)
        self.env = {"LC_ALL": "C", "HOME": str(home), "XDG_CONFIG_HOME": str(home),
                    "PATH": os.defpath, "LD_LIBRARY_PATH": str(source / "lib")}

    def command(self, role, port, topic, *, explicit=False, qos=1):
        publisher = role == "pub"
        cert, client_id = ("sensor", "capteur-1") if publisher else ("dashboard", "dashboard")
        command = [str(self.source / "client" / f"mosquitto_{role}"), "-h", HOST, "-p", str(port),
                   "--tls-version", "tlsv1.3", "--cafile", str(self.pki / "ca.crt"),
                   "--cert", str(self.pki / f"{cert}.crt"), "--key", str(self.pki / f"{cert}.key"),
                   "-i", client_id, "-d", "-q", str(qos), "-t", topic]
        if explicit:
            command += ["-V", "mqttv311"]
        return command

    def publish(self, port, topic, payload, *, explicit=False, qos=1):
        args = self.command("pub", port, topic, explicit=explicit, qos=qos) + ["-m", payload]
        result = subprocess.run(args, env=self.env, text=True, capture_output=True, check=True, timeout=TIMEOUT)
        if qos == 1:
            require("received PUBACK" in result.stdout, "mosquitto_publisher_did_not_observe_puback")
            return {"argv": args, "puback": result.stdout.splitlines()}
        patterns = [
            r"sending PUBLISH \(d0, q2, r0, m(\d+),",
            r"received PUBREC \(Mid: (\d+)\)",
            r"sending PUBREL \(m(\d+)\)",
            r"received PUBCOMP \(Mid: (\d+), RC:0\)",
        ]
        frames = [re.search(pattern, result.stdout) for pattern in patterns]
        require(all(frames), "mosquitto_qos2_handshake_incomplete")
        require(len({match[1] for match in frames}) == 1
                and [match.start() for match in frames] == sorted(match.start() for match in frames),
                "mosquitto_qos2_handshake_order_or_identifier_mismatch")
        return {"argv": args, "packet_id": int(frames[0][1]), "debug": result.stdout.splitlines()}

    @contextmanager
    def subscribe(self, port, topic, name, *, explicit=False, qos=1):
        args = self.command("sub", port, topic, explicit=explicit, qos=qos) + ["-C", "1", "-W", "15", "-F", "%q %r %t %p"]
        log = self.out / f"{name}.log"
        with process(["stdbuf", "-oL", "-eL", *args], log, env=self.env) as child:
            def granted():
                require(child.poll() is None, "mosquitto_subscriber_exited_before_suback")
                return any(re.fullmatch(rf"Subscribed \(mid: \d+\): {qos}", line)
                           for line in log.read_text().splitlines())

            wait_for(granted, f"mosquitto_subscriber_did_not_observe_qos{qos}_suback")
            yield child, log, args


def mosquitto_result(subscriber, topic, payload, *, qos=1, granted=1):
    child, log, args = subscriber
    require(child.wait(timeout=TIMEOUT) == 0, "mosquitto_subscriber_failed")
    line = f"{qos} 0 {topic} {payload}"
    debug = log.read_text()
    require(debug.splitlines().count(line) == 1, "mosquitto_received_different_qos_topic_or_payload")
    result = {"argv": args, "received": line, "qos": qos, "retain": False,
              "requested_qos": int(args[args.index("-q") + 1]), "granted_qos": granted}
    if qos == 2:
        patterns = [
            r"received PUBLISH \(d0, q2, r0, m(\d+),",
            r"sending PUBREC \(m(\d+), rc0\)",
            r"received PUBREL \(Mid: (\d+)\)",
            r"sending PUBCOMP \(m(\d+)\)",
        ]
        frames = [re.search(pattern, debug) for pattern in patterns]
        require(all(frames) and len({match[1] for match in frames}) == 1
                and [match.start() for match in frames] == sorted(match.start() for match in frames),
                "mosquitto_subscriber_qos2_handshake_incomplete")
        result.update(packet_id=int(frames[0][1]), debug=debug.splitlines())
    return result


def exchange(clients, port, *, explicit, name):
    mode = "explicit" if explicit else "default"
    topic, payload = f"{TOPIC}/{mode}", f"mosquitto {mode} mqtt311"
    with clients.subscribe(port, topic, f"{name}-{mode}", explicit=explicit) as subscriber:
        pub = clients.publish(port, topic, payload, explicit=explicit)
        sub = mosquitto_result(subscriber, topic, payload)
    return {"mode": mode, "topic": topic, "payload": payload, "publisher": pub, "subscriber": sub}


def connect_levels(stream, since):
    return [{"connection": row["connection"], "level": row["details"]["protocol_level"]}
            for row in events(stream)[since:] if row["kind"] == "connect_validated"]


def observed_levels(stream, since, expected):
    observed = connect_levels(stream, since)
    require([row["level"] for row in observed] == expected, "unexpected_negotiated_protocol_levels")
    return observed


async def mosquitto_to_mqttium(port, stream, clients, pki, db, qos, granted=1):
    before = len(events(stream))
    topic = f"{TOPIC}/to-v5-qos{qos}-grant{granted}"
    payload = f"3.1.1 QoS {qos} publisher to MQTT 5 subscriber"
    async with AsyncClient("dashboard", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as subscriber:
        await subscriber.connect(HOST, port, ssl=tls(pki, "dashboard"), timeout=TIMEOUT)
        suback = await subscriber.subscribe(topic, qos=granted, timeout=TIMEOUT)
        require(list(suback.reason_codes) == [granted], "mqttium_suback_did_not_grant_requested_qos")
        async with asyncio.timeout(TIMEOUT):
            reader = subscriber.messages()
            try:
                published = await asyncio.to_thread(clients.publish, port, topic, payload, qos=qos)
                message = await anext(reader)
            finally:
                await reader.aclose()
        require((message.topic, message.payload, int(message.qos), message.retain)
                == (topic, payload.encode(), min(qos, granted), False), "mqttium_received_different_qos_topic_or_payload")
        if qos:
            await asyncio.to_thread(wait_for, lambda: deliveries(db, topic) == [],
                                    "mqttium_subscriber_delivery_did_not_finish")
    return {"topic": topic, "payload": payload, "qos": int(message.qos), "publisher": published,
            "published_qos": qos, "requested_qos": granted, "granted_qos": granted,
            "delivery_finished": True,
            "protocols": observed_levels(stream, before, [5, 4])}


async def interop(record, port, stream, clients, pki, db):
    record["mqtt311_to_v5"] = await mosquitto_to_mqttium(port, stream, clients, pki, db, 1)
    record["mqtt311_qos2_to_v5"] = await mosquitto_to_mqttium(port, stream, clients, pki, db, 2)

    before = len(events(stream))
    topic = f"{TOPIC}/from-v5"
    payload = "MQTT 5 publisher to 3.1.1 subscriber"
    with clients.subscribe(port, topic, "from-v5") as subscriber:
        async with AsyncClient("capteur-1", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as publisher:
            await publisher.connect(HOST, port, ssl=tls(pki, "sensor"), timeout=TIMEOUT)
            sent = await publisher.publish(topic, payload.encode(), qos=1, timeout=TIMEOUT)
            await sent.wait(timeout=TIMEOUT)
            require(sent.is_done() and sent.exception() is None, "mqttium_publisher_did_not_observe_puback")
        received = mosquitto_result(subscriber, topic, payload)
    record["mqtt5_to_v311"] = {"topic": topic, "payload": payload, "qos": 1, "mqttium_puback": sent.mid,
                              "subscriber": received, "protocols": observed_levels(stream, before, [4, 5])}

    cases = (
        ("both-legacy-qos2", 2, 4),
        ("mqtt5-to-legacy-qos2", 2, 5),
        ("qos1-to-grant2", 1, 4),
    )
    outgoing = {}
    for name, qos, publisher_level in cases:
        boundary = len(events(stream))
        topic, payload = f"{TOPIC}/{name}", f"mqttiumd delivery {name}"
        with clients.subscribe(port, topic, name, qos=2) as subscriber:
            if publisher_level == 4:
                published = clients.publish(port, topic, payload, qos=qos)
            else:
                async with AsyncClient("capteur-1", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as publisher:
                    await publisher.connect(HOST, port, ssl=tls(pki, "sensor"), timeout=TIMEOUT)
                    sent = await publisher.publish(topic, payload.encode(), qos=qos, timeout=TIMEOUT)
                    await sent.wait(timeout=TIMEOUT)
                    require(sent.is_done() and sent.exception() is None, "mqttium_qos2_publish_failed")
                    published = {"packet_id": sent.mid}
            received = mosquitto_result(subscriber, topic, payload, qos=qos, granted=2)
        wait_for(lambda: deliveries(db, topic) == [], "mosquitto_subscriber_delivery_did_not_finish")
        outgoing[name] = {"topic": topic, "payload": payload, "published_qos": qos,
                          "publisher": published, "subscriber": received, "delivery_finished": True,
                          "protocols": observed_levels(stream, boundary, [4, publisher_level])}
    outgoing["legacy-to-mqtt5-qos2"] = await mosquitto_to_mqttium(port, stream, clients, pki, db, 2, granted=2)
    record["outgoing_qos2_interop"] = outgoing


def field(value):
    data = value.encode() if isinstance(value, str) else value
    return len(data).to_bytes(2, "big") + data


class Wire311:
    """Small independent wire witness for exact ACK frames and transport close."""

    def __init__(self, pki, port, identity):
        self.connection = tls(pki, identity).wrap_socket(
            socket.create_connection((HOST, port), timeout=TIMEOUT), server_hostname=HOST,
        )
        self.source = self.connection.makefile("rb")

    def close(self):
        self.source.close()
        self.connection.close()

    def send(self, first, body=b""):
        remaining = len(body)
        header = bytearray([first])
        while remaining >= 128:
            header.append(128 | (remaining % 128))
            remaining //= 128
        header.append(remaining)
        self.connection.sendall(header + body)

    def receive(self):
        first = self.source.read(1)
        if not first:
            return None
        length = 0
        for shift in range(0, 28, 7):
            byte = self.source.read(1)
            require(byte, "truncated_remaining_length")
            length += (byte[0] & 127) << shift
            if byte[0] < 128:
                require(length <= 65536, "peer_frame_too_large")
                body = self.source.read(length)
                require(len(body) == length, "truncated_frame")
                return first[0], body
        raise ValueError("invalid_remaining_length")

    def expect(self, first, body):
        actual = self.receive()
        require(actual == (first, body), f"expected_{first:02x}_{body.hex()}_got_{actual!r}")

    def connect(self, client_id, *, clean=True):
        self.send(0x10, field("MQTT") + bytes([4, 2 if clean else 0, 0, 30]) + field(client_id))

    def subscribe(self, topic, *, mid=1, qos=1, grant=1):
        packet_id = mid.to_bytes(2, "big")
        self.send(0x82, packet_id + field(topic) + bytes([qos]))
        self.expect(0x90, packet_id + bytes([grant]))

    def publish(self, topic, payload, mid=None):
        self.send(0x30 if mid is None else 0x32,
                  field(topic) + (mid.to_bytes(2, "big") if mid is not None else b"") + payload)

    def publish_qos2(self, topic, payload, mid, *, duplicate=False):
        self.send(0x3c if duplicate else 0x34, field(topic) + mid.to_bytes(2, "big") + payload)


def wire_exchange(record, pki, port):
    topic = f"{TOPIC}/qos0"
    with closing(Wire311(pki, port, "sensor")) as foreign:
        foreign.connect("dashboard")
        foreign.expect(0x20, b"\0\x05")
        require(foreign.receive() is None, "unauthorized_legacy_connection_stayed_open")
    with closing(Wire311(pki, port, "dashboard")) as sub, closing(Wire311(pki, port, "sensor")) as pub:
        sub.connect("dashboard-check")
        sub.expect(0x20, b"\0\0")
        sub.subscribe(topic, qos=0, grant=0)
        pub.connect("capteur-1")
        pub.expect(0x20, b"\0\0")
        pub.subscribe("denied/+", grant=0x80)
        pub.send(0xc0)
        pub.expect(0xd0, b"")
        payload = b"\0\xfflegacy QoS0"
        pub.publish(topic, payload)
        sub.expect(0x30, field(topic) + payload)
        sub.send(0xa2, b"\0\x02" + field(topic))
        sub.expect(0xb0, b"\0\x02")
        sub.send(0xe0)
        pub.send(0xe0)
    record["wire"] = {"foreign_connack": "20020005", "denied_suback": "9003000180", "qos0_topic": topic,
                      "qos0_payload_hex": payload.hex(), "unsuback": "b0020002", "pingresp": "d000",
                      "disconnect": "e000"}


async def inherited_retain(record, pki, port, db):
    topic, payload = f"{TOPIC}/retained", b"\0retained-timed-handoff"
    async with AsyncClient("dashboard", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True,
                           connect_properties=Properties({"session_expiry_interval": 0xffffffff})) as target:
        await target.connect(HOST, port, ssl=tls(pki, "dashboard"), timeout=TIMEOUT)
        ack = await target.subscribe([(topic, SubscribeOptions(qos=1, retain_as_published=True))], timeout=TIMEOUT)
        require(tuple(ack.reason_codes) == (1,), "retained_route_not_granted")
    async with AsyncClient("capteur-1", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as publisher:
        await publisher.connect(HOST, port, ssl=tls(pki, "sensor"), timeout=TIMEOUT)
        sent = await publisher.publish(topic, payload, qos=1, retain=True,
                                       properties=Properties({"message_expiry_interval": 60}), timeout=TIMEOUT)
        await sent.wait(timeout=TIMEOUT)
        require(sent.is_done() and sent.exception() is None, "timed_retained_publish_was_not_admitted")
    with closing(Wire311(pki, port, "dashboard")) as target:
        target.connect("dashboard", clean=False)
        target.expect(0x20, b"\x01\0")
        for header in (0x32, 0x33):
            if header == 0x33:
                target.subscribe(topic, mid=9)
            packet = target.receive()
            require(packet and packet[0] == header, "legacy_retain_bit_disagrees_with_delivery_cause")
            field_len = len(field(topic))
            body = packet[1]
            require(body[:field_len] == field(topic) and body[field_len + 2:] == payload,
                    "legacy_timed_publish_added_a_property_or_changed_payload")
            target.send(0x40, body[field_len:field_len + 2])
        target.send(0xe0)
    wait_for(lambda: deliveries(db, topic) == [], "legacy_retained_claim_did_not_finish")
    record["retained_handoff"] = {"mqtt5_subscription_rap": True, "legacy_session_present": True,
                                  "legacy_live_header": "32", "legacy_replay_header": "33",
                                  "topic": topic, "exact_payload_hex": payload.hex()}


@contextmanager
def paho_session(pki, port, name, *, clean):
    client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=name, clean_session=clean,
                         protocol=paho.MQTTv311, reconnect_on_failure=False, manual_ack=True)
    client.tls_set_context(tls(pki, "dashboard"))
    connack, suback, messages = Queue(1), Queue(1), Queue(4)
    client.on_connect = lambda _c, _u, flags, reason, _p: connack.put_nowait((reason.value, bool(flags.session_present)))
    client.on_subscribe = lambda _c, _u, mid, reasons, _p: suback.put_nowait((mid, [reason.value for reason in reasons]))
    client.on_message = lambda _c, _u, message: messages.put_nowait(message)
    require(client.connect(HOST, port, keepalive=30) == paho.MQTT_ERR_SUCCESS, "paho_connect_call_failed")
    require(client.loop_start() == paho.MQTT_ERR_SUCCESS, "paho_loop_start_failed")
    try:
        yield client, connack.get(timeout=TIMEOUT), suback, messages
    finally:
        client.disconnect()
        client.loop_stop()


def query(db, sql, values=()):
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
        return connection.execute(sql, values).fetchall()


def deliveries(db, topic):
    return query(db, """
        SELECT s.client_id, p.topic, y.bytes, o.qos
        FROM obligations o
        JOIN publications p ON p.id=o.publication
        JOIN payloads y ON y.id=p.payload
        JOIN sessions s ON s.id=o.target
        WHERE p.topic=?
    """, (topic.encode(),))


def acl_connect(wire, level, *, persistent=False, resume=False, client_id="dashboard", receive_maximum=None):
    clean = not resume and (level == 5 or not persistent)
    properties = b""
    if level == 5:
        body = b"\x11\xff\xff\xff\xff" if persistent else b""
        if receive_maximum is not None:
            body += b"\x21" + receive_maximum.to_bytes(2, "big")
        properties = bytes([len(body)]) + body
    wire.send(0x10, field("MQTT") + bytes([level, 2 if clean else 0, 0, 30])
              + properties + field(client_id))
    hello = wire.receive()
    require(hello is not None and hello[0] == 0x20 and hello[1][:2] == bytes([int(resume), 0]),
            "acl_dashboard_connack")
    if level == 4:
        require(len(hello[1]) == 2, "properties_in_legacy_connack")


def acl_delivery(wire, level, topic, payload):
    frame = wire.receive()
    prefix = field(topic)
    require(frame is not None and frame[0] == 0x32 and frame[1].startswith(prefix), "acl_delivery_topic_or_qos")
    mid = frame[1][len(prefix):len(prefix) + 2]
    properties = b"\0" if level == 5 else b""
    require(int.from_bytes(mid, "big") > 0 and frame[1] == prefix + mid + properties + payload,
            "acl_delivery_payload")
    wire.send(0x40, mid)
    wire.send(0xc0)
    wire.expect(0xd0, b"")


def acl_publish(pki, port, payload):
    with closing(Wire311(pki, port, "sensor")) as pub:
        pub.connect("capteur-1")
        pub.expect(0x20, b"\0\0")
        for mid, topic in enumerate(("atelier/a", "autre/a"), 1):
            pub.publish(topic, payload, mid)
            pub.expect(0x40, mid.to_bytes(2, "big"))
        pub.send(0xe0)


def subscription_authorization(record, out, pki):
    # Keep the dashboard policy and certificates identical to the template.
    # Let the sensor publish both exact topics to make the denied route observable.
    pki = Path(shutil.copytree(pki, pki.with_name("acl-pki")))
    identities = pki / "identities.json"
    policy = json.loads(identities.read_text())
    next(rule for rule in policy["principals"] if rule["name"] == "sensor-1")["publish"].extend(("atelier/a", "autre/a"))
    identities.write_text(json.dumps(policy))
    results = {}
    for level, refusal in ((4, 0x80), (5, 0x87)):
        data = out / f"acl-{level}-data"
        db = data / "broker.sqlite"
        cases = [(("atelier/a",), [1]), (("autre/a",), [refusal]),
                 (("atelier/a", "autre/a"), [1, refusal]), (("autre/a", "atelier/a"), [refusal, 1])]
        props = b"\0" if level == 5 else b""
        rows = []
        with launch_broker(out, pki, f"acl-{level}-start", data, record) as (port, stream):
            for count, (filters, codes) in enumerate(cases, 1):
                persistent = count == len(cases)
                with closing(Wire311(pki, port, "dashboard")) as sub:
                    acl_connect(sub, level, persistent=persistent)
                    before = len(events(stream))
                    mid = count.to_bytes(2, "big")
                    sub.send(0x82, mid + props + b"".join(field(topic) + b"\x01" for topic in filters))
                    sub.expect(0x90, mid + props + bytes(codes))
                    observed = events(stream)[before:]
                    commits = [row for row in observed if row["kind"] == "store_result"
                               and row["command_kind"] == "Subscriptions" and row["details"]["committed_now"]]
                    commit = None
                    if 1 in codes:
                        commit, = commits
                        ack, = [row for row in observed if row["kind"] == "frame_queued"
                                and row["details"]["packet_kind"] == "SUBACK" and row["details"]["packet_id"] == count]
                        require(int(commit["seq"]) < int(ack["seq"]), "acl_suback_preceded_commit")
                    else:
                        require(not commits, "denied_only_subscription_reached_storage")
                    routes = query(db, "SELECT topic,qos FROM routes ORDER BY topic")
                    require(routes == ([(b"atelier/a", 1)] if 1 in codes else []), "acl_durable_route_differed")
                    rows.append({"filters": filters, "suback_codes": codes, "commit": commit})
                    if persistent:
                        acl_publish(pki, port, b"allowed-live")
                        acl_delivery(sub, level, "atelier/a", b"allowed-live")
                    sub.send(0xe0)
                if not persistent:
                    wait_for(lambda: query(db, "SELECT id FROM sessions WHERE client_id=?", (b"dashboard",)) == [],
                             "ephemeral_acl_session_not_removed")
            generation = observed_levels(stream, 0, [level] * len(cases) + [4])[len(cases) - 1]["connection"]
            wait_for(lambda: any(row["kind"] == "connection_closed" and row["connection"] == generation
                                 for row in events(stream)), "acl_dashboard_not_offline")
            acl_publish(pki, port, b"allowed-after-restart")
            require(deliveries(db, "atelier/a") == [(b"dashboard", b"atelier/a", b"allowed-after-restart", 1)]
                    and not deliveries(db, "autre/a"), "acl_wrong_offline_work")
        with launch_broker(out, pki, f"acl-{level}-resume", data, record) as (port, _):
            require(record["starts"][-2]["sqlite"] == record["starts"][-1]["sqlite"], "acl_restart_changed_database")
            with closing(Wire311(pki, port, "dashboard")) as sub:
                acl_connect(sub, level, persistent=True, resume=True)
                acl_delivery(sub, level, "atelier/a", b"allowed-after-restart")
                require(query(db, "SELECT topic,qos FROM routes ORDER BY topic") == [(b"atelier/a", 1)],
                        "acl_restart_restored_denied_route")
                sub.send(0xe0)
        results[str(level)] = {"cases": rows, "live": "allowed-live", "recovered": "allowed-after-restart",
                               "permitted_route": "atelier/a", "session_present": True}
    record["subscription_authorization"] = results


def subscription_capacity(record, out, pki, levels=(4, 5)):
    prefix = "atelier/capacity"
    seeded = [(f"{prefix}/{i:03}", 1) for i in range(127)]
    first, second = f"{prefix}/first", f"{prefix}/second"
    replacement = seeded[0][0]

    def subscribe(wire, level, number, filters):
        mid = number.to_bytes(2, "big")
        props = b"\0" if level == 5 else b""
        wire.send(0x82, mid + props + b"".join(field(name) + bytes([qos]) for name, qos in filters))
        header, body = wire.receive()
        require(header == 0x90 and body.startswith(mid + props), "capacity_suback_header")
        return list(body[len(mid + props):])

    def committed_before_suback(stream, boundary, number):
        rows = wait_for(lambda: [row for row in events(stream)[boundary:]
                                 if row["kind"] == "frame_queued"
                                 and row["details"]["packet_kind"] == "SUBACK"
                                 and row["details"]["packet_id"] == number], "capacity_suback_trace")
        ack, = rows
        commit, = [row for row in events(stream)[boundary:]
                   if row["kind"] == "store_result" and row["command_kind"] == "Subscriptions"
                   and row["details"]["committed_now"]]
        require(int(commit["seq"]) < int(ack["seq"]), "capacity_suback_preceded_commit")
        return [commit["seq"], ack["seq"]]

    result = record.setdefault("subscription_capacity", {})
    for level in levels:
        refusal, quota = (0x80, 0x80) if level == 4 else (0x87, 0x97)
        data = out / f"routes-{level}-data"
        db = data / "broker.sqlite"
        with launch_broker(out, pki, f"routes-{level}-start", data, record) as (port, stream):
            with closing(Wire311(pki, port, "dashboard")) as sub:
                acl_connect(sub, level, persistent=True)
                for start in range(0, len(seeded), 8):
                    batch = seeded[start:start + 8]
                    require(subscribe(sub, level, start + 1, batch) == [1] * len(batch), "capacity_seed")
                require(query(db, "SELECT count(*) FROM routes") == [(127,)], "capacity_fixture_not_at_limit")
                boundary = len(events(stream))
                added = subscribe(sub, level, 200, [(first, 1), (second, 1)])
                require(added == [1, quota], f"ordered_subscription_capacity_v{level}_got_{bytes(added).hex()}")
                added_order = committed_before_suback(stream, boundary, 200)
                boundary = len(events(stream))
                replaced = subscribe(sub, level, 201, [("outside/policy", 1), (second, 1), (replacement, 0)])
                require(replaced == [refusal, quota, 0], "replacement_at_full_capacity")
                replacement_order = committed_before_suback(stream, boundary, 201)
                sub.send(0xe0)
        expected = sorted([(name.encode(), 0 if name == replacement else qos) for name, qos in seeded]
                          + [(first.encode(), 1)])
        with launch_broker(out, pki, f"routes-{level}-resume", data, record) as (port, _):
            require(record["starts"][-2]["sqlite"] == record["starts"][-1]["sqlite"], "capacity_restart_changed_db")
            with closing(Wire311(pki, port, "dashboard")) as sub:
                acl_connect(sub, level, persistent=True, resume=True)
                require(query(db, "SELECT topic,qos FROM routes ORDER BY topic") == expected,
                        "capacity_recovered_routes_differ")
                sub.send(0xe0)
        result[str(level)] = {"after_127": added, "at_full": replaced, "routes_recovered": len(expected),
                              "replaced": replacement, "refused": second,
                              "commit_and_suback": [added_order, replacement_order]}


def durable_before(record, pki, port, stream, clients, db):
    trials = [("dashboard", False, f"{TOPIC}/persistent"), ("dashboard-check", True, f"{TOPIC}/clean")]
    for name, clean, topic in trials:
        before = len(events(stream))
        with paho_session(pki, port, name, clean=clean) as (sub, connack, suback, _):
            require(connack == (0, False), "initial_session_was_not_fresh")
            result, mid = sub.subscribe(topic, qos=1)
            require(result == paho.MQTT_ERR_SUCCESS and suback.get(timeout=TIMEOUT) == (mid, [1]),
                    "paho_subscription_was_not_granted")
        connection, = observed_levels(stream, before, [4])
        wait_for(lambda: any(row["kind"] == "connection_closed" and row["connection"] == connection["connection"]
                             for row in events(stream)[before:]), "subscriber_did_not_disconnect")
    payload = "offline MQTT 3.1.1 QoS 1"
    record["offline_published"] = [clients.publish(port, topic, payload) for _, _, topic in trials]
    persistent = trials[0][2]
    clean = trials[1][2]
    require(deliveries(db, persistent) == [(b"dashboard", persistent.encode(), payload.encode(), 1)],
            "offline_legacy_obligation_missing_before_restart")
    require(deliveries(db, clean) == [] and query(db, "SELECT count(*) FROM sessions WHERE client_id=?",
                                                 (b"dashboard-check",)) == [(0,)],
            "clean_session_left_offline_work")
    record["before_restart"] = {"persistent_obligations": 1, "clean_obligations": 0, "payload": payload}


def durable_after(record, pki, port, db):
    require(record["starts"][0]["sqlite"] == record["starts"][1]["sqlite"], "restart_changed_sqlite_file")
    topic, payload = f"{TOPIC}/persistent", record["before_restart"]["payload"]
    with paho_session(pki, port, "dashboard", clean=False) as (sub, connack, _, messages):
        require(connack == (0, True), "legacy_persistent_connack_lost_session")
        message = messages.get(timeout=TIMEOUT)
        require((message.topic, message.payload, message.qos, message.retain)
                == (topic, payload.encode(), 1, False), "legacy_offline_delivery_changed")
        require(sub.ack(message.mid, message.qos) == paho.MQTT_ERR_SUCCESS, "paho_ack_failed")
        wait_for(lambda: deliveries(db, topic) == [], "legacy_delivery_did_not_finish")
    with closing(Wire311(pki, port, "dashboard")) as clean:
        clean.connect("dashboard-check")
        clean.expect(0x20, b"\0\0")
        clean.send(0xc0)
        clean.expect(0xd0, b"")
        clean.send(0xe0)
    require(deliveries(db, f"{TOPIC}/clean") == [], "clean_session_replayed_offline_work")
    record["restart"] = {"persistent": {"session_present": True, "topic": topic, "payload": payload,
                                        "qos": 1, "packet_id": message.mid, "delivery_finished": True},
                         "clean": {"session_present": False, "next_frame": "PINGRESP", "offline_obligations": 0}}


def admission_commits(rows):
    return [row for row in rows if row["kind"] == "store_result" and row["command_kind"] == "Admission"
            and row["details"]["committed_now"] and row["details"]["wrote"]]


def qos2_marker(db, mid, topic, payload):
    rows = query(db, """
        SELECT hex(i.session), i.packet_id, hex(i.exchange), hex(i.publication), i.phase, p.qos, y.bytes
        FROM inbound i JOIN sessions s ON i.session=s.id
        JOIN publications p ON p.id=i.publication LEFT JOIN payloads y ON y.id=p.payload
        WHERE s.client_id=? AND i.packet_id=? AND p.topic=? AND p.source=i.session
    """, (b"capteur-1", mid, topic.encode()))
    require(len(rows) == 1, "persistent_qos2_marker_missing")
    marker, = rows
    require(marker[1] == mid and marker[4:] == (1, 2, payload), "invalid_persistent_qos2_marker")
    return dict(zip(("session", "packet_id", "exchange", "publication", "await_pubrel", "qos", "payload_hex"),
                    (*marker[:-1], payload.hex() if payload is not None else None), strict=True))


def written_ack(stream, since, kind, mid):
    return wait_for(lambda: next((row for row in events(stream)[since:]
                                 if row["kind"] == "frame_written" and row["details"]["packet_kind"] == kind
                                 and row["details"]["packet_id"] == mid), None), f"{kind.lower()}_not_written")


def received_qos1(wire, topic, payload):
    header, body = wire.receive()
    prefix = field(topic)
    require(header == 0x32 and body.startswith(prefix) and body[len(prefix) + 2:] == payload,
            "qos2_recipient_delivery_did_not_match")
    return body[len(prefix):len(prefix) + 2]


def qos2_recovery(record, out, pki):
    data = out / "qos2-data"
    db = data / "broker.sqlite"
    topic, mid = f"{TOPIC}/restart-qos2", 37
    identifier = mid.to_bytes(2, "big")
    original, fresh = b"admitted-before-pubrel", b"new-publish-after-pubcomp"
    wire = {"pubrec": (b"\x50\x02" + identifier).hex(), "pubrel": (b"\x62\x02" + identifier).hex(),
            "pubcomp": (b"\x70\x02" + identifier).hex()}
    stages, start = [], len(record["starts"])

    with launch_broker(out, pki, "qos2-pubrec", data, record) as (port, stream):
        with closing(Wire311(pki, port, "dashboard")) as sub, closing(Wire311(pki, port, "sensor")) as pub:
            sub.connect("dashboard", clean=False)
            sub.expect(0x20, b"\0\0")
            sub.subscribe(topic)
            pub.connect("capteur-1", clean=False)
            pub.expect(0x20, b"\0\0")
            before = len(events(stream))
            pub.publish_qos2(topic, original, mid)
            # Leave the publisher's PUBREC unread and never send PUBREL at this stage.
            ack = written_ack(stream, before, "PUBREC", mid)
            phase = events(stream)[before:]
            admission, = admission_commits(phase)
            queued, = [row for row in phase if row["kind"] == "frame_queued"
                       and row["details"]["packet_kind"] == "PUBREC" and row["details"]["packet_id"] == mid]
            require(int(admission["seq"]) < int(queued["seq"]) < int(ack["seq"]),
                    "qos2_pubrec_preceded_durable_admission")
            marker = qos2_marker(db, mid, topic, original)
            require(deliveries(db, topic) == [(b"dashboard", topic.encode(), original, 1)],
                    "qos2_obligation_missing_at_pubrec")
            outgoing = received_qos1(sub, topic, original)
            sub.send(0x40, outgoing)
            wait_for(lambda: deliveries(db, topic) == [], "qos2_delivery_did_not_finish_before_pubrel")
            # The final recipient releases the payload bytes. The inbound marker
            # retains its publication identity and can still answer a replay.
            pending = dict(marker, payload_hex=None)
            require(qos2_marker(db, mid, topic, None) == pending, "qos2_marker_lost_before_pubrel")
            stages.append({"loss": "publisher_closed_without_reading_pubrec", "marker": marker,
                           "admission": admission, "pubrec_queued": queued, "pubrec_written": ack,
                           "recipient_qos": 1, "recipient_mid": int.from_bytes(outgoing, "big"),
                           "recipient_finished_before_pubrel": True})

    with launch_broker(out, pki, "qos2-pubcomp", data, record) as (port, stream):
        with closing(Wire311(pki, port, "sensor")) as pub:
            pub.connect("capteur-1", clean=False)
            pub.expect(0x20, b"\1\0")
            before = len(events(stream))
            pub.publish_qos2(topic, original, mid, duplicate=True)
            pub.expect(0x50, identifier)
            require(qos2_marker(db, mid, topic, None) == pending,
                    "duplicate_publish_changed_the_recovered_exchange")
            require(deliveries(db, topic) == [], "duplicate_publish_recreated_recipient_work")
            pub.send(0x62, identifier)
            ack = written_ack(stream, before, "PUBCOMP", mid)
            require(query(db, "SELECT packet_id FROM inbound") == [], "pubcomp_with_unfinished_exchange")
            require(admission_commits(events(stream)[before:]) == [], "duplicate_publish_or_pubrel_admitted_again")
            stages.append({"session_present": True, "duplicate_publish": "3c", "wire_pubrec": wire["pubrec"],
                           "loss": "publisher_closed_without_reading_pubcomp", "pubcomp_written": ack,
                           "durable_admissions": 0, "inbound_after_pubcomp": 0, "marker_before_pubrel": pending})

    with launch_broker(out, pki, "qos2-reuse", data, record) as (port, stream):
        with closing(Wire311(pki, port, "sensor")) as pub:
            pub.connect("capteur-1", clean=False)
            pub.expect(0x20, b"\1\0")
            before = len(events(stream))
            pub.send(0x62, identifier)
            pub.expect(0x70, identifier)
            require(admission_commits(events(stream)[before:]) == [] and deliveries(db, topic) == [],
                    "duplicate_pubrel_created_an_admission")

            pub.publish_qos2(topic, fresh, mid)
            pub.expect(0x50, identifier)
            second = qos2_marker(db, mid, topic, fresh)
            require(second["session"] == marker["session"] and second["exchange"] != marker["exchange"]
                    and second["publication"] != marker["publication"], "reused_qos2_id_did_not_create_new_exchange")
            require(deliveries(db, topic) == [(b"dashboard", topic.encode(), fresh, 1)],
                    "reused_qos2_id_did_not_admit_new_payload")
            pub.send(0x62, identifier)
            pub.expect(0x70, identifier)
            require(query(db, "SELECT packet_id FROM inbound") == [], "new_exchange_did_not_terminate")
            created, = admission_commits(events(stream)[before:])
            stages.append({"session_present": True, "duplicate_pubrel": wire["pubrel"], "wire_pubcomp": wire["pubcomp"],
                           "reused_packet_id": mid, "new_marker": second, "new_admission": created})
        with closing(Wire311(pki, port, "dashboard")) as sub:
            sub.connect("dashboard", clean=False)
            sub.expect(0x20, b"\1\0")
            sub.send(0x40, received_qos1(sub, topic, fresh))
            wait_for(lambda: deliveries(db, topic) == [], "new_qos2_obligation_did_not_finish")
    require(len({tuple(row["sqlite"]) for row in record["starts"][start:]}) == 1, "qos2_restart_changed_sqlite")
    record["qos2_recovery"] = {"topic": topic, "wire": wire, "packet_id": mid, "stages": stages,
                              "original_admissions": 1, "replays_admitted": 0, "fresh_admissions": 1}


def grant_qos2(wire, level, topic):
    props = b"\0" if level == 5 else b""
    wire.send(0x82, b"\0\x01" + props + field(topic) + b"\x02")
    wire.expect(0x90, b"\0\x01" + props + b"\x02")


def wire_publish(port, pki, level, topic, payload, *, qos=2, expiry=None):
    with closing(Wire311(pki, port, "sensor")) as pub:
        acl_connect(pub, level, client_id="capteur-1")
        mid = b"\0\x09"
        props = b"" if level == 4 else b"\0"
        if expiry is not None:
            require(level == 5, "expiry_requires_mqtt5")
            props = b"\x05\x02" + expiry.to_bytes(4, "big")
        pub.send(0x30 | (qos << 1), field(topic) + mid + props + payload)
        if qos == 2:
            pub.expect(0x50, mid)
            pub.send(0x62, mid)
            pub.expect(0x70, mid)
        else:
            pub.expect(0x40, mid)
        pub.send(0xe0)


def decode_delivery(frame, level, topic, payload, qos, *, dup=False):
    expected = 0x30 | qos << 1 | (8 if dup else 0)
    require(frame is not None and frame[0] == expected,
            f"expected_delivery_{expected:02x}_got_{frame!r}")
    _, body = frame
    prefix = field(topic)
    require(body.startswith(prefix), "outgoing_qos2_topic_mismatch")
    mid = body[len(prefix):len(prefix) + 2]
    require(len(mid) == 2 and mid != b"\0\0", "invalid_outgoing_packet_id")
    remainder = body[len(prefix) + 2:]
    if level == 5:
        require(remainder and remainder[0] < 128, "invalid_probe_publication_properties")
        remainder = remainder[1 + remainder[0]:]
    require(remainder == payload, "outgoing_qos2_payload_mismatch")
    return mid


def outbound_state(db):
    return query(db, """
        SELECT hex(o.publication), o.idx, o.qos, o.phase, c.packet_id, hex(c.claim), c.started
        FROM obligations o LEFT JOIN outbound c USING(publication,idx)
        ORDER BY o.publication,o.idx
    """)


def finish_recipient(wire, db, stream, mid):
    boundary = len(events(stream))
    wire.send(0x70, mid)
    wait_for(lambda: outbound_state(db) == [], "pubcomp_did_not_finish_delivery")
    return wait_for(lambda: next((row for row in events(stream)[boundary:]
                                 if row["kind"] == "store_result" and row["command_kind"] == "Finish"
                                 and row["details"]["committed_now"]), None), "missing_pubcomp_commit")


def outgoing_crashes(record, out, pki):
    trials = {}
    for level in (4, 5):
        for withheld, phase in (("PUBREC", 1), ("PUBCOMP", 2)):
            name = f"outgoing-{level}-{withheld.lower()}"
            data = out / f"{name}-data"
            db = data / "broker.sqlite"
            topic = f"{TOPIC}/{name}"
            payload = b"one persistent QoS 2 delivery"
            first_commit = None
            with launch_broker(out, pki, f"{name}-cut", data, record, killed=True) as (port, stream):
                with closing(Wire311(pki, port, "dashboard")) as sub:
                    acl_connect(sub, level, persistent=True)
                    grant_qos2(sub, level, topic)
                    wire_publish(port, pki, level, topic, payload)
                    mid = decode_delivery(sub.receive(), level, topic, payload, 2)
                    begun, = outbound_state(db)
                    require(begun[2:5] == (2, 1, int.from_bytes(mid, "big")), "qos2_claim_not_durable_before_publish")
                    if phase == 2:
                        boundary = len(events(stream))
                        sub.send(0x50, mid)
                        sub.expect(0x62, mid)
                        first_commit = wait_for(lambda: next((row for row in events(stream)[boundary:]
                            if row["kind"] == "store_result" and row["command_kind"] == "DeliveryPubrelReady"
                            and row["details"]["committed_now"]), None), "pubrel_commit_missing")
                        queued = wait_for(lambda: next((row for row in events(stream)[boundary:]
                            if row["kind"] == "frame_queued" and row["details"]["packet_kind"] == "PUBREL"), None),
                            "pubrel_queue_missing")
                        require(int(first_commit["seq"]) < int(queued["seq"]), "pubrel_queued_before_commit")
                    durable, = outbound_state(db)
                    require(durable[:3] == begun[:3] and durable[3] == phase and durable[4:] == begun[4:],
                            "qos2_wrong_durable_phase_at_crash")
                    os.kill(record["starts"][-1]["pid"], signal.SIGKILL)
            require(outbound_state(db) == [durable], "qos2_obligation_changed_at_crash")
            with launch_broker(out, pki, f"{name}-restart", data, record) as (port, stream):
                require(record["starts"][-2]["sqlite"] == record["starts"][-1]["sqlite"], "qos2_restart_changed_file")
                with closing(Wire311(pki, port, "dashboard")) as sub:
                    acl_connect(sub, level, persistent=True, resume=True)
                    first = sub.receive()
                    if phase == 1:
                        require(decode_delivery(first, level, topic, payload, 2, dup=True) == mid,
                                "qos2_publish_replay_changed_id")
                        require(outbound_state(db) == [durable], "qos2_replay_created_an_obligation")
                        sub.send(0x50, mid)
                        sub.expect(0x62, mid)
                    else:
                        require(first == (0x62, mid), f"qos2_replay_pubrel_required_got_{first!r}")
                        require(outbound_state(db) == [durable], "qos2_replay_created_an_obligation")
                    terminal = finish_recipient(sub, db, stream, mid)
                    new_payload = b"new delivery after the durable finish"
                    wire_publish(port, pki, level, topic, new_payload)
                    fresh = decode_delivery(sub.receive(), level, topic, new_payload, 2)
                    next_row, = outbound_state(db)
                    require(fresh == mid and next_row[0] != durable[0] and next_row[5] != durable[5],
                            "qos2_packet_id_not_reused_for_new_occurrence")
                    sub.send(0x50, fresh)
                    sub.expect(0x62, fresh)
                    finish_recipient(sub, db, stream, fresh)
                    sub.send(0xe0)
            trials[name] = {"protocol_level": level, "withheld": withheld, "before_kill": durable,
                            "generation_restarted": True, "session_present": True,
                            "first_frame": {"header": f"{first[0]:02x}", "body": first[1].hex()},
                            "same_obligation": True, "pubrel_commit": first_commit,
                            "finish_commit": terminal, "obligations_after_pubcomp": 0,
                            "new_packet_id": int.from_bytes(fresh, "big")}
    record["outgoing_qos2_crashes"] = trials


def outgoing_receive_maximum(record, out, pki):
    data = out / "outgoing-window-data"
    db = data / "broker.sqlite"
    outcomes = {}
    with launch_broker(out, pki, "outgoing-window", data, record) as (port, stream):
        for mode in ("positive_expired", "negative"):
            topic = f"{TOPIC}/window/{mode}"
            with closing(Wire311(pki, port, "dashboard")) as sub:
                acl_connect(sub, 5, receive_maximum=1)
                grant_qos2(sub, 5, topic)
                expiry = 2 if mode == "positive_expired" else None
                wire_publish(port, pki, 5, topic, b"holds the window", expiry=expiry)
                first = sub.receive()
                mid = decode_delivery(first, 5, topic, b"holds the window", 2)
                if expiry is not None:
                    started = monotonic()
                    wait_for(lambda: outbound_state(db)[0][6] == 1, "expiry_started_marker_missing")
                wire_publish(port, pki, 5, topic, b"waiting QoS 1", qos=1)
                states = outbound_state(db)
                require(len(states) == 2 and states[0][4] is not None and states[1][4] is None,
                        "receive_maximum_not_one_before_pubrec")
                if mode == "positive_expired":
                    sleep(max(0, started + expiry + 0.1 - monotonic()))
                    require(outbound_state(db) == states, "expired_started_qos2_was_discarded")
                    sub.send(0x50, mid)
                    sub.expect(0x62, mid)
                    sub.send(0xc0)
                    require(sub.receive() == (0xd0, b""), "receive_maximum_released_before_pubcomp")
                    ready, still_waiting = outbound_state(db)
                    require(ready[3] == 2 and ready[4] == int.from_bytes(mid, "big") and still_waiting[4] is None,
                            "receive_maximum_released_before_pubcomp")
                    sub.send(0x70, mid)
                else:
                    sub.send(0x50, mid + b"\x80")
                next_frame = sub.receive()
                next_mid = decode_delivery(next_frame, 5, topic, b"waiting QoS 1", 1)
                require(next_mid == mid, "receive_window_released_without_packet_id")
                sub.send(0x40, next_mid)
                wait_for(lambda: outbound_state(db) == [], "receive_maximum_queued_delivery_did_not_finish")
                sub.send(0xe0)
            outcomes[mode] = {"maximum": 1, "packet_id": int.from_bytes(mid, "big"),
                              "pubrec_reason": 0 if expiry is not None else 0x80,
                              "expired_after_publish": expiry is not None, "pubrel": expiry is not None,
                              "waiting_publish_qos": 1, "obligations_after_completion": 0}
    record["outgoing_qos2_receive_maximum"] = outcomes


def reduced_receive_maximum(record, out, pki, *, package=PACKAGE):
    # Both deliveries belong to the same persistent subscription. B has
    # reached PUBREL before the cut, while A still awaits PUBREC.
    trials = record["reduced_receive_maximum"] = {}
    for order in (("A", "B"), ("B", "A")):
        name = "reduced-window-" + "".join(order)
        topic = f"{TOPIC}/{name}"
        data = out / f"{name}-data"
        db = data / "broker.sqlite"
        ids = {}
        with launch_broker(out, pki, f"{name}-cut", data, record, killed=True, package=package) as (port, _):
            with closing(Wire311(pki, port, "dashboard")) as sub:
                acl_connect(sub, 5, persistent=True, receive_maximum=2)
                grant_qos2(sub, 5, topic)
                for letter in order:
                    wire_publish(port, pki, 5, topic, letter.encode())
                    ids[letter] = decode_delivery(sub.receive(), 5, topic, letter.encode(), 2)
                    if letter == "B":
                        sub.send(0x50, ids[letter])
                        sub.expect(0x62, ids[letter])
                durable = outbound_state(db)
                require([row[2:5] for row in durable] == [
                    (2, 1 if letter == "A" else 2, int.from_bytes(ids[letter], "big"))
                    for letter in order
                ] and ids["A"] != ids["B"], "reduced_window_wrong_phase_before_cut")
                os.kill(record["starts"][-1]["pid"], signal.SIGKILL)
        require(outbound_state(db) == durable, "reduced_window_cut_changed_obligations")

        with launch_broker(out, pki, f"{name}-resume", data, record, package=package) as (port, stream):
            require(record["starts"][-2]["sqlite"] == record["starts"][-1]["sqlite"], "reduced_window_changed_db")
            with closing(Wire311(pki, port, "dashboard")) as sub:
                acl_connect(sub, 5, persistent=True, resume=True, receive_maximum=1)

                def observe(letter):
                    frame = sub.receive()
                    if letter == "A":
                        require(decode_delivery(frame, 5, topic, b"A", 2, dup=True) == ids[letter],
                                "reduced_window_publish_changed_identifier")
                    else:
                        require(frame == (0x62, ids[letter]), "reduced_window_expected_pubrel")
                    return f"{frame[0]:02x}:{ids[letter].hex()}"

                def finish(letter):
                    if letter == "A":
                        sub.send(0x50, ids[letter])
                        sub.expect(0x62, ids[letter])
                    sub.send(0x70, ids[letter])
                    wait_for(lambda: all(row[4] != int.from_bytes(ids[letter], "big") for row in outbound_state(db)),
                             "reduced_window_finish_did_not_commit")

                first, second = order
                frames = [observe(first)]
                kind = "PUBREL" if second == "B" else "PUBLISH"
                try:
                    wait_for(lambda: any(row["kind"] == "frame_written"
                                         and row["details"]["packet_kind"] == kind
                                         and row["details"]["packet_id"] == int.from_bytes(ids[second], "big")
                                         for row in events(stream)), "second_replay_waited_for_ack", timeout=1.5)
                except TimeoutError:
                    # Positive evidence on the baseline: PING still works,
                    # and the withheld frame follows completion of the first.
                    sub.send(0xc0)
                    sub.expect(0xd0, b"")
                    require(outbound_state(db) == durable, "reduced_window_changed_before_ack")
                    finish(first)
                    frames.append(observe(second))
                    finish(second)
                    before_ack = False
                else:
                    frames.append(observe(second))
                    require(outbound_state(db) == durable, "reduced_window_allocated_new_claim")
                    finish("B")
                    finish("A")
                    before_ack = True
                require(outbound_state(db) == [], "reduced_window_did_not_drain")
                sub.send(0xe0)
        trials[name] = {"initial_receive_maximum": 2, "resumed_receive_maximum": 1, "session_present": True,
                        "before_cut": durable, "frames": frames, "second_frame_before_ack": before_ack,
                        "obligations_after_completion": 0}
    return all(case["second_frame_before_ack"] for case in trials.values())


def capacity(record, pki, port, stream, db):
    topic = f"{TOPIC}/capacity"
    with closing(Wire311(pki, port, "dashboard")) as sub:
        before = len(events(stream))
        sub.connect("dashboard", clean=False)
        sub.expect(0x20, b"\0\0")
        sub.subscribe(topic)
        sub.send(0xe0)
    connection, = observed_levels(stream, before, [4])
    wait_for(lambda: any(row["kind"] == "connection_closed" and row["connection"] == connection["connection"]
                         for row in events(stream)[before:]), "capacity_subscriber_did_not_disconnect")
    with closing(Wire311(pki, port, "sensor")) as pub:
        pub.connect("capteur-1")
        pub.expect(0x20, b"\0\0")
        before = len(events(stream))
        pub.publish(topic, b"committed-first", 1)
        pub.expect(0x40, b"\0\x01")
        require(deliveries(db, topic) == [(b"dashboard", topic.encode(), b"committed-first", 1)],
                "first_ack_without_protected_offline_obligation")
        window = events(stream)[before:]
        commit, = admission_commits(window)
        ack, = [row for row in window if row["kind"] == "frame_queued" and row["details"]["packet_kind"] == "PUBACK"]
        require(int(commit["seq"]) < int(ack["seq"]) and ack["details"]["packet_id"] == 1,
                "first_puback_preceded_commit")
        for mid in range(2, 161):
            boundary = len(events(stream))
            pub.publish(topic, f"capacity-{mid}".encode(), mid)
            wire = pub.receive()
            if wire is None:
                break
            require(wire == (0x40, mid.to_bytes(2, "big")), "unexpected_capacity_response")
        else:
            raise ValueError("capacity_was_not_reached")

    terminal = wait_for(lambda: next((row for row in events(stream)[boundary:]
                                     if row["kind"] == "connection_closed"
                                     and row["details"]["code"] == "capacity_exceeded"), None),
                        "missing_capacity_disconnect")
    last = events(stream)[boundary:]
    require(not any(row["kind"] == "frame_queued" and row["connection"] == terminal["connection"]
                    and row["details"]["packet_kind"] == "PUBACK" for row in last), "puback_queued_for_refused_publish")
    require(mid == 129, f"expected_128_pubacks_before_close_at_129_got_{mid - 1}")
    accepted = sorted([b"committed-first", *(f"capacity-{i}".encode() for i in range(2, 129))])
    require(sorted(row[2] for row in deliveries(db, topic)) == accepted, "capacity_changed_durable_obligations")
    record["capacity"] = {"positive_pubacks": mid - 1, "refused_packet_id": mid, "wire_after_refused_publish": "EOF",
                          "disconnect": terminal, "refused_puback_queued": False, "first_commit": commit,
                          "first_wire_puback": "40020001", "first_puback_queued": ack,
                          "durable_obligations": mid - 1, "topic": topic}

    def durable_work():
        return [query(db, f"SELECT * FROM {name} ORDER BY 1, 2")
                for name in ("inbound", "publications", "obligations", "payloads")]

    with closing(Wire311(pki, port, "sensor")) as pub:
        pub.connect("capteur-1", clean=False)
        pub.expect(0x20, b"\0\0")
        before, durable = len(events(stream)), durable_work()
        pub.publish_qos2(topic, b"quota-refused-qos2", 7)
        require(pub.receive() is None, "saturated_qos2_connection_sent_a_wire_ack")
        refused = wait_for(lambda: next((row for row in events(stream)[before:]
                                         if row["kind"] == "connection_closed"
                                         and row["details"]["code"] == "capacity_exceeded"), None),
                           "qos2_capacity_disconnect_missing")
        trace = events(stream)[before:]
        require(not any(row["kind"] == "frame_queued" and row["connection"] == refused["connection"]
                        and row["details"]["packet_kind"] == "PUBREC" for row in trace),
                "qos2_pubrec_queued_for_refused_publish")
        require(admission_commits(trace) == [] and durable_work() == durable,
                "qos2_refusal_partially_changed_durable_work")
    record["qos2_capacity"] = {"packet_id": 7, "wire": "EOF", "disconnect": refused,
                               "pubrec_queued": False, "durable_work_unchanged": True}


def normalized_arguments(exchange_record):
    def normalized(argv):
        copy = list(argv)
        for flag in ("-h", "-p"):
            copy[copy.index(flag) + 1] = "<endpoint>"
        return copy

    return [normalized(exchange_record[role]["argv"]) for role in ("publisher", "subscriber")]


def exercise(out, pki, reference, record, *, cutover_only=False):
    clients = Mosquitto(out, pki, reference)
    reference_hash = sha(reference.parent / f"mosquitto-{REFERENCE}.tar.gz")
    require(reference_hash == REFERENCE_SHA256, "reference_source_archive_digest_mismatch")
    record["reference"] = {"version": REFERENCE, "binary_sha256": sha(reference / "src/mosquitto"),
                           "source_archive_sha256": reference_hash, "exchanges": [],
                           "clients": {role: sha(reference / f"client/mosquitto_{role}") for role in ("pub", "sub")}}
    from cutover_demo import exercise_cutover

    exercise_cutover(sys.modules[__name__], out, pki, reference, record, clients)
    if cutover_only:
        return
    with launch_reference(out, pki, reference) as (port, reference_log):
        for explicit in (False, True):
            before = len(reference_log.read_text().splitlines())
            trial = exchange(clients, port, explicit=explicit, name="reference")
            connections = [line for line in reference_log.read_text().splitlines()[before:]
                           if "New client connected" in line]
            # p2 is mosq_p_mqtt311 in the pinned reference source.
            require(len(connections) == 2 and all("(p2," in line for line in connections),
                    "reference_did_not_negotiate_mqtt311")
            trial["connections"] = connections
            record["reference"]["exchanges"].append(trial)
    data = out / "data"
    db = data / "broker.sqlite"
    with launch_broker(out, pki, "start-1", data, record) as (port, stream):
        record["mqttiumd_exchanges"] = []
        for explicit, original in zip((False, True), record["reference"]["exchanges"], strict=True):
            before = len(events(stream))
            current = exchange(clients, port, explicit=explicit, name="mqttiumd")
            require(normalized_arguments(current) == normalized_arguments(original),
                    "reference_and_candidate_client_invocations_changed")
            require(current["subscriber"]["received"] == original["subscriber"]["received"], "wire_comparison_mismatch")
            current["protocols"] = observed_levels(stream, before, [4, 4])
            record["mqttiumd_exchanges"].append(current)
        asyncio.run(interop(record, port, stream, clients, pki, db))
        wire_exchange(record, pki, port)
        durable_before(record, pki, port, stream, clients, db)
    with launch_broker(out, pki, "start-2", data, record) as (port, _):
        durable_after(record, pki, port, db)
        asyncio.run(inherited_retain(record, pki, port, db))
    subscription_authorization(record, out, pki)
    subscription_capacity(record, out, pki)
    qos2_recovery(record, out, pki)
    outgoing_crashes(record, out, pki)
    outgoing_receive_maximum(record, out, pki)
    resumed = reduced_receive_maximum(record, out, pki)
    receipt = {key: record[key] for key in
               ("machine", "broker_source_sha", "binary_sha256", "archive_sha256", "reduced_receive_maximum")}
    receipt.update(format="mqttiumd.reduced-receive-maximum.v1", status="PASS" if resumed else "FAIL",
                   starts=[entry for entry in record["starts"] if entry["name"].startswith("reduced-window-")])
    (out / "apres.json").write_text(json.dumps(receipt, indent=2) + "\n")
    require(resumed, "replay_depended_on_an_ack_from_the_reduced_window")
    with launch_broker(out, pki, "capacity", out / "capacity-data", record) as (port, stream):
        capacity(record, pki, port, stream, out / "capacity-data/broker.sqlite")


def main():
    os.umask(0o077)
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("out", type=Path)
    cli.add_argument("reference", type=Path, help="Mosquitto 2.0.21 source and binaries from build-mosquitto-reference.sh")
    cli.add_argument("archive_sha256", help="digest recorded by the external archive verifier")
    cli.add_argument("--cutover-only", action="store_true", help="run the operator switch and rollback only")
    args = cli.parse_args()
    out, reference = args.out.resolve(), args.reference.resolve()
    out.mkdir(mode=0o700)
    build = json.loads((PACKAGE / "BUILD.json").read_text())
    record = {"format": "mqttiumd.mqtt311.interop-receipt.v1", "broker_source_sha": build["source_sha"],
              "binary_sha256": sha(PACKAGE / "mqttiumd"), "archive_sha256": args.archive_sha256,
              "machine": platform.machine(), "platform": platform.platform(), "glibc": platform.libc_ver(),
              "starts": [], "clients": {"mqttium": version("mqttium"), "paho-mqtt": version("paho-mqtt")},
              "scope": "normal extracted loopback mTLS preview"}
    try:
        require(re.fullmatch(r"[0-9a-f]{64}", args.archive_sha256), "invalid_archive_sha256")
        require(record["binary_sha256"] == build["binary_sha256"] and record["machine"] == build["machine"],
                "binary_or_platform_mismatch")
        require(record["clients"] == {"mqttium": "1.3.0", "paho-mqtt": "2.1.0"}, "client_versions_mismatch")
        # Credentials live outside the evidence tree.
        with TemporaryDirectory(prefix="mqttiumd-311-") as scratch:
            pki = Path(scratch) / "pki"
            subprocess.run(["python3", PACKAGE / "demo-pki.py", pki], check=True, timeout=30)
            exercise(out, pki, reference, record, cutover_only=args.cutover_only)
    except BaseException as error:
        record.update(status="FAIL", error=f"{type(error).__name__}: {error}")
        raise
    else:
        record["status"] = "PASS"
    finally:
        (out / "mqtt311.json").write_text(json.dumps(record, indent=2) + "\n")
    print(out / "mqtt311.json")


if __name__ == "__main__":
    main()
