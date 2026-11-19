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
import signal
import socket
import sqlite3
import ssl
import subprocess
from tempfile import TemporaryDirectory
from time import monotonic, sleep

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


def wait_for(probe, failure):
    deadline = monotonic() + TIMEOUT
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
def launch_broker(out, pki, name, data, record):
    logs = data / "logs"
    previous = set(logs.glob("run.*/events.jsonl"))
    command = [PACKAGE / "start-tls.sh", data, pki]
    stream = None
    try:
        with process(command, out / f"{name}.log", broker=True,
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
    finally:
        if stream is not None:
            (out / f"{name}.jsonl").write_bytes(stream.read_bytes())


@contextmanager
def launch_reference(out, pki, source):
    with socket.socket() as reservation:
        reservation.bind((HOST, 0))
        port = reservation.getsockname()[1]
    acl = out / "reference.acl"
    acl.write_text("user sensor\ntopic write atelier/capteur-1/#\nuser dashboard\ntopic read atelier/#\n")
    config = out / "reference.conf"
    config.write_text("\n".join([
        f"listener {port} {HOST}", "persistence false", "allow_anonymous false",
        "require_certificate true", "use_identity_as_username true", "tls_version tlsv1.3",
        f"acl_file {acl}", f"cafile {pki / 'ca.crt'}", f"certfile {pki / 'server.crt'}",
        f"keyfile {pki / 'server.key'}", "log_type all", "",
    ]))
    log = out / "reference.log"
    with process([source / "src/mosquitto", "-c", config], log, broker=True) as child:
        def ready():
            require(child.poll() is None, "reference_exited_from_startup")
            return f"mosquitto version {REFERENCE} running" in log.read_text()

        wait_for(ready, "reference_not_ready")
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

    def command(self, role, port, topic, *, explicit=False):
        publisher = role == "pub"
        cert, client_id = ("sensor", "capteur-1") if publisher else ("dashboard", "dashboard")
        command = [str(self.source / "client" / f"mosquitto_{role}"), "-h", HOST, "-p", str(port),
                   "--tls-version", "tlsv1.3", "--cafile", str(self.pki / "ca.crt"),
                   "--cert", str(self.pki / f"{cert}.crt"), "--key", str(self.pki / f"{cert}.key"),
                   "-i", client_id, "-d", "-q", "1", "-t", topic]
        if explicit:
            command += ["-V", "mqttv311"]
        return command

    def publish(self, port, topic, payload, *, explicit=False):
        args = self.command("pub", port, topic, explicit=explicit) + ["-m", payload]
        result = subprocess.run(args, env=self.env, text=True, capture_output=True, check=True, timeout=TIMEOUT)
        require("received PUBACK" in result.stdout, "mosquitto_publisher_did_not_observe_puback")
        return {"argv": args, "puback": result.stdout.splitlines()}

    @contextmanager
    def subscribe(self, port, topic, name, *, explicit=False):
        args = self.command("sub", port, topic, explicit=explicit) + ["-C", "1", "-W", "15", "-F", "%q %r %t %p"]
        log = self.out / f"{name}.log"
        with process(["stdbuf", "-oL", "-eL", *args], log, env=self.env) as child:
            def granted():
                require(child.poll() is None, "mosquitto_subscriber_exited_before_suback")
                return any(re.fullmatch(r"Subscribed \(mid: \d+\): 1", line)
                           for line in log.read_text().splitlines())

            wait_for(granted, "mosquitto_subscriber_did_not_observe_qos1_suback")
            yield child, log, args


def mosquitto_result(subscriber, topic, payload):
    child, log, args = subscriber
    require(child.wait(timeout=TIMEOUT) == 0, "mosquitto_subscriber_failed")
    line = f"1 0 {topic} {payload}"
    require(log.read_text().splitlines().count(line) == 1, "mosquitto_received_different_qos_topic_or_payload")
    return {"argv": args, "received": line, "qos": 1, "retain": False}


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


async def interop(record, port, stream, clients, pki):
    before = len(events(stream))
    topic = f"{TOPIC}/to-v5"
    payload = "3.1.1 publisher to MQTT 5 subscriber"
    async with AsyncClient("dashboard", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True) as subscriber:
        await subscriber.connect(HOST, port, ssl=tls(pki, "dashboard"), timeout=TIMEOUT)
        suback = await subscriber.subscribe(topic, qos=1, timeout=TIMEOUT)
        require(list(suback.reason_codes) == [1], "mqttium_suback_did_not_grant_qos1")
        async with asyncio.timeout(TIMEOUT):
            reader = subscriber.messages()
            try:
                published = await asyncio.to_thread(clients.publish, port, topic, payload)
                message = await anext(reader)
            finally:
                await reader.aclose()
        require((message.topic, message.payload, int(message.qos), message.retain)
                == (topic, payload.encode(), 1, False), "mqttium_received_different_qos_topic_or_payload")
    record["mqtt311_to_v5"] = {"topic": topic, "payload": payload, "qos": int(message.qos), "publisher": published,
                              "protocols": observed_levels(stream, before, [5, 4])}

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
    with closing(Wire311(pki, port, "sensor")) as qos2:
        qos2.connect("capteur-1")
        qos2.expect(0x20, b"\0\0")
        qos2.send(0x34, field(topic) + b"\0\x07qos2-outside-profile")
        require(qos2.receive() is None, "qos2_received_a_legacy_acknowledgement")
    record["wire"] = {"foreign_connack": "20020005", "denied_suback": "9003000180", "qos0_topic": topic,
                      "qos0_payload_hex": payload.hex(), "unsuback": "b0020002", "pingresp": "d000",
                      "disconnect": "e000", "qos2_publish_response": "EOF"}


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
        commit, = [row for row in window if row["kind"] == "store_result" and row["command_kind"] == "Admission"
                   and row["details"]["committed_now"] and row["details"]["wrote"]]
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
    require(len(deliveries(db, topic)) == mid - 1 and mid > 2, "refused_publication_changed_durable_obligations")
    record["capacity"] = {"positive_pubacks": mid - 1, "refused_packet_id": mid, "wire_after_refused_publish": "EOF",
                          "disconnect": terminal, "refused_puback_queued": False, "first_commit": commit,
                          "first_wire_puback": "40020001", "first_puback_queued": ack,
                          "durable_obligations": mid - 1, "topic": topic}


def normalized_arguments(exchange_record):
    def normalized(argv):
        copy = list(argv)
        for flag in ("-h", "-p"):
            copy[copy.index(flag) + 1] = "<endpoint>"
        return copy

    return [normalized(exchange_record[role]["argv"]) for role in ("publisher", "subscriber")]


def exercise(out, pki, reference, record):
    clients = Mosquitto(out, pki, reference)
    reference_hash = sha(reference.parent / f"mosquitto-{REFERENCE}.tar.gz")
    require(reference_hash == REFERENCE_SHA256, "reference_source_archive_digest_mismatch")
    record["reference"] = {"version": REFERENCE, "binary_sha256": sha(reference / "src/mosquitto"),
                           "source_archive_sha256": reference_hash, "exchanges": [],
                           "clients": {role: sha(reference / f"client/mosquitto_{role}") for role in ("pub", "sub")}}
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
        asyncio.run(interop(record, port, stream, clients, pki))
        wire_exchange(record, pki, port)
        durable_before(record, pki, port, stream, clients, db)
    with launch_broker(out, pki, "start-2", data, record) as (port, _):
        durable_after(record, pki, port, db)
        asyncio.run(inherited_retain(record, pki, port, db))
    with launch_broker(out, pki, "capacity", out / "capacity-data", record) as (port, stream):
        capacity(record, pki, port, stream, out / "capacity-data/broker.sqlite")


def main():
    os.umask(0o077)
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("out", type=Path)
    cli.add_argument("reference", type=Path, help="Mosquitto 2.0.21 source and binaries from build-mosquitto-reference.sh")
    cli.add_argument("archive_sha256", help="digest recorded by the external archive verifier")
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
            exercise(out, pki, reference, record)
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
