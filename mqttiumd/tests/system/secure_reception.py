#!/usr/bin/env python3
"""Receive TLS and MQTT authority at their public socket and durable boundaries."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import queue
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time

from external_mqttium import client_identity
from message_expiry_probe import MessageExpiryWire, publish_packet
from pubsub_probe import subscribe_packet, unsubscribe_packet
from tcp_probe import Observations, ReceiptFailure, packet, require, utf8, variable_integer, write_json
from tcp_reception import Broker

ROOT = Path(__file__).resolve().parents[2]
pki_spec = importlib.util.spec_from_file_location("demo_pki", ROOT / "preview/demo-pki.py")
pki_module = importlib.util.module_from_spec(pki_spec)
pki_spec.loader.exec_module(pki_module)
STATUS = "atelier/capteur-1/status"
OTHER = "atelier/capteur-10/status"
INFINITE = 0xffffffff


def client_context(pki: Path, name: str | None = "sensor", *, version=ssl.TLSVersion.TLSv1_3):
    context = ssl.create_default_context(cafile=str(pki / "ca.crt"))
    context.minimum_version = context.maximum_version = version
    if name is not None:
        context.load_cert_chain(pki / f"{name}.crt", pki / f"{name}.key")
    return context


def tls_args(pki: Path, *, identities=None):
    return ("--tls-cert", str(pki / "server.crt"), "--tls-key", str(pki / "server.key"),
            "--client-ca", str(pki / "ca.crt"), "--identities", str(identities or pki / "identities.json"))


class Peer(MessageExpiryWire):
    def __init__(self, broker, name, context, *, server_name="127.0.0.1", log=None):
        self.begin = time.monotonic_ns()
        super().__init__(broker.address, log or broker.observations, name)
        self.tls = context is not None
        if context is not None:
            try:
                self.socket = context.wrap_socket(self.socket, server_hostname=server_name)
                require(self.socket.version() == "TLSv1.3" and not self.socket.session_reused,
                        "secure_handshake_profile")
            except BaseException:
                self.close()
                raise
        self.handshake_complete = time.monotonic_ns()
        self.observations.record("transport_established", label=name, tls=self.tls,
                                 version=self.socket.version() if self.tls else None,
                                 cipher=self.socket.cipher()[0] if self.tls else None)

    def poll(self, timeout):
        # SSLSocket can already hold decrypted records even when select sees
        # no more TCP bytes. Use its bounded read as the observation boundary.
        deadline = time.monotonic() + timeout
        while True:
            if self.frames:
                return self.frames.pop(0)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            self.socket.settimeout(remaining)
            try:
                data = self.socket.recv(65536)
            except TimeoutError:
                return None
            except OSError as error:
                raise ReceiptFailure("secure_wire_closed", type(error).__name__) from error
            finally:
                self.socket.settimeout(3)
            if not data:
                self.decoder.finish()
                raise ReceiptFailure("secure_wire_closed")
            self.observations.record_bytes("client_rx", data, label=self.label)
            self.frames.extend(self.decoder.feed(data))

    def open(self, client_id, *, qos_window=8, expiry=INFINITE, will=None, clean=False, keep_alive=30, reason=0):
        flags = int(clean) * 2 | (0x0c if will else 0)
        props = b"\x11" + expiry.to_bytes(4, "big") + b"\x21" + qos_window.to_bytes(2, "big")
        body = b"\x00\x04MQTT\x05" + bytes([flags]) + keep_alive.to_bytes(2, "big")
        body += variable_integer(len(props)) + props + utf8(client_id)
        if will:
            topic, payload = will
            body += b"\x00" + utf8(topic) + len(payload).to_bytes(2, "big") + payload
        self.send(packet(0x10, body))
        ack = self.expect(0x20)
        require(len(ack.body) >= 3, "secure_connack_shape")
        require(ack.body[1] == reason, "foreign_client_id_accepted" if reason else "authorized_client_refused",
                str(ack.body[1]))
        require(not reason or ack.body[0] == 0, "authorization_refusal_leaked_session_present")
        return bool(ack.body[0])

    def deny(self, frame, code):
        self.send(frame)
        answer = self.poll(3)
        require(answer is not None, "authorization_reply_missing")
        require(answer.kind == 0xe0 and answer.body[:1] == b"\x87", code, repr(answer))

    def ping(self):
        self.send(b"\xc0\x00")
        self.wait(0xd0)

    def publish(self, topic, data, mid=1, *, qos=1, expiry=None, retain=False):
        self.send(publish_packet(topic, data, mid if qos else None, qos=qos, expiry=expiry, retain=retain))
        if qos:
            self.expect(0x40 if qos == 1 else 0x50, mid)
        if qos == 2:
            self.send(packet(0x62, mid.to_bytes(2, "big")))
            self.expect(0x70, mid)

    def message(self, payload, *, ack=True):
        message = self.next_publication()
        require(message.payload == payload, "secure_wrong_payload")
        if ack and message.packet_id:
            self.acknowledge(message)
        return message


def broker_with(binary, out, pki, *, db=None, label="broker", pauses=(), policy=None, hooks=False):
    child = Broker(binary, out, db=db, label=label, pauses=pauses, test_control=hooks,
                   extra_args=tls_args(pki, identities=policy) if pki else ())
    try:
        ready = child.ready()["details"]
        require(ready["profile"] == ("mtls-will-pubsub-v1" if pki else "tcp-will-pubsub-v1"),
                "requested_security_profile_not_ready")
        require(ready["sqlite_schema"] == (11 if pki else 10), "wrong_security_schema")
        return child
    except BaseException:
        child.cleanup()
        raise


def state(broker):
    with sqlite3.connect(broker.db.as_uri() + "?mode=ro", uri=True) as sql:
        counts = {table: sql.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("sessions", "routes", "wills", "retained", "obligations", "outbound", "payloads", "operations")}
        if sql.execute("PRAGMA user_version").fetchone()[0] == 11:
            counts["owners"] = sql.execute(
                "SELECT CAST(s.client_id AS TEXT), hex(s.id), o.principal FROM sessions s "
                "JOIN security_session_owners o ON o.session=s.id ORDER BY s.client_id"
            ).fetchall()
        return counts


def observed(broker, probe, code, seconds=5):
    stop = time.monotonic() + seconds
    while time.monotonic() < stop:
        value = probe()
        if value:
            return value
        require(broker.process.poll() is None, code, "broker exited")
        time.sleep(.005)
    raise ReceiptFailure(code)


def latest(broker):
    return int(broker.events()[-1]["seq"])


def closed_with(broker, after, code):
    return observed(broker, lambda: next((event for event in broker.events()
        if event["kind"] == "connection_closed" and int(event["seq"]) > after
        and event["details"]["code"] == code), None), code)


def add_test_certificates(root):
    policy = pki_module.generate(root)
    pki_module.ca(root, "foreign")
    for name, options in (
        ("unlisted", {}), ("untrusted", {"issuer": "foreign"}),
        ("expired", {}), ("rotated", {}), ("server-only", {"usage": "serverAuth"}),
    ):
        usage = options.pop("usage", "clientAuth")
        pin = pki_module.issue(root, name, usage, **options)
        if name == "expired":
            pin = expire_certificate(root)
        if name not in ("unlisted", "rotated"):
            policy["principals"][0]["certificates"].append(pin)
    policy["principals"][0]["client_ids"].extend(f"parallel-{n}" for n in range(16))
    (root / "identities.json").write_text(json.dumps(policy, indent=2) + "\n")
    return policy


def expire_certificate(root):
    # Explicit past bounds work on both the CI OpenSSL and newer versions.
    config = root / "expired-ca.cnf"
    config.write_text(f"""[ca]
default_ca=test
[test]
database={root}/expired-index
serial={root}/expired-serial
new_certs_dir={root}
certificate={root}/ca.crt
private_key={root}/ca.key
default_md=sha256
policy=names
x509_extensions=client
[names]
commonName=supplied
[client]
basicConstraints=critical,CA:FALSE
extendedKeyUsage=clientAuth
""")
    (root / "expired-index").touch()
    (root / "expired-serial").write_text("01\n")
    csr, cert = root / "expired.csr", root / "expired.crt"
    pki_module.openssl("req", "-new", "-key", root / "expired.key", "-subj", "/CN=expired", "-out", csr)
    pki_module.openssl("ca", "-batch", "-config", config, "-startdate", "20000101000000Z",
                       "-enddate", "20000102000000Z", "-in", csr, "-notext", "-out", cert)
    return hashlib.sha256(pki_module.openssl("x509", "-in", cert, "-outform", "DER")).hexdigest()


def invalid_certificates(binary, out, pki):
    broker = broker_with(binary, out, pki)
    refusals = []
    try:
        for identity, cause, version in [
            (None, "tls_client_certificate_missing", ssl.TLSVersion.TLSv1_3),
            ("untrusted", "tls_certificate_invalid", ssl.TLSVersion.TLSv1_3),
            ("expired", "tls_certificate_time", ssl.TLSVersion.TLSv1_3),
            ("unlisted", "tls_identity_not_allowed", ssl.TLSVersion.TLSv1_3),
            ("server-only", "tls_certificate_invalid", ssl.TLSVersion.TLSv1_3),
            ("sensor", "tls_protocol_incompatible", ssl.TLSVersion.TLSv1_2),
        ]:
            after = latest(broker)
            failed = False
            try:
                with Peer(broker, "certificate-refusal", client_context(pki, identity, version=version)) as wire:
                    wire.open("capteur-1", will=(STATUS, b"must-not-install"))
            except (ssl.SSLError, ReceiptFailure, ConnectionError):
                failed = True
            require(failed, "invalid_client_certificate_established", cause)
            refusal = closed_with(broker, after, cause)
            require(not any(event["kind"] == "tls_authenticated" and int(event["seq"]) > after
                            for event in broker.events()), "unverified_certificate_authenticated", cause)
            require(state(broker)["sessions"] == state(broker)["wills"] == 0,
                    "unauthenticated_tls_created_durable_state", cause)
            refusals.append({"identity": identity, "cause": cause, "event": refusal})
        try:
            with Peer(broker, "wrong-server-name", client_context(pki), server_name="wrong.invalid"):
                raise ReceiptFailure("server_name_not_verified")
        except ssl.SSLCertVerificationError as error:
            require(error.verify_code in (62, 64), "wrong_server_name_failure_cause", str(error.verify_code))
            refusals.append({"client_verification": "server_name_mismatch", "verify_code": error.verify_code})
        with Peer(broker, "valid-client", client_context(pki)) as wire:
            require(not wire.open("capteur-1", expiry=0), "unexpected_existing_session")
            wire.ping()
        return {"refusals": refusals}
    finally:
        broker.cleanup()


def policy_refusal(binary, out, pki, db, policy, *, code):
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    out.mkdir()
    events = out / "refused.jsonl"
    command = [str(binary), "--listen", "127.0.0.1:0", "--db", str(db), "--events", str(events)]
    if pki:
        command.extend(tls_args(pki, identities=policy))
    refused = subprocess.run(command, capture_output=True, timeout=8, check=False)
    require(refused.returncode == 2 and code.encode() in refused.stderr, "wrong_policy_startup_refusal")
    require("ready" not in {json.loads(line)["kind"] for line in events.read_text().splitlines()},
            "policy_mismatch_reached_ready")
    require(before == hashlib.sha256(db.read_bytes()).hexdigest(), "policy_mismatch_changed_database")
    return {"code": code, "exit": refused.returncode, "database_sha256": before, "command": command}


def authority(binary, out, pki, original_policy):
    broker = broker_with(binary, out, pki)
    restarted = None
    try:
        sensor_context, dashboard_context = client_context(pki), client_context(pki, "dashboard")
        with Peer(broker, "dashboard", dashboard_context) as dashboard, Peer(broker, "sensor", sensor_context) as sensor:
            require(not dashboard.open("dashboard"), "unexpected_dashboard_resume")
            dashboard.subscribe(1, [("atelier/+/status", 1)])
            require(not sensor.open("capteur-1", will=(STATUS, b"offline")), "unexpected_sensor_resume")
            initial = state(broker)
            for clean in (False, True):
                with Peer(broker, "other-principal", dashboard_context) as other:
                    other.open("capteur-1", clean=clean, will=(STATUS, b"forged"), reason=0x87)
                require(state(broker)["owners"] == initial["owners"] and state(broker)["wills"] == 1,
                        "foreign_client_id_changed_durable_attachment")
                sensor.ping()
            sensor.publish(STATUS, b"online", retain=True)
            dashboard.message(b"online")
            sensor.send(b"\xe0\x00")
            observed(broker, lambda: state(broker)["wills"] == 0, "normal_disconnect_left_will")
            dashboard.send(b"\xe0\x00")
        with Peer(broker, "offline-source", sensor_context) as sensor:
            require(sensor.open("capteur-1", will=(STATUS, b"offline")), "legitimate_resume_refused")
            sensor.publish(STATUS, b"unread")
            sensor.close()
        before = observed(broker, lambda: (lambda row: row if row["obligations"] == 2
            and row["wills"] == 0 else None)(state(broker)), "offline_will_obligations_missing")

        # Negative topic operations are a full packet refusal before any route
        # or admission, including packet IDs/QoS that otherwise commit.
        for qos in (0, 1, 2):
            with Peer(broker, f"forbidden-publish-{qos}", sensor_context) as peer:
                require(peer.open("capteur-1"), "unexpected_topic_test_session_loss")
                peer.deny(publish_packet(OTHER, b"forbidden", 9 if qos else None, qos=qos, retain=True),
                          "forbidden_publication_not_refused")
        with Peer(broker, "forbidden-subscribe", dashboard_context) as peer:
            require(peer.open("dashboard-check") is False, "unexpected_dashboard_check_resume")
            peer.deny(subscribe_packet(1, [("atelier/+/status", 1), ("#", 1)]), "overbroad_subscription_accepted")
        with Peer(broker, "forbidden-unsubscribe", sensor_context) as peer:
            peer.open("capteur-1")
            peer.deny(unsubscribe_packet(1, ["atelier/#"]), "overbroad_unsubscribe_accepted")
        with Peer(broker, "forbidden-will", sensor_context) as peer:
            peer.open("capteur-1", clean=True, will=(OTHER, b"forbidden"), reason=0x87)
        after = state(broker)
        for field in ("routes", "wills", "retained", "payloads"):
            require(after[field] == before[field], "denied_packet_mutated_business_state", field)

        require(broker.stop(killed=True) == -9, "secure_broker_sigkill_missing")
        rotated_policy = json.loads(json.dumps(original_policy))
        new_pin = hashlib.sha256(pki_module.openssl("x509", "-in", pki / "rotated.crt", "-outform", "DER")).hexdigest()
        rotated_policy["principals"][0]["certificates"].append(new_pin)
        rotated_file = pki / "rotated.json"
        rotated_file.write_text(json.dumps(rotated_policy))
        restarted = broker_with(binary, out, pki, db=broker.db, label="recovered", policy=rotated_file)
        with Peer(restarted, "resumed-rotated-cert", client_context(pki, "rotated")) as sensor:
            require(sensor.open("capteur-1"), "rotated_leaf_failed_legitimate_resume")
            for clean in (False, True):
                with Peer(restarted, "foreign-restart", dashboard_context) as other:
                    other.open("capteur-1", clean=clean, reason=0x87)
            sensor.ping()
            sensor.send(b"\xe0\x00")
        with Peer(restarted, "dashboard-recovered", dashboard_context) as dashboard:
            require(dashboard.open("dashboard"), "secure_dashboard_session_lost")
            dashboard.message(b"unread")
            will = dashboard.message(b"offline")
            require(will.qos == 1 and not will.retain, "secure_will_wrong_delivery")
            dashboard.send(b"\xe0\x00")
        final = observed(restarted, lambda: (lambda row: row if row["obligations"] == row["outbound"] == 0
            else None)(state(restarted)), "secure_claims_not_drained")
        require(final["routes"] == before["routes"], "secure_route_not_durable")
        require(restarted.stop() == 0, "secure_restart_shutdown")
        changed = json.loads(json.dumps(rotated_policy))
        changed["principals"][1]["subscribe"] = ["atelier/capteur-1"]
        file = pki / "changed-rights.json"
        file.write_text(json.dumps(changed))
        refused = policy_refusal(binary, out / "change-authorization", pki, broker.db, file,
                                 code="security_policy_mismatch")
        downgrade = policy_refusal(binary, out / "cleartext-downgrade", None, broker.db, None, code="InvalidData")
        return {"before": before, "after": final, "pid": broker.process.pid, "signal": -9,
                "same_file": restarted.db == broker.db, "policy_change": refused, "downgrade": downgrade,
                "rotation": "new leaf / same principal / same session"}
    finally:
        if restarted:
            restarted.cleanup()
        broker.cleanup()


def tls_close_notify(binary, out, pki):
    broker = broker_with(binary, out, pki)
    try:
        with Peer(broker, "close-notify-target", client_context(pki, "dashboard")) as dashboard, Peer(
            broker, "close-notify-source", client_context(pki)
        ) as sensor:
            dashboard.open("dashboard")
            dashboard.subscribe(1, [("atelier/#", 1)])
            sensor.open("capteur-1", will=(STATUS, b"tls-close-notify"), expiry=0)
            before = latest(broker)
            try:
                raw = sensor.socket.unwrap()
                raw.close()
            except (ssl.SSLError, ConnectionError):
                # The peer's close_notify is sent by unwrap. The event below,
                # rather than a local unwrap result, establishes its receipt.
                pass
            closed = closed_with(broker, before, "tls_close_notify")
            message = dashboard.message(b"tls-close-notify")
            require(message.qos == 1, "tls_close_notify_lost_will")
            return {"closed": closed, "will_qos": message.qos}
    finally:
        broker.cleanup()


def capacity_and_flow(binary, out, pki):
    broker = broker_with(binary, out, pki)
    try:
        sensor_context = client_context(pki)
        dashboard_context = client_context(pki, "dashboard")
        with Peer(broker, "flow-subscriber", dashboard_context) as dashboard, Peer(broker, "flow-sensor", sensor_context) as sensor:
            dashboard.open("dashboard", qos_window=1)
            dashboard.subscribe(1, [("atelier/#", 1)])
            sensor.open("capteur-1")
            for n in range(8):
                sensor.publish(STATUS, bytes([n]), mid=n+1, qos=2 if n == 7 else 1)
            first = dashboard.message(b"\x00", ack=False)
            dashboard.send(b"\xc0\x00")
            dashboard.expect(0xd0)
            require(dashboard.poll(.1) is None, "tls_receive_maximum_exceeded")
            dashboard.acknowledge(first)
            for n in range(1, 8):
                dashboard.message(bytes([n]))
            plaintexts = []
            try:
                for _ in range(8):
                    plaintexts.append(socket.create_connection(broker.address, timeout=2))
                observed(broker, lambda: sum(e["kind"] == "connection_opened" for e in broker.events()) >= 10,
                         "tls_handshake_slots_not_occupied")
                with socket.create_connection(broker.address, timeout=2):
                    broker.wait_event("connection_refused")
                require(any(e["kind"] == "connection_refused" and e["details"]["code"] == "tls_handshake_limit"
                            for e in broker.events()), "tls_handshake_limit_missing")
                dashboard.ping()
                sensor.ping()
                deadline = observed(broker, lambda: [e for e in broker.events() if e["kind"] == "connection_closed"
                    and e["details"]["code"] == "tls_handshake_deadline"]
                    if sum(e["kind"] == "connection_closed" and e["details"]["code"] == "tls_handshake_deadline"
                           for e in broker.events()) == 8 else None, "slow_tls_handshake_not_reaped", seconds=4)
            finally:
                for plain in plaintexts:
                    plain.close()
            with ExitStack() as stack:
                for n in range(14):
                    peer = stack.enter_context(Peer(broker, f"capacity-{n}", sensor_context))
                    peer.open(f"parallel-{n}", expiry=0)
                after = latest(broker)
                with socket.create_connection(broker.address, timeout=2):
                    full = broker.wait_event("connection_refused", after=after)
                require(full["details"]["code"] == "connection_limit", "tls_connection_limit_missing")
                dashboard.ping()
                sensor.ping()
            return {"window": 1, "deliveries": 8, "independent_ping": True,
                    "reaped_handshakes": len(deadline), "full_connections": 16, "extra_connection": full}
    finally:
        broker.cleanup()


def public_clients(binary, out, pki):
    from mqttium import MQTTProtocolVersion
    from mqttium.api import AsyncClient, Properties
    import paho.mqtt.client as paho
    from importlib.metadata import version

    broker = broker_with(binary, out, pki)
    topics = queue.Queue(maxsize=4)
    connected, subscribed = threading.Event(), threading.Event()
    reasons = []
    client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id="dashboard",
                         protocol=paho.MQTTv5, reconnect_on_failure=False)
    client.tls_set_context(client_context(pki, "dashboard"))

    def on_connect(peer, _data, _flags, reason, _props):
        reasons.append(reason.value)
        if not reason.is_failure:
            peer.subscribe("atelier/+/status", qos=1)
        connected.set()

    def on_subscribe(_peer, _data, _mid, result, _props):
        reasons.extend(reason.value for reason in result)
        subscribed.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = lambda _c, _d, m: topics.put_nowait((m.topic, m.payload, m.qos, m.retain))

    async def publish():
        sensor = AsyncClient("capteur-1", protocol=MQTTProtocolVersion.MQTTv5,
                             clean_start=False, maximum_packet_size=65536,
                             connect_properties=Properties({"session_expiry_interval": INFINITE}))
        try:
            connack = await sensor.connect(*broker.address, ssl=client_context(pki), timeout=4)
            require(not connack.session_present, "public_sensor_unexpected_resume")
            result = await sensor.publish(STATUS, b"public-mqttium", qos=1, timeout=4)
            await result.wait(timeout=4)
            require(result.is_done() and result.exception() is None, "public_tls_publish_failed")
        finally:
            await sensor.disconnect()

    try:
        client.connect(*broker.address, clean_start=False)
        client.loop_start()
        require(connected.wait(5) and subscribed.wait(5) and reasons == [0, 1],
                 "public_tls_dashboard_not_subscribed", str(reasons))
        asyncio.run(publish())
        received = topics.get(timeout=5)
        require(received == (STATUS, b"public-mqttium", 1, False), "public_tls_message_mismatch")
        return {"mqttium": client_identity(), "paho": version("paho-mqtt"), "connack_suback": reasons,
                "topic": received[0], "message": received[1].decode(), "qos": received[2], "retain": received[3]}
    finally:
        client.disconnect()
        client.loop_stop()
        broker.cleanup()


def deadlines(binary, out, pki, *, only=None):
    results = {}
    for scenario in ("unstarted", "fragmented"):
        if only and scenario != only:
            continue
        case = out / scenario
        broker = broker_with(binary, case, pki, pauses=("tls.frame.sealed", "tls.frame.first_bytes"))
        try:
            with Peer(broker, "expiry-dashboard", client_context(pki, "dashboard")) as dashboard, Peer(
                broker, "expiry-source", client_context(pki)
            ) as sensor:
                dashboard.open("dashboard")
                dashboard.subscribe(1, [("atelier/#", 1)])
                sensor.open("capteur-1", expiry=0)
                before = latest(broker)
                if scenario == "unstarted":
                    (broker.control / "hold-next-tls-frame").touch(exist_ok=False)
                    sensor.publish(STATUS, b"must-expire-before-wire", expiry=1)
                    barrier = broker.wait_event("barrier_hit", point="tls.frame.sealed", after=before)
                    require(barrier["details"]["ciphertext_bytes_sent"] == 0, "tls_seal_barrier_after_write")
                    require(dashboard.poll(.12) is None, "sealed_tls_frame_reached_subscriber")
                    # The actual writer's stamp defines the threshold. Linux
                    # CLOCK_BOOTTIME is also visible to this independent peer.
                    received = broker.wait_event("message_expiry_received", after=before)
                    due = int(received["details"]["tick"]) + 1_000_000_000
                    while time.clock_gettime_ns(time.CLOCK_BOOTTIME) < due:
                        require(not any(e["kind"] == "message_transmission_started"
                            and e.get("details", {}).get("effect") == barrier["details"]["effect"]
                            for e in broker.events()), "tls_plaintext_declared_started")
                        time.sleep(.01)
                    broker.release("tls.frame.sealed")
                    refused = broker.wait_event("message_transmission_not_started", after=before)
                    require(refused["details"]["effect"] == barrier["details"]["effect"]
                            and refused["details"]["reason"] == "Expired", "tls_expiry_wrong_zero_progress_fact")
                    observed(broker, lambda: state(broker)["obligations"] == 0,
                             "unstarted_tls_claim_not_reclaimed")
                    try:
                        require(dashboard.poll(.25) is None, "expired_tls_packet_observed")
                    except ReceiptFailure as failure:
                        require(failure.code == "secure_wire_closed", failure.code)
                    results[scenario] = {"sealed": barrier, "not_started": refused, "resources": state(broker)}
                else:
                    (broker.control / "fragment-next-tls-publish").touch(exist_ok=False)
                    (broker.control / "hold-next-outgoing-written").touch(exist_ok=False)
                    payload = b"F" * 24_576
                    sensor.publish(STATUS, payload, expiry=1)
                    barrier = broker.wait_event("barrier_hit", point="tls.frame.first_bytes", after=before)
                    require(barrier["details"]["ciphertext_bytes_sent"] == 3,
                            "tls_fragmentation_control_not_reached")
                    started = broker.wait_event("message_transmission_started", after=before)
                    require(started["details"]["effect"] == barrier["details"]["effect"],
                            "wrong_tls_started_effect")
                    # Only a prefix of the first TLS record is in the socket.
                    # The subsequent fragment has yet to be offered to TLS.
                    require(dashboard.poll(.025) is None, "partial_tls_record_yielded_publication")
                    broker.release("tls.frame.first_bytes")
                    message = dashboard.message(payload, ack=False)
                    require(message.message_expiry_interval in (0, 1), "bad_tls_expiry_property")
                    held = broker.wait_event("outgoing_written_held", after=before)
                    now = broker.wait_event("message_expiry_received", after=before)
                    end = int(now["details"]["tick"]) + 1_000_000_000
                    while time.clock_gettime_ns(time.CLOCK_BOOTTIME) < end:
                        time.sleep(.01)
                    require(state(broker)["outbound"] == 1, "started_tls_claim_freed_before_puback")
                    dashboard.acknowledge(message)
                    observed(broker, lambda: state(broker)["outbound"] == 0,
                             "tls_ack_before_callback_not_finished")
                    results[scenario] = {"first_ciphertext": barrier, "started": started,
                         "local_completion_held": held, "ack_after_expiry": True,
                         "payload_bytes": len(payload), "resources": state(broker)}
        finally:
            broker.cleanup()
    return results


def resources(pid):
    status = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines())
    stat = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()
    return {"rss_kib": int(status["VmRSS"].split()[0]), "threads": int(status["Threads"]),
            "fd_count": len(list(Path(f"/proc/{pid}/fd").iterdir())),
            "cpu_ticks": int(stat[11]) + int(stat[12])}


def cpu_quota(root=Path("/sys/fs/cgroup")):
    try:
        return {"source": "cgroup_v2", "limit": (root / "cpu.max").read_text().strip()}
    except (OSError, UnicodeError):
        pass
    for controller in ("cpu", "cpu,cpuacct", "cpuacct,cpu"):
        directory = root / controller
        try:
            quota_us = int((directory / "cpu.cfs_quota_us").read_text())
            period_us = int((directory / "cpu.cfs_period_us").read_text())
        except (OSError, ValueError):
            continue
        return {"source": "cgroup_v1", "quota_us": quota_us, "period_us": period_us}
    return {"source": "unavailable"}


def distribution(series):
    ordered = sorted(series)
    return {"count": len(series), "min_ms": ordered[0], "p50_ms": ordered[(len(ordered) + 1)//2 - 1],
            "p95_ms": ordered[(95*len(ordered) + 99)//100 - 1], "max_ms": ordered[-1],
            "over_1000ms": [i for i, value in enumerate(series) if value >= 1000]}


def measure(binary, out, pki):
    broker = broker_with(binary, out, pki)
    sensor_context = client_context(pki) if pki else None
    dashboard_context = client_context(pki, "dashboard") if pki else None
    samples = []
    initial = resources(broker.process.pid)
    keepalive = {}
    try:
        with Peer(broker, "latency-dashboard", dashboard_context) as dashboard:
            dashboard.open("dashboard")
            dashboard.subscribe(1, [("atelier/+/status", 1)])
            for n in range(100):
                will_payload = f"offline:{n}".encode()
                live_payload = f"online:{n}".encode()
                with Peer(broker, f"latency-source-{n}", sensor_context) as sensor:
                    sensor.open("capteur-1", expiry=0, will=(STATUS, will_payload))
                    connack = time.monotonic_ns()
                    start = time.monotonic_ns()
                    sensor.publish(STATUS, live_payload)
                    acknowledged = time.monotonic_ns()
                    dashboard.message(live_payload)
                    sample = {"seq": n, "tcp_tls_connack_ms": (connack - sensor.begin)/1e6,
                              "handshake_to_connack_ms": (connack - sensor.handshake_complete)/1e6,
                              "qos1_ms": (acknowledged - start)/1e6}
                notification = dashboard.message(will_payload, ack=False)
                sample["notification_ns"] = time.monotonic_ns()
                dashboard.acknowledge(notification)
                if n % 10 == 0:
                    sample["resources"] = resources(broker.process.pid)
                samples.append(sample)
            parsed = broker.events()
            physical = [row for row in parsed if row["kind"] == "connection_closed"
                        and row["details"]["code"] in ("tls_eof", "eof")]
            commits = [row for row in parsed if row["kind"] == "will_commit_observed"]
            require(len(physical) == len(commits) == 100, "latency_missing_causal_observations",
                    str((len(physical), len(commits))))
            emitted = [row for row in parsed if row["kind"] == "will_admitted"]
            for sample, close, commit, admission in zip(samples, physical, commits, emitted, strict=True):
                require(commit["operation"] == admission["operation"], "latency_wrong_will_operation")
                ended = int(close["details"]["observed_monotonic_ns"])
                engaged = int(commit["details"]["monotonic_ns"])
                require(ended <= engaged <= sample["notification_ns"], "latency_monotonic_clock_not_shared")
                sample.update(close_generation=close["connection"], operation=commit["operation"],
                              end_to_commit_ms=(engaged - ended)/1e6,
                              commit_to_notification_ms=(sample["notification_ns"] - engaged)/1e6)

            start = time.monotonic_ns()
            with Peer(broker, "latency-keepalive", sensor_context) as sensor:
                sensor.open("capteur-1", expiry=0, keep_alive=1, will=(STATUS, b"keepalive"))
                keepalive_start = time.monotonic_ns()
                dashboard.message(b"keepalive")
            event = closed_with(broker, int(physical[-1]["seq"]), "keep_alive")
            keepalive = {"negotiated_seconds": 1, "detection_ms":
                         (int(event["details"]["observed_monotonic_ns"]) - keepalive_start)/1e6}
            require(time.monotonic_ns() - start >= 1_000_000_000, "keepalive_measured_without_deadline")

        def worker(worker_id):
            rows = []
            trace = Observations(out / f"parallel-{worker_id}.jsonl")
            for n in range(6):
                with Peer(broker, f"parallel-{worker_id}-{n}", sensor_context, log=trace) as peer:
                    peer.open(f"parallel-{worker_id}", expiry=0)
                    ack = time.monotonic_ns()
                    start = time.monotonic_ns()
                    peer.publish("atelier/capteur-1/parallel", b"parallel")
                    complete = time.monotonic_ns()
                    peer.ping()
                    peer.send(b"\xe0\x00")
                    rows.append({"worker": worker_id, "seq": n, "tcp_tls_connack_ms": (ack - peer.begin)/1e6,
                                 "qos1_ms": (complete - start)/1e6, "resources": resources(broker.process.pid)})
            return rows

        with ThreadPoolExecutor(max_workers=4) as pool:
            parallel = [row for rows in pool.map(worker, range(4)) for row in rows]
        end_resources = resources(broker.process.pid)
        final = observed(broker, lambda: (lambda result: result if result["obligations"] == result["wills"] == 0 else None)
                         (state(broker)), "measurement_work_not_drained")
        distributions = {name: distribution([row[name] for row in samples]) for name in
                         ("tcp_tls_connack_ms", "handshake_to_connack_ms", "qos1_ms", "end_to_commit_ms", "commit_to_notification_ms")}
        return {"transport": "TLS1.3-mTLS" if pki else "loopback-cleartext",
                "samples": samples, "distribution": distributions, "concurrent_connections": 4,
                "parallel_samples": parallel, "parallel_distribution": {
                    name: distribution([row[name] for row in parallel]) for name in ("tcp_tls_connack_ms", "qos1_ms")},
                "keep_alive": keepalive, "initial": initial, "final_process": end_resources, "final_store": final,
                "cpu_ticks_per_sec": os.sysconf("SC_CLK_TCK")}
    finally:
        broker.cleanup()


def startup_refusals(binary, out, pki):
    out.mkdir()
    cases = []
    for label, addr, extra, code in [
        ("cleartext-any", "0.0.0.0:0", (), "cleartext_requires_loopback"),
        ("partial-tls", "127.0.0.1:0", tls_args(pki)[:2], "TLS requires"),
        ("bad-key", "127.0.0.1:0", tuple(str(pki / "dashboard.key") if item == str(pki / "server.key") else item
                                       for item in tls_args(pki)), "server_certificate_key_mismatch"),
    ]:
        db, events = out / (label + ".sqlite"), out / (label + ".jsonl")
        command = [str(binary), "--listen", addr, "--db", str(db), "--events", str(events), *extra]
        result = subprocess.run(command, capture_output=True, timeout=8)
        require(result.returncode == 2 and code.encode() in result.stderr and not db.exists(),
                "bad_security_configuration_reached_writer", label)
        cases.append({"label": label, "command": command, "exit": result.returncode, "reason": code})
    insecure = broker_with(binary, out / "legacy-schema", None)
    try:
        require(insecure.stop() == 0, "legacy_shutdown")
        cases.append(policy_refusal(binary, out / "no-implicit-upgrade", pki, insecure.db, None, code="InvalidData"))
    finally:
        insecure.cleanup()
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--mode", choices=("normal", "hooks", "measurement", "chain", "client-id", "wildcard", "seal"), default="normal")
    args = parser.parse_args()
    binary, out = args.binary.resolve(), args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    receipt = {"status": "FAIL", "mode": args.mode, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
               "execution_platform": platform.platform(), "python": platform.python_version(),
               "openssl": ssl.OPENSSL_VERSION, "groups": {}}
    try:
        # Credentials live in a private temporary tree outside evidence.
        old_mask = os.umask(0o077)
        try:
            with tempfile.TemporaryDirectory(prefix="mqttiumd-private-demo-") as temp:
                pki = Path(temp) / "pki"
                policy = add_test_certificates(pki)
                mode = args.mode
                if mode == "normal":
                    cases = [("startup", startup_refusals, ()),
                             ("certificates", invalid_certificates, ()),
                             ("clients", public_clients, ()),
                             ("authority", authority, (policy,)),
                             ("tls-close-notify", tls_close_notify, ()),
                             ("resources", capacity_and_flow, ())]
                    for label, operation, rest in cases:
                        receipt["groups"][label] = operation(binary, out / label, pki, *rest)
                        print(label, "PASS", flush=True)
                elif mode in ("hooks", "seal"):
                    receipt["groups"]["expiry"] = deadlines(binary, out / "expiry", pki,
                                                             only="unstarted" if mode == "seal" else None)
                elif mode == "measurement":
                    receipt["conditions"] = {"cpu_count": os.cpu_count(), "load_before": os.getloadavg(),
                        "cpu_quota": cpu_quota(),
                        "workload": "100 sequential + 24 connections across four independent workers per mode"}
                    for label, mode_pki in (("tls", pki), ("cleartext", None)):
                        receipt["groups"][label] = measure(binary, out / label, mode_pki)
                elif mode == "client-id":
                    broker = broker_with(binary, out / "client-id", pki)
                    try:
                        with Peer(broker, "legitimate", client_context(pki)) as owner:
                            owner.open("capteur-1")
                            with Peer(broker, "foreign", client_context(pki, "dashboard")) as foreign:
                                foreign.open("capteur-1", clean=True, reason=0x87)
                            owner.ping()
                    finally:
                        broker.cleanup()
                elif mode == "chain":
                    receipt["groups"]["certificates"] = invalid_certificates(binary, out / "chain", pki)
                elif mode == "wildcard":
                    broker = broker_with(binary, out / "wildcard", pki)
                    try:
                        with Peer(broker, "dashboard", client_context(pki, "dashboard")) as dashboard:
                            dashboard.open("dashboard")
                            dashboard.deny(subscribe_packet(1, [("#", 1)]), "overbroad_subscription_accepted")
                    finally:
                        broker.cleanup()
        finally:
            os.umask(old_mask)
        receipt["status"] = "PASS"
    except Exception as error:
        receipt["failure"] = {"code": getattr(error, "code", type(error).__name__), "detail": str(error)[:768]}
    finally:
        write_json(out / "receipt.json", receipt)
        print(json.dumps({"status": receipt["status"], "failure": receipt.get("failure"), "path": str(out)}))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
