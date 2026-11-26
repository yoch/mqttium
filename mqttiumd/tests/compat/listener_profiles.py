#!/usr/bin/env python3
"""Check the MQTT 5 listener profiles and journal rotation on live executables."""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import sys
import tempfile
import time

import harness
import mqttwire as wire


def expect(observed, **fields):
    for key, value in fields.items():
        if observed.get(key) != value:
            raise AssertionError(f"expected {key}={value!r}, received {observed!r}")
    return observed


class Peer:
    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""
        self.frames = []

    def receive(self):
        while not self.frames:
            chunk = self.sock.recv(16384)
            if not chunk:
                raise EOFError("peer closed before the expected MQTT frame")
            self.frames, self.buffer = wire.split_packets(self.buffer + chunk)
            if len(self.buffer) > 65536:
                raise AssertionError("response exceeds the size of any fixture packet")
        return wire.decode(self.frames.pop(0), level=5)

    def exchange(self, frame, **expected):
        self.sock.sendall(frame)
        return expect(self.receive(), **expected)


@contextmanager
def peer(port, credentials=None, identity=None):
    with socket.create_connection((harness.HOST, port), timeout=5) as tcp:
        tcp.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if credentials is None:
            yield Peer(tcp)
            return
        context = ssl.create_default_context(cafile=credentials / "ca.crt")
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(credentials / f"{identity}.crt", credentials / f"{identity}.key")
        with context.wrap_socket(tcp, server_hostname=harness.HOST) as secured:
            yield Peer(secured)


@contextmanager
def running(work, binary, pki=None, reference=False, password_file=None):
    work.mkdir()
    log = work / "broker.log"
    if reference:
        port = harness.free_port()
        config = work / "reference.conf"
        lines = ["user root"] if os.geteuid() == 0 else []
        lines += [f"listener {port} {harness.HOST}", "log_dest stderr", "allow_anonymous false"]
        if password_file:
            lines.append(f"password_file {password_file}")
        else:
            acl = work / "acl"
            acl.write_text("user sensor\ntopic write atelier/capteur-1/#\nuser dashboard\ntopic read atelier/#\n")
            lines += [f"cafile {pki / 'ca.crt'}", f"certfile {pki / 'server.crt'}", f"keyfile {pki / 'server.key'}",
                      "tls_version tlsv1.3", "require_certificate true", "use_identity_as_username true",
                      f"acl_file {acl}"]
        config.write_text("\n".join(lines) + "\n")
        command = [binary, "-c", config]
        marker = r"mosquitto version \S+ running"
    else:
        command = [binary, "--listen", f"{harness.HOST}:0", "--db", work / "data.db",
                   "--events", work / "events.jsonl", "--event-level", "audit"]
        if DELIVERY_SHARDS:
            command += ["--delivery-shards", str(DELIVERY_SHARDS)]
        if pki:
            command += ["--tls-cert", pki / "server.crt", "--tls-key", pki / "server.key",
                        "--client-ca", pki / "ca.crt", "--identities", pki / "identities.json"]
        else:
            command += ["--storage-mode", "volatile"]
        marker = r"mqttiumd ready \S+:(\d+)"
    with log.open("wb") as out:
        child = subprocess.Popen(command, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    try:
        ready = harness.wait_ready(child, log, marker)
        if not ready:
            raise RuntimeError(f"broker did not start: {harness.tail(log)}")
        yield port if reference else int(ready.group(1))
    finally:
        ended = harness.stop(child)
    if ended != "terminated" or child.returncode:
        raise RuntimeError(f"broker stop {ended}, status {child.returncode}: {harness.tail(log)}")


def connect(client, client_id, *, clean=True, present=False, reason=0):
    client.exchange(wire.connect(client_id, level=5, clean=clean, props={"session_expiry_interval": 300}),
                    type="CONNACK", session_present=present, reason=reason)


def finish(client):
    client.sock.sendall(wire.disconnect(level=5))
    if client.sock.recv(1):
        raise AssertionError("unexpected application bytes following DISCONNECT")


def listener(work, binary, pki, reference):
    exact = "atelier/capteur-1/status"
    subtree = "atelier/capteur-1/measurements"
    with running(work, binary, pki, reference) as port:
        with peer(port, pki, "dashboard") as dashboard:
            connect(dashboard, "dashboard")
            for mid, name in enumerate([exact, subtree + "/#"], start=1):
                dashboard.exchange(wire.subscribe(mid, [(name, 0)], level=5), type="SUBACK", packet_id=mid, codes=[0])
            if not reference:
                dashboard.exchange(wire.subscribe(3, [("other/#", 0)], level=5), type="SUBACK", packet_id=3,
                                   codes=[0x87])
            finish(dashboard)
        if not reference:
            for clean in (False, True):
                with peer(port, pki, "sensor") as sensor:
                    connect(sensor, "dashboard", clean=clean, reason=0x87)
        with peer(port, pki, "dashboard") as dashboard, peer(port, pki, "sensor") as sensor:
            connect(dashboard, "dashboard", clean=False, present=True)
            connect(sensor, "capteur-1")
            for mid, topic in enumerate([exact, subtree + "/temperature"], start=1):
                sensor.exchange(wire.publish(topic, "reading", level=5, qos=1, packet_id=mid),
                                type="PUBACK", packet_id=mid, reason=0)
                expect(dashboard.receive(), type="PUBLISH", topic=topic, payload="reading", qos=0)
            finish(sensor)
            finish(dashboard)
    return {"tls": "TLSv1.3", "exact_and_subtree_subscribed": True, "owned_session_resumed": True,
            "subscribed_reading_received": True,
            "unassigned_id_and_outside_grant": "mqttiumd refused both" if not reference else "outside common profile"}


def password_profile(work, broker, reference):
    password = secrets.token_hex(16).encode()
    salt = secrets.token_bytes(12)
    derived = hashlib.pbkdf2_hmac("sha512", password, salt, 20000)
    password_file = work / "passwords"
    password_file.write_bytes(b"reader:$7$20000$" + base64.b64encode(salt) + b"$" + base64.b64encode(derived) + b"\n")
    password_file.chmod(0o600)
    with running(work / "password-reference", reference, reference=True, password_file=password_file) as port:
        with peer(port) as client:
            client.exchange(wire.connect("reader", level=5, username="reader", password=password),
                            type="CONNACK", session_present=False, reason=0)
            client.exchange(wire.pingreq(), type="PINGRESP")
            finish(client)
    config = work / "password-reference/reference.conf"
    result = subprocess.run([broker, "check-config", config], capture_output=True, check=False, timeout=10)
    report = json.loads(result.stdout)
    if result.returncode != 2 or report["ok"] or not any(
        row["directive"] == "password_file" and row["status"] == "blocking" for row in report["directives"]
    ):
        raise AssertionError("mqttiumd check-config must report password_file as blocking")
    return {"reference_authenticated": True, "mqttiumd_password_file": "blocking"}


def journal_summary(previous, current):
    sequence = 0
    summaries = []
    for path in (previous, current):
        first = None
        sha = hashlib.sha256()
        with path.open("rb") as lines:
            for line in lines:
                sha.update(line)
                event = json.loads(line)
                if not line.endswith(b"\n") or int(event["seq"]) != sequence + 1:
                    raise AssertionError(f"incomplete line or sequence gap after {sequence}")
                if first is None:
                    first = sequence + 1
                    if path == current:
                        expect(event, kind="log_rotated")
                        expect(event["details"], previous_last_seq=str(sequence))
                sequence += 1
        if first is None:
            raise AssertionError(f"empty journal {path.name}")
        summaries.append({"file": path.name, "first": first, "last": sequence, "sha256": sha.hexdigest()})
    return summaries


def anonymous_journal(work, binary):
    current = work / "events.jsonl"
    previous = work / "events.jsonl.previous"
    with running(work, binary) as port:
        with peer(port) as first:
            connect(first, "anonymous-session")
            finish(first)
        with peer(port) as client:
            connect(client, "anonymous-session", clean=False, present=True)
            client.exchange(wire.pingreq(), type="PINGRESP")
            publications = 0
            deadline = time.monotonic() + 120
            while not previous.exists():
                if time.monotonic() >= deadline or publications >= 65528:
                    raise AssertionError(f"journal did not rotate after {publications} publications")
                ids = range(publications + 1, publications + 9)
                client.sock.sendall(b"".join(wire.publish("journal/sample", b"normal", qos=1, packet_id=mid, level=5)
                                             for mid in ids))
                for mid in ids:
                    expect(client.receive(), type="PUBACK", packet_id=mid, reason=0x10)
                publications += 8
            client.exchange(wire.pingreq(), type="PINGRESP")
            finish(client)
    return {"anonymous_id_resumed": True, "ping_before_and_after_rotation": True,
            "acknowledged_publications": publications, "journal": journal_summary(previous, current)}


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


# Delivery shards of mqttiumd for every profile; 0 is the default runtime.
DELIVERY_SHARDS = 0


def main():
    global DELIVERY_SHARDS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--delivery-shards", type=int, default=0,
                        help="delivery shards of mqttiumd (0 to 16)")
    args = parser.parse_args()
    DELIVERY_SHARDS = args.delivery_shards
    broker = args.broker.resolve()
    reference = harness.reference_binary(args.reference)
    version = harness.reference_version(reference)
    if version != harness.MOSQUITTO_VERSION:
        raise ValueError(f"unexpected reference version {version}")
    with tempfile.TemporaryDirectory(prefix="mqttiumd-listeners-") as tmp:
        work = Path(tmp)
        pki = work / "pki"
        subprocess.run([sys.executable, Path(__file__).resolve().parents[2] / "preview/demo-pki.py", pki],
                       check=True, timeout=30)
        report = {"reference_version": version,
                  "binary_sha256": {path.name: digest(path) for path in (broker, reference)},
                  "mqttiumd": listener(work / "candidate", broker, pki, False),
                  "mosquitto": listener(work / "reference", reference, pki, True),
                  "password_file": password_profile(work, broker, reference),
                  "anonymous_and_rotation": anonymous_journal(work / "anonymous", broker)}
    args.out.parent.mkdir(exist_ok=True, parents=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(args.out)


if __name__ == "__main__":
    main()
