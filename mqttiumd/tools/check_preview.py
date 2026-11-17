#!/usr/bin/env python3
"""Extract and execute the normal preview without Git, Cargo or laboratory I/O."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tarfile
import time


def platforms(build: dict) -> dict:
    return {"build_platform": build["platform"], "build_machine": build["machine"],
            "execution_platform": platform.platform(), "execution_machine": platform.machine(),
            "execution_python": platform.python_version()}


def require(ok: bool, code: str) -> None:
    if not ok:
        raise AssertionError(code)


def until(probe, code: str):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(.01)
    raise AssertionError(code)


def main(archive: Path, out: Path) -> None:
    require(not out.exists(), "smoke_output_already_exists")
    out.mkdir(parents=True)
    extracted = out / "extracted"
    extracted.mkdir()
    with tarfile.open(archive, "r:gz") as tar:
        entries = tar.getmembers()
        require(len(entries) <= 512 and sum(e.size for e in entries) <= 256 * 1024 * 1024,
                "archive_budget_exceeded")
        require(all(not e.issym() and not e.islnk() and not Path(e.name).is_absolute()
                    and ".." not in Path(e.name).parts for e in entries), "unsafe_archive_entry")
        tar.extractall(extracted, filter="data")
    roots = list(extracted.iterdir())
    require(len(roots) == 1 and roots[0].is_dir(), "archive_root_invalid")
    package = roots[0]
    for line in (package / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        require(hashlib.sha256((package / name).read_bytes()).hexdigest() == digest,
                "archive_member_hash_mismatch")
    build = json.loads((package / "BUILD.json").read_text())
    binary = package / "mqttiumd"
    require(hashlib.sha256(binary.read_bytes()).hexdigest() == build["binary_sha256"],
            "archive_binary_identity_mismatch")
    require(build["features"] == [] and build["instrumented"] is False,
            "archive_is_not_normal")
    actions: list[dict] = []
    children: list[dict] = []
    data = out / "data"
    process = None
    peers: list[socket.socket] = []

    def start():
        log = (out / f"start-{len(children)}.log").open("wb")
        child = subprocess.Popen([str(package / "start.sh"), str(data)], cwd=package,
                                 env=dict(os.environ, MQTTIUMD_LISTEN="127.0.0.1:0"),
                                 stdout=log, stderr=subprocess.STDOUT)
        children.append({"pid": child.pid, "status": None})
        known = len(children)
        def ready():
            if child.poll() is not None:
                raise AssertionError("packaged_broker_failed_startup")
            files = sorted((data / "logs").glob("run.*/events.jsonl")) if data.exists() else []
            for f in files:
                for line in f.read_text().splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if event["kind"] == "ready" and f not in seen_events:
                        seen_events.add(f)
                        actions.append({"event": "packaged_ready", "run": known,
                                        "details": event["details"], "events": str(f)})
                        host, port = event["details"]["address"].rsplit(":", 1)
                        return host, int(port)
            return None
        address = until(ready, "packaged_broker_no_readiness")
        log.close()
        return child, address

    def stop(child):
        child.send_signal(signal.SIGINT)
        try:
            status = child.wait(timeout=8)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
            raise AssertionError("packaged_broker_shutdown_timeout")
        children[-1]["status"] = status
        require(status == 0, "packaged_broker_shutdown_failure")

    def vbi(n: int) -> bytes:
        result = bytearray()
        while True:
            d = n % 128
            n //= 128
            result.append(d | (128 if n else 0))
            if not n:
                return bytes(result)

    def text(value: bytes) -> bytes:
        return len(value).to_bytes(2, "big") + value

    def send(peer, first: int, body: bytes):
        frame = bytes([first]) + vbi(len(body)) + body
        peer.sendall(frame)
        actions.append({"direction": "to_broker", "hex": frame.hex()})

    def exact(peer, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            part = peer.recv(n - len(buf))
            require(bool(part), "unexpected_packaged_peer_eof")
            buf.extend(part)
        return bytes(buf)

    def receive(peer, first: int):
        head = exact(peer, 1)[0]
        n, power = 0, 1
        header = bytearray([head])
        for _ in range(4):
            d = exact(peer, 1)[0]
            header.append(d)
            n += (d & 127) * power
            if not d & 128:
                break
            power *= 128
        require(n <= 65536, "packaged_frame_exceeds_bound")
        body = exact(peer, n)
        actions.append({"direction": "from_broker", "hex": (header + body).hex()})
        require(head == first, "packaged_wrong_packet")
        return body

    def connect(address, name: bytes, present: bool):
        peer = socket.create_connection(address, timeout=3)
        peer.settimeout(3)
        peers.append(peer)
        send(peer, 0x10, b"\0\x04MQTT\x05\0\0\x0a\x05\x11\xff\xff\xff\xff" + text(name))
        connack = receive(peer, 0x20)
        require(connack[:2] == bytes([int(present), 0]), "packaged_session_present_mismatch")
        return peer

    def publish(peer, mid: int, payload: bytes):
        send(peer, 0x32, text(b"atelier/salle1/temperature") + mid.to_bytes(2, "big") + b"\0" + payload)
        require(receive(peer, 0x40)[:2] == mid.to_bytes(2, "big"), "packaged_publish_not_admitted")

    def message(peer, expected: bytes):
        body = receive(peer, 0x32)
        length = int.from_bytes(body[:2], "big")
        require(body[2:2+length] == b"atelier/salle1/temperature", "packaged_wrong_topic")
        mid = body[2+length:4+length]
        require(body[4+length:] == b"\0" + expected, "packaged_wrong_payload")
        send(peer, 0x40, mid)

    def snapshot():
        with sqlite3.connect(f"file:{data / 'broker.sqlite'}?mode=ro", uri=True) as db:
            return {"sessions": db.execute("select count(*) from sessions").fetchone()[0],
                    "routes": db.execute("select count(*) from routes").fetchone()[0],
                    "obligations": db.execute("select count(*) from obligations").fetchone()[0]}

    seen_events: set[Path] = set()
    try:
        process, address = start()
        subscriber = connect(address, b"archive-smoke-sub", False)
        send(subscriber, 0x82, b"\0\x01\0" + text(b"atelier/+/temperature") + b"\x01"
             + text(b"atelier/#") + b"\x01")
        require(receive(subscriber, 0x90) == b"\0\x01\0\x01\x01", "packaged_subscribe_not_granted")
        publisher = connect(address, b"archive-smoke-pub", False)
        publish(publisher, 1, b"archive:1")
        message(subscriber, b"archive:1")
        until(lambda: snapshot()["obligations"] == 0, "packaged_overlap_not_single_or_not_finished")
        send(subscriber, 0xe0, b"")
        subscriber.close()
        # Ordered control roundtrip after close processing, then independent SQL fact.
        send(publisher, 0xc0, b"")
        receive(publisher, 0xd0)
        publish(publisher, 2, b"archive:2-offline")
        until(lambda: snapshot()["obligations"] == 1, "packaged_offline_obligation_missing")
        publisher.close()
        stop(process)
        process = None
        before = snapshot()
        process, address = start()
        subscriber = connect(address, b"archive-smoke-sub", True)
        message(subscriber, b"archive:2-offline")
        until(lambda: snapshot()["obligations"] == 0, "packaged_recovery_not_finished")
        final = snapshot()
        stop(process)
        process = None
        require(before["sessions"] == final["sessions"] == 2 and final["routes"] == 2,
                "packaged_database_was_replaced")
        record = {"status": "PASS", "source_sha": build["source_sha"],
                  "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                  "binary_sha256": build["binary_sha256"],
                  **platforms(build),
                  "before_restart": before, "after_restart": final,
                  "same_database": str(data / "broker.sqlite"), "processes": children,
                  "observations": actions}
        (out / "receipt.json").write_text(json.dumps(record, indent=2) + "\n")
    finally:
        for peer in peers:
            peer.close()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    main(args.archive.resolve(), args.out.resolve())
