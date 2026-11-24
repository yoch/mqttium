#!/usr/bin/env python3
"""Measure QoS 1 between pairs of clients, and the broker CPU time it costs.

PAIRS publishers each send MESSAGES QoS 1 publications to a topic of their
own, keeping WINDOW of them unacknowledged; one subscriber per topic
subscribes at QoS 1 and acknowledges every PUBLISH it receives. The broker
runs on the volatile store by default, so that synchronous writes do not
hide the cost of the core (report 0046).

The script prints one JSON line: the publications acknowledged per second,
the deliveries received, and `broker_cpu_us_per_delivery`, the CPU time of
the broker (user and system, read in /proc before it stops) over the
deliveries received. The figures compare two executables on one machine and
are no capacity claim.

    python3 tools/measure_qos1_pairs.py --binary target/dist/mqttiumd --pairs 8

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path


def text(value: bytes) -> bytes:
    return len(value).to_bytes(2, "big") + value


def remaining(length: int) -> bytes:
    out = bytearray()
    while True:
        byte, length = length % 128, length // 128
        out.append(byte | (0x80 if length else 0))
        if not length:
            return bytes(out)


def packet(first: int, body: bytes) -> bytes:
    return bytes([first]) + remaining(len(body)) + body


def publish_qos1(topic: bytes, packet_id: int, payload: bytes) -> bytes:
    """A v5 QoS 1 PUBLISH with no properties."""
    return packet(0x32, text(topic) + packet_id.to_bytes(2, "big") + b"\x00" + payload)


def puback(packet_id_bytes: bytes) -> bytes:
    """A v5 PUBACK with reason code 0."""
    return bytes([0x40, 3]) + packet_id_bytes + b"\x00"


def cpu_seconds(pid: int) -> float:
    """User plus system CPU time of a process, from /proc (Linux)."""
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


class Reader:
    """Splits what a socket receives into MQTT packets."""

    def __init__(self, stream: socket.socket) -> None:
        self.stream = stream
        self.buffer = bytearray()

    def feed(self, data: bytes) -> list[tuple[int, bytes]]:
        self.buffer += data
        packets = []
        while len(self.buffer) >= 2:
            length, shift, index = 0, 0, 1
            while True:
                if index >= len(self.buffer):
                    return packets
                byte = self.buffer[index]
                length |= (byte & 0x7F) << shift
                index += 1
                if not byte & 0x80:
                    break
                shift += 7
            if len(self.buffer) < index + length:
                return packets
            packets.append((self.buffer[0], bytes(self.buffer[index:index + length])))
            del self.buffer[:index + length]
        return packets

    def packets(self):
        while True:
            chunk = self.stream.recv(1 << 16)
            if not chunk:
                raise ConnectionError("the broker closed the connection")
            yield from self.feed(chunk)


def connect(address: tuple[str, int], client: bytes) -> tuple[socket.socket, Reader]:
    stream = socket.create_connection(address)
    stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # MQTT 5, Clean Start, Keep Alive 0, Receive Maximum 65535.
    properties = bytes([0x21, 0xFF, 0xFF])
    stream.sendall(packet(0x10, text(b"MQTT") + bytes([5, 0x02, 0, 0]) + remaining(len(properties))
                          + properties + text(client)))
    reader = Reader(stream)
    first, body = next(reader.packets())
    if first != 0x20 or body[1] != 0:
        raise RuntimeError(f"CONNACK refused: {first:#x} {body!r}")
    return stream, reader


def subscriber(address, index: int, messages: int, ready: threading.Event, counts: list) -> None:
    stream, reader = connect(address, b"measure-q1-sub-%d" % index)
    stream.sendall(packet(0x82, bytes([0, 1, 0]) + text(b"measure/q1/%d" % index) + bytes([1])))
    received = 0
    for first, body in reader.packets():
        if first >> 4 == 9:
            ready.set()
            continue
        if first >> 4 == 3:
            topic_length = int.from_bytes(body[:2], "big")
            stream.sendall(puback(body[2 + topic_length:4 + topic_length]))
            received += 1
            if received == messages:
                counts[index] = received
                stream.close()
                return


def publisher(address, index: int, messages: int, window: int, payload: bytes, ends: list) -> None:
    stream, reader = connect(address, b"measure-q1-pub-%d" % index)
    topic = b"measure/q1/%d" % index
    sent = min(window, messages)
    stream.sendall(b"".join(publish_qos1(topic, k % 65535 + 1, payload) for k in range(sent)))
    acknowledged = 0
    for first, _ in reader.packets():
        if first >> 4 == 4:
            acknowledged += 1
            if sent < messages:
                stream.sendall(publish_qos1(topic, sent % 65535 + 1, payload))
                sent += 1
            if acknowledged == messages:
                break
    ends[index] = time.monotonic()
    stream.close()


def measure(args: argparse.Namespace) -> dict:
    directory = Path(tempfile.mkdtemp(prefix="mqttiumd-qos1-"))
    broker = subprocess.Popen(
        [args.binary, "--db", str(directory / "store.sqlite"), "--listen", "127.0.0.1:0",
         "--events", str(directory / "events.jsonl"), "--storage-mode", args.storage],
        stdout=subprocess.PIPE, text=True)
    try:
        line = broker.stdout.readline()
        if not line.startswith("mqttiumd ready "):
            raise RuntimeError(f"unexpected readiness line: {line!r}")
        host, port = line.split()[2].rsplit(":", 1)
        address = (host, int(port))
        ready = [threading.Event() for _ in range(args.pairs)]
        counts = [0] * args.pairs
        ends = [0.0] * args.pairs
        subscribers = [threading.Thread(target=subscriber, args=(address, i, args.messages, ready[i], counts),
                                        daemon=True) for i in range(args.pairs)]
        for thread in subscribers:
            thread.start()
        for event in ready:
            if not event.wait(10):
                raise RuntimeError("SUBACK expected")
        publishers = [threading.Thread(target=publisher,
                                       args=(address, i, args.messages, args.window, bytes(args.payload), ends),
                                       daemon=True) for i in range(args.pairs)]
        cpu_start = cpu_seconds(broker.pid)
        start = time.monotonic()
        for thread in publishers:
            thread.start()
        for thread in publishers + subscribers:
            thread.join(args.timeout)
        cpu = cpu_seconds(broker.pid) - cpu_start
        elapsed = max(ends) - start
        received = sum(counts)
        return {
            "pairs": args.pairs,
            "messages": args.messages,
            "window": args.window,
            "payload_bytes": args.payload,
            "storage": args.storage,
            "received": received,
            "acked_per_second": round(args.pairs * args.messages / elapsed) if elapsed > 0 else None,
            "broker_cpu_us_per_delivery": round(cpu * 1e6 / received, 3) if received else None,
        }
    finally:
        broker.terminate()
        try:
            broker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            broker.kill()
        shutil.rmtree(directory, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--binary", required=True, help="the mqttiumd executable")
    parser.add_argument("--pairs", type=int, default=1)
    parser.add_argument("--messages", type=int, default=20_000, help="publications per publisher")
    parser.add_argument("--window", type=int, default=8,
                        help="publications in flight per publisher; the broker announces 8")
    parser.add_argument("--payload", type=int, default=64, help="payload bytes of each message")
    parser.add_argument("--storage", default="volatile", choices=("durable", "log", "volatile"))
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    print(json.dumps(measure(args)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
