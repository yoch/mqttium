#!/usr/bin/env python3
"""Measure what QoS 0 subscribers receive of a stream of publications, and
how fast (record 0063).

One publisher sends QoS 0 messages to a topic that SUBSCRIBERS clients
subscribe at QoS 0. Two shapes:

* `blast`: the publisher writes MESSAGES messages as fast as the broker
  reads them. A broker may shed QoS 0 here; the script reports how many
  each subscriber received and the rate of what was received.
* `bursts`: the publisher writes bursts of BURST messages and waits for a
  PINGRESP after each one, so the broker has read a burst before the next.
  Subscribers that read should receive everything; the script reports the
  rate and what is missing.

Each subscriber is read by its own thread. The script measures the broker
with the Python client beside it on the same host: the figures compare two
executables on one machine and are no capacity claim.

`broker_cpu_us_per_delivery` is the CPU time of the broker (user and
system, read in /proc before it stops) over the deliveries received, the
statistic of `tools/run_perf_gate.py` (report 0043).

    python3 tools/measure_qos0_delivery.py --binary target/release/mqttiumd \
        --shape bursts --messages 100000 --burst 100 --subscribers 1

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

TOPIC = b"measure/qos0"


def cpu_seconds(pid: int) -> float:
    """User plus system CPU time of a process, from /proc (Linux)."""
    import os

    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


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


def read_exact(stream: socket.socket, count: int) -> bytes:
    data = bytearray()
    while len(data) < count:
        chunk = stream.recv(count - len(data))
        if not chunk:
            raise ConnectionError("the broker closed the connection")
        data += chunk
    return bytes(data)


def read_packet(stream: socket.socket) -> tuple[int, bytes]:
    first = read_exact(stream, 1)[0]
    length, shift = 0, 0
    while True:
        byte = read_exact(stream, 1)[0]
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return first, read_exact(stream, length)


def connect(address: tuple[str, int], client: bytes) -> socket.socket:
    stream = socket.create_connection(address)
    stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # MQTT 5, Clean Start, Keep Alive 0, no properties.
    stream.sendall(packet(0x10, text(b"MQTT") + bytes([5, 0x02, 0, 0, 0]) + text(client)))
    first, body = read_packet(stream)
    if first != 0x20 or body[1] != 0:
        raise RuntimeError(f"CONNACK refused: {first:#x} {body!r}")
    return stream


class Subscriber(threading.Thread):
    """Counts the PUBLISH packets it receives; every packet has one size."""

    def __init__(self, address: tuple[str, int], index: int, size: int) -> None:
        super().__init__(daemon=True)
        self.stream = connect(address, b"measure-sub-%d" % index)
        self.stream.sendall(packet(0x82, bytes([0, 1, 0]) + text(TOPIC) + bytes([0])))
        first, _ = read_packet(self.stream)
        if first != 0x90:
            raise RuntimeError("SUBACK expected")
        self.size = size
        self.received = 0
        self.last = time.monotonic()
        self.closed = False

    def run(self) -> None:
        pending = 0
        self.stream.settimeout(0.5)
        while True:
            try:
                chunk = self.stream.recv(1 << 16)
            except TimeoutError:
                continue
            except OSError:
                self.closed = True
                return
            if not chunk:
                self.closed = True
                return
            pending += len(chunk)
            self.received += pending // self.size
            pending %= self.size
            self.last = time.monotonic()


def measure(args: argparse.Namespace) -> dict:
    directory = Path(tempfile.mkdtemp(prefix="mqttiumd-qos0-"))
    command = [
        args.binary,
        "--db", str(directory / "store.sqlite"),
        "--listen", "127.0.0.1:0",
        "--events", str(directory / "events.jsonl"),
    ]
    if args.io_threads:
        command += ["--io-threads", str(args.io_threads)]
    broker = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    try:
        line = broker.stdout.readline()
        if not line.startswith("mqttiumd ready "):
            raise RuntimeError(f"unexpected readiness line: {line!r}")
        host, port = line.split()[2].rsplit(":", 1)
        address = (host, int(port))

        payload = bytes(args.payload)
        # Every message has one size, so a subscriber counts by size alone.
        body = text(TOPIC) + bytes([0])
        frame = packet(0x30, body + payload)
        subscribers = [Subscriber(address, index, len(frame)) for index in range(args.subscribers)]
        for subscriber in subscribers:
            subscriber.start()
        publisher = connect(address, b"measure-pub")

        cpu_start = cpu_seconds(broker.pid)
        start = time.monotonic()
        if args.shape == "blast":
            publisher.sendall(frame * args.messages)
            publisher.sendall(bytes([0xC0, 0]))
            read_packet(publisher)
        else:
            burst = frame * args.burst
            for _ in range(args.messages // args.burst):
                publisher.sendall(burst)
                publisher.sendall(bytes([0xC0, 0]))
                read_packet(publisher)
        published = time.monotonic()

        # Wait until every subscriber has everything, or has been silent for
        # a second.
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            now = time.monotonic()
            if all(s.received >= args.messages or now - s.last > 1.0 or s.closed for s in subscribers):
                break
            time.sleep(0.01)
        cpu = cpu_seconds(broker.pid) - cpu_start
        received = [s.received for s in subscribers]
        ends = [max(s.last, published) for s in subscribers]
        elapsed = max(ends) - start
        return {
            "shape": args.shape,
            "messages": args.messages,
            "burst": args.burst if args.shape == "bursts" else None,
            "subscribers": args.subscribers,
            "payload_bytes": args.payload,
            "io_threads": args.io_threads or "default",
            "received_min": min(received),
            "received_max": max(received),
            "closed_subscribers": sum(s.closed for s in subscribers),
            "publish_seconds": round(published - start, 4),
            "elapsed_seconds": round(elapsed, 4),
            "delivered_per_second": round(sum(received) / elapsed) if elapsed > 0 else None,
            "broker_cpu_us_per_delivery": round(cpu * 1e6 / sum(received), 3) if sum(received) else None,
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
    parser.add_argument("--shape", choices=("blast", "bursts"), default="bursts")
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--burst", type=int, default=100)
    parser.add_argument("--subscribers", type=int, default=1)
    parser.add_argument("--payload", type=int, default=64, help="payload bytes of each message")
    parser.add_argument("--io-threads", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    if args.shape == "bursts" and args.messages % args.burst:
        parser.error("--messages must be a multiple of --burst")
    for _ in range(args.repeat):
        print(json.dumps(measure(args)))
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
