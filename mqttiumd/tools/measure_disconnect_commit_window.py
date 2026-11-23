#!/usr/bin/env python3
"""Measure the window between the end of a client's stream and the commit
of the end of its session (record 0059).

A client opens a session with Session Expiry Interval 600 s and ends it with
DISCONNECT 0x00 and interval 0. The broker closes the transport first and
commits afterwards. For each sample the script records, on one clock:

* when the client read the end of its stream;
* when the writer's `store_result` event of the close reached the event
  file, read by a thread that follows the file.

The difference is an upper reading of the window on this host: it includes
the time the event takes to be written and seen. It is not a bound, and a
kill of the process says nothing about a power loss.

    python3 tools/measure_disconnect_commit_window.py \
        --binary target/debug/mqttiumd --samples 200 [--storage-mode log]

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


def text(value: bytes) -> bytes:
    return struct.pack(">H", len(value)) + value


def packet(first: int, body: bytes) -> bytes:
    length, encoded = len(body), bytearray()
    while True:
        byte, length = length % 128, length // 128
        encoded.append(byte | (0x80 if length else 0))
        if not length:
            return bytes([first]) + bytes(encoded) + body


def read_exact(stream: socket.socket, count: int) -> bytes:
    data = b""
    while len(data) < count:
        chunk = stream.recv(count - len(data))
        if not chunk:
            raise ConnectionError("the broker closed before its CONNACK")
        data += chunk
    return data


def follow(path: Path, closes: list[float], stop: threading.Event) -> None:
    """Timestamps each `store_result` of a committed session close."""
    while not path.exists():
        time.sleep(0.001)
    with path.open("rb") as events:
        pending = b""
        while not stop.is_set():
            chunk = events.read()
            if not chunk:
                time.sleep(0.00005)
                continue
            seen = time.perf_counter()
            pending += chunk
            *lines, pending = pending.split(b"\n")
            for line in lines:
                if b'"store_result"' in line and b'"SessionClose"' in line:
                    closes.append(seen)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--binary", required=True)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--storage-mode", default="durable", choices=("durable", "log"))
    args = parser.parse_args()

    directory = Path(tempfile.mkdtemp(prefix="mqttiumd-window-"))
    events = directory / "events.jsonl"
    broker = subprocess.Popen(
        [
            args.binary,
            "--db",
            str(directory / "store.sqlite"),
            "--listen",
            "127.0.0.1:0",
            "--events",
            str(events),
            "--event-level",
            "audit",
            "--storage-mode",
            args.storage_mode,
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        ready = broker.stdout.readline()
        if not ready.startswith("mqttiumd ready "):
            print(f"unexpected readiness line: {ready!r}", file=sys.stderr)
            return 1
        host, port = ready.split()[2].rstrip(",").rsplit(":", 1)
        results: list[float] = []
        stop = threading.Event()
        follower = threading.Thread(target=follow, args=(events, results, stop), daemon=True)
        follower.start()
        windows = []
        for sample in range(args.samples):
            stream = socket.create_connection((host, int(port)))
            stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            connect = (
                text(b"MQTT")
                + bytes([5, 0x02, 0, 60])
                + bytes([5, 0x11])
                + struct.pack(">I", 600)
                + text(f"window-{sample}".encode())
            )
            stream.sendall(packet(0x10, connect))
            header = read_exact(stream, 2)
            connack = read_exact(stream, header[1])
            if header[0] != 0x20 or connack[1] != 0:
                print(f"CONNECT refused: {header!r} {connack!r}", file=sys.stderr)
                return 1
            stream.sendall(packet(0xE0, bytes([0, 5, 0x11, 0, 0, 0, 0])))
            if stream.recv(16) != b"":
                print("the broker answered a DISCONNECT", file=sys.stderr)
                return 1
            end_of_stream = time.perf_counter()
            stream.close()
            deadline = end_of_stream + 5
            while len(results) <= sample:
                if time.perf_counter() > deadline:
                    print("the Close was not reported", file=sys.stderr)
                    return 1
                time.sleep(0.00005)
            windows.append((results[sample] - end_of_stream) * 1000)
        stop.set()
        ordered = sorted(windows)
        report = {
            "storage_mode": args.storage_mode,
            "samples": len(ordered),
            "commit_after_end_of_stream": sum(1 for value in ordered if value > 0),
            "milliseconds": {
                "min": round(ordered[0], 3),
                "median": round(statistics.median(ordered), 3),
                "p95": round(ordered[int(len(ordered) * 0.95) - 1], 3),
                "max": round(ordered[-1], 3),
            },
        }
        print(json.dumps(report, indent=2))
        return 0
    finally:
        broker.send_signal(signal.SIGINT)
        try:
            broker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            broker.kill()
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
