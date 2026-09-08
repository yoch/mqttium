#!/usr/bin/env python3
"""Targeted allocator probe for PR #446 StreamReader backlog.

This is diagnostic evidence, not an official MQTT benchmark. A separate server
process sends 192-KiB bursts. The client deliberately does not consume a burst
until the transport's unread queue is strictly above StreamReader's 128-KiB
pause threshold. For #446 this guarantees that ``read(256 KiB)`` exercises a
returned-bytes allocation larger than 128 KiB without assuming that selector
callbacks arrive as two exact 80-KiB chunks.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import socket
import struct
from typing import Any

BURST_BYTES = 192 * 1024
FORCED_BACKLOG_BYTES = 128 * 1024 + 1
READ_BYTES = 256 * 1024
HEADER = struct.Struct("!II")


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("peer closed early")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def run_server(host: str, port: int) -> None:
    payload = b"x" * BURST_BYTES
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen()
        while True:
            conn, _ = listener.accept()
            try:
                with conn:
                    warmup, measured = HEADER.unpack(_recv_exact(conn, HEADER.size))
                    for _ in range(warmup):
                        conn.sendall(payload)
                        if _recv_exact(conn, 1) != b"A":
                            raise RuntimeError("invalid warmup acknowledgement")
                    if _recv_exact(conn, 1) != b"M":
                        raise RuntimeError("missing measurement start marker")
                    for _ in range(measured):
                        conn.sendall(payload)
                        if _recv_exact(conn, 1) != b"A":
                            raise RuntimeError("invalid measurement acknowledgement")
            except (ConnectionError, ConnectionResetError, BrokenPipeError):
                # A failed diagnostic client must not kill the reusable server;
                # the orchestrator records and surfaces the client failure.
                continue


def _buffered_bytes(transport: Any) -> tuple[str, int]:
    reader = getattr(transport, "_reader", None)
    if reader is not None:
        buffer = getattr(reader, "_buffer", None)
        if buffer is None:
            raise RuntimeError("StreamReader has no _buffer diagnostic attribute")
        return "streamreader", len(buffer)

    protocol = getattr(transport, "_protocol", None)
    if protocol is not None and hasattr(protocol, "buffered_bytes"):
        return "direct", int(protocol.buffered_bytes)

    raise RuntimeError(f"unsupported transport shape: {type(transport)!r}")


async def _wait_for_backlog(transport: Any, timeout_s: float) -> tuple[str, int]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    maximum = 0
    implementation = "unknown"
    while True:
        implementation, current = _buffered_bytes(transport)
        maximum = max(maximum, current)
        if current >= FORCED_BACKLOG_BYTES:
            return implementation, maximum
        if loop.time() >= deadline:
            raise TimeoutError(
                f"backlog never exceeded 128 KiB; max={maximum} impl={implementation}"
            )
        await asyncio.sleep(0)


async def _consume_burst(transport: Any) -> int:
    first = await transport.read(READ_BYTES)
    if not first:
        raise ConnectionError("short burst before first read")
    received = len(first)
    while received < BURST_BYTES:
        data = await transport.read(READ_BYTES)
        if not data:
            raise ConnectionError(f"short burst: {received}/{BURST_BYTES}")
        received += len(data)
    if received != BURST_BYTES:
        raise RuntimeError(f"burst overshoot: {received}/{BURST_BYTES}")
    return len(first)


async def run_client(host: str, port: int, warmup: int, measured: int, timeout_s: float) -> dict[str, Any]:
    from mqttium.transport.tcp import TcpTransport

    transport = await TcpTransport.connect(host, port)
    max_backlog = 0
    first_read_sizes: list[int] = []
    implementation = "unknown"
    try:
        await transport.write(HEADER.pack(warmup, measured))
        for _ in range(warmup):
            implementation, seen = await _wait_for_backlog(transport, timeout_s)
            max_backlog = max(max_backlog, seen)
            await _consume_burst(transport)
            await transport.write(b"A")

        before = resource.getrusage(resource.RUSAGE_SELF)
        await transport.write(b"M")
        for _ in range(measured):
            implementation, seen = await _wait_for_backlog(transport, timeout_s)
            max_backlog = max(max_backlog, seen)
            first_read_sizes.append(await _consume_burst(transport))
            await transport.write(b"A")
        after = resource.getrusage(resource.RUSAGE_SELF)
    finally:
        await transport.close()

    if implementation == "streamreader" and min(first_read_sizes) < FORCED_BACKLOG_BYTES:
        raise RuntimeError(
            "StreamReader first read did not exercise >128-KiB returned allocation: "
            f"min={min(first_read_sizes)}"
        )

    minflt = after.ru_minflt - before.ru_minflt
    majflt = after.ru_majflt - before.ru_majflt
    stime = after.ru_stime - before.ru_stime
    utime = after.ru_utime - before.ru_utime
    return {
        "schema_version": 2,
        "probe": "forced_over_128k_unread_backlog",
        "implementation": implementation,
        "burst_bytes": BURST_BYTES,
        "forced_backlog_bytes": FORCED_BACKLOG_BYTES,
        "read_bytes": READ_BYTES,
        "warmup_cycles": warmup,
        "measured_cycles": measured,
        "max_buffered_bytes": max_backlog,
        "first_read_bytes_min": min(first_read_sizes),
        "first_read_bytes_max": max(first_read_sizes),
        "forced_backlog_proven": max_backlog >= FORCED_BACKLOG_BYTES,
        "large_streamreader_read_proven": (
            implementation != "streamreader" or min(first_read_sizes) >= FORCED_BACKLOG_BYTES
        ),
        "ru_minflt": minflt,
        "ru_majflt": majflt,
        "ru_stime_s": stime,
        "ru_utime_s": utime,
        "minflt_per_cycle": minflt / measured,
        "stime_us_per_cycle": stime * 1_000_000 / measured,
        "utime_us_per_cycle": utime * 1_000_000 / measured,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--client", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11990)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    args = parser.parse_args()
    if args.server == args.client:
        parser.error("choose exactly one of --server or --client")
    if args.server:
        run_server(args.host, args.port)
        return
    result = asyncio.run(run_client(args.host, args.port, args.warmup, args.cycles, args.timeout_s))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
