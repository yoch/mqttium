#!/usr/bin/env python3
"""Cross-platform receive-path microbenchmark for PR #445 design choices.

Compares asyncio StreamReader against a #445-like BufferedProtocol for several
reusable receive-buffer sizes.  The buffered variants all use the same read
high/low water budget so receive chunk size is not intentionally coupled to a
larger buffering allowance.  Results include pause/resume counts and peak queued
bytes so any residual coupling from one-chunk overshoot remains visible.

This is diagnostic evidence, not an official mqttium benchmark.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import random
import statistics
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

TOTAL_BYTES = 64 * 1024 * 1024
SERVER_WRITE_CHUNK = 64 * 1024
APP_READ_SIZE = 256 * 1024
FIXED_HIGH_WATER = 1024 * 1024
FIXED_LOW_WATER = 512 * 1024
CHUNKS = (65536, 98304, 122880, 131072, 163840, 262144)


class BenchProtocol(asyncio.BufferedProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop, recv_chunk: int) -> None:
        self.loop = loop
        self.recv_chunk = recv_chunk
        self.buffer = bytearray(recv_chunk)
        self.transport: asyncio.Transport | None = None
        self.chunks: deque[bytes] = deque()
        self.buffered = 0
        self.peak_buffered = 0
        self.eof = False
        self.exc: BaseException | None = None
        self.read_waiter: asyncio.Future[None] | None = None
        self.closed = loop.create_future()
        self.paused = False
        self.pause_count = 0
        self.resume_count = 0
        self.callbacks = 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def connection_lost(self, exc: Exception | None) -> None:
        self.eof = True
        self.exc = exc
        self._wake()
        if not self.closed.done():
            self.closed.set_result(None)

    def eof_received(self):
        self.eof = True
        self._wake()
        return None

    def get_buffer(self, sizehint: int):
        return self.buffer

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        self.callbacks += 1
        chunk = bytes(memoryview(self.buffer)[:nbytes])
        self.chunks.append(chunk)
        self.buffered += nbytes
        self.peak_buffered = max(self.peak_buffered, self.buffered)
        if (
            self.transport is not None
            and not self.paused
            and self.buffered > FIXED_HIGH_WATER
        ):
            self.transport.pause_reading()
            self.paused = True
            self.pause_count += 1
        self._wake()

    def _wake(self) -> None:
        waiter = self.read_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _resume_if_needed(self) -> None:
        if (
            self.transport is not None
            and self.paused
            and self.buffered <= FIXED_LOW_WATER
        ):
            self.transport.resume_reading()
            self.paused = False
            self.resume_count += 1

    async def read(self, n: int) -> bytes:
        while not self.chunks:
            if self.exc is not None:
                raise self.exc
            if self.eof:
                return b""
            waiter = self.loop.create_future()
            self.read_waiter = waiter
            try:
                await waiter
            finally:
                if self.read_waiter is waiter:
                    self.read_waiter = None
        if self.exc is not None:
            raise self.exc
        chunk = self.chunks.popleft()
        if len(chunk) <= n:
            self.buffered -= len(chunk)
            self._resume_if_needed()
            return chunk
        head = chunk[:n]
        self.chunks.appendleft(chunk[n:])
        self.buffered -= len(head)
        self._resume_if_needed()
        return head


@dataclass
class OneResult:
    mode: str
    recv_chunk: int | None
    wall_s: float
    cpu_s: float
    read_calls: int
    callbacks: int | None
    pause_count: int | None
    resume_count: int | None
    peak_buffered: int | None


async def serve_once(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, total: int) -> None:
    del reader
    payload = b"x" * SERVER_WRITE_CHUNK
    remaining = total
    try:
        while remaining:
            part = payload if remaining >= len(payload) else payload[:remaining]
            writer.write(part)
            remaining -= len(part)
            if writer.transport.get_write_buffer_size() > 1024 * 1024:
                await writer.drain()
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def measure_stream(host: str, port: int, total: int) -> OneResult:
    before_cpu = time.process_time()
    started = time.perf_counter()
    reader, writer = await asyncio.open_connection(host, port)
    received = 0
    reads = 0
    try:
        while received < total:
            data = await reader.read(APP_READ_SIZE)
            if not data:
                break
            received += len(data)
            reads += 1
    finally:
        writer.close()
        await writer.wait_closed()
    if received != total:
        raise RuntimeError(f"stream short read: {received} != {total}")
    return OneResult(
        "stream",
        None,
        time.perf_counter() - started,
        time.process_time() - before_cpu,
        reads,
        None,
        None,
        None,
        None,
    )


async def measure_buffered(host: str, port: int, total: int, recv_chunk: int) -> OneResult:
    loop = asyncio.get_running_loop()
    protocol = BenchProtocol(loop, recv_chunk)
    before_cpu = time.process_time()
    started = time.perf_counter()
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    received = 0
    reads = 0
    try:
        while received < total:
            data = await protocol.read(APP_READ_SIZE)
            if not data:
                break
            received += len(data)
            reads += 1
    finally:
        transport.close()
        await protocol.closed
    if received != total:
        raise RuntimeError(f"buffered short read: {received} != {total}")
    return OneResult(
        "buffered",
        recv_chunk,
        time.perf_counter() - started,
        time.process_time() - before_cpu,
        reads,
        protocol.callbacks,
        protocol.pause_count,
        protocol.resume_count,
        protocol.peak_buffered,
    )


async def run_one(mode: str, recv_chunk: int | None, total: int) -> OneResult:
    server = await asyncio.start_server(lambda r, w: serve_once(r, w, total), "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        if mode == "stream":
            return await measure_stream(str(host), int(port), total)
        assert recv_chunk is not None
        return await measure_buffered(str(host), int(port), total, recv_chunk)
    finally:
        server.close()
        await server.wait_closed()


async def main_async(repeats: int, total: int, seed: int) -> dict:
    cells: list[tuple[str, int | None]] = [("stream", None)] + [("buffered", n) for n in CHUNKS]
    plan = [(cell, repeat) for cell in cells for repeat in range(repeats)]
    random.Random(seed).shuffle(plan)
    rows = []
    for index, ((mode, chunk), repeat) in enumerate(plan, 1):
        result = await run_one(mode, chunk, total)
        row = result.__dict__ | {"repeat": repeat, "execution_index": index}
        rows.append(row)
        label = mode if chunk is None else f"buffered-{chunk//1024}K"
        print(
            f"{index:02d}/{len(plan)} {label:14s} wall={result.wall_s:.4f}s "
            f"cpu={result.cpu_s:.4f}s reads={result.read_calls} "
            f"pauses={result.pause_count} peak={result.peak_buffered}",
            file=sys.stderr,
            flush=True,
        )

    summary = {}
    for mode, chunk in cells:
        subset = [r for r in rows if r["mode"] == mode and r["recv_chunk"] == chunk]
        key = mode if chunk is None else f"buffered:{chunk}"
        summary[key] = {
            "n": len(subset),
            "wall_s_median": statistics.median(r["wall_s"] for r in subset),
            "cpu_s_median": statistics.median(r["cpu_s"] for r in subset),
            "read_calls_median": statistics.median(r["read_calls"] for r in subset),
            "callbacks_median": None if chunk is None else statistics.median(r["callbacks"] for r in subset),
            "pause_count_median": None if chunk is None else statistics.median(r["pause_count"] for r in subset),
            "peak_buffered_max": None if chunk is None else max(r["peak_buffered"] for r in subset),
        }
    return {
        "schema_version": 1,
        "probe": "buffered_receive_granularity",
        "official_benchmark": False,
        "platform": platform.platform(),
        "python": sys.version,
        "event_loop": type(asyncio.get_running_loop()).__name__,
        "total_bytes": total,
        "app_read_size": APP_READ_SIZE,
        "fixed_high_water": FIXED_HIGH_WATER,
        "fixed_low_water": FIXED_LOW_WATER,
        "chunks": list(CHUNKS),
        "summary": summary,
        "rows": rows,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--total-mib", type=int, default=64)
    p.add_argument("--seed", type=int, default=445)
    p.add_argument("--output")
    args = p.parse_args()
    payload = asyncio.run(main_async(args.repeats, args.total_mib * 1024 * 1024, args.seed))
    text = json.dumps(payload, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
