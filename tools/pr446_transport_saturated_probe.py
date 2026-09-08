#!/usr/bin/env python3
"""Saturated raw-TCP receive probe for PR #445 vs PR #446.

Diagnostic only. The server continuously writes a fixed byte budget; the client
reads through mqttium's TcpTransport and reports throughput, CPU time, read-call
count and page faults. Run server/client on different CPUs from the workflow.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import time

TOTAL_DEFAULT = 256 * 1024 * 1024
WRITE_CHUNK = 64 * 1024
APP_READ = 256 * 1024


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    total: int,
) -> None:
    del reader
    block = b"x" * WRITE_CHUNK
    remaining = total
    try:
        while remaining:
            part = block if remaining >= WRITE_CHUNK else block[:remaining]
            writer.write(part)
            remaining -= len(part)
            if writer.transport.get_write_buffer_size() > 1024 * 1024:
                await writer.drain()
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def run_server(host: str, port: int, total: int) -> None:
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, total),
        host,
        port,
    )
    async with server:
        await server.serve_forever()


async def run_client(host: str, port: int, total: int) -> dict[str, float | int]:
    from mqttium.transport.tcp import TcpTransport

    before = resource.getrusage(resource.RUSAGE_SELF)
    cpu_before = time.process_time()
    start = time.perf_counter()
    transport = await TcpTransport.connect(host, port)
    received = 0
    reads = 0
    try:
        while received < total:
            data = await transport.read(APP_READ)
            if not data:
                break
            received += len(data)
            reads += 1
    finally:
        await transport.close()
    elapsed = time.perf_counter() - start
    cpu = time.process_time() - cpu_before
    after = resource.getrusage(resource.RUSAGE_SELF)
    if received != total:
        raise RuntimeError(f"short read: {received} != {total}")
    return {
        "bytes": received,
        "wall_s": elapsed,
        "cpu_s": cpu,
        "mib_s": received / (1024 * 1024) / elapsed,
        "read_calls": reads,
        "ru_minflt": after.ru_minflt - before.ru_minflt,
        "ru_majflt": after.ru_majflt - before.ru_majflt,
        "ru_utime_s": after.ru_utime - before.ru_utime,
        "ru_stime_s": after.ru_stime - before.ru_stime,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=("server", "client"))
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=11999)
    p.add_argument("--total", type=int, default=TOTAL_DEFAULT)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "server":
        asyncio.run(run_server(args.host, args.port, args.total))
    else:
        print(json.dumps(asyncio.run(run_client(args.host, args.port, args.total)), sort_keys=True))


if __name__ == "__main__":
    main()
