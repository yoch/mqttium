#!/usr/bin/env python3
"""One-way saturated MQTT receive probe for PR #445 chunk sizing.

Two fresh processes are used per trial: a QoS0 publisher drives continuously and
a subscriber measures the receive path.  The workflow pins publisher, broker and
subscriber to distinct CPUs.  Only the subscriber patches BufferedSocketProtocol
receive-chunk/watermark constants.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import time
from pathlib import Path


def _usage() -> tuple[int, int, float, float]:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return int(r.ru_minflt), int(r.ru_majflt), float(r.ru_stime), float(r.ru_utime)


def _proc_cpu_seconds(pid: int) -> float | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    except OSError:
        return None
    ticks = int(fields[13]) + int(fields[14])
    return ticks / float(os.sysconf("SC_CLK_TCK"))


async def run_subscriber(args: argparse.Namespace) -> dict:
    from mqttium.transport import _buffered

    _buffered._READ_CHUNK = args.recv_chunk
    _buffered._READ_HIGH_WATER = args.high_water
    _buffered._READ_LOW_WATER = args.low_water

    metrics = {
        "recv_callbacks": 0,
        "recv_bytes": 0,
        "pause_count": 0,
        "resume_count": 0,
        "peak_buffered_bytes": 0,
    }
    measuring = False
    original_updated = _buffered.BufferedSocketProtocol.buffer_updated
    original_resume = _buffered.BufferedSocketProtocol._maybe_resume_reading

    def tracked_updated(self, nbytes: int) -> None:
        was_paused = self._paused_reading
        original_updated(self, nbytes)
        if measuring:
            metrics["recv_callbacks"] += 1
            metrics["recv_bytes"] += max(0, nbytes)
            metrics["peak_buffered_bytes"] = max(
                metrics["peak_buffered_bytes"], self._buffered_bytes
            )
            if not was_paused and self._paused_reading:
                metrics["pause_count"] += 1

    def tracked_resume(self) -> None:
        was_paused = self._paused_reading
        original_resume(self)
        if measuring and was_paused and not self._paused_reading:
            metrics["resume_count"] += 1

    _buffered.BufferedSocketProtocol.buffer_updated = tracked_updated
    _buffered.BufferedSocketProtocol._maybe_resume_reading = tracked_resume

    from mqttium.api import AsyncClient

    first_message = asyncio.Event()
    messages = 0
    payload_bytes = 0

    def on_message(message) -> None:
        nonlocal messages, payload_bytes
        first_message.set()
        if measuring:
            messages += 1
            payload_bytes += len(message.payload)

    client = AsyncClient(
        f"sat-sub-{os.getpid()}",
        message_delivery="callback",
        max_pending_delivery_bytes=256 * 1024 * 1024,
        max_pending_inbound_bytes=256 * 1024 * 1024,
    )
    client.on_message = on_message
    await client.connect(args.host, args.port)
    await client.subscribe(args.topic, qos=0)
    Path(args.ready_file).write_text("ready\n", encoding="utf-8")

    try:
        await asyncio.wait_for(first_message.wait(), timeout=10.0)
        await asyncio.sleep(args.warmup_s)
        before = _usage()
        broker_before = _proc_cpu_seconds(args.broker_pid) if args.broker_pid else None
        started = time.perf_counter()
        measuring = True
        await asyncio.sleep(args.duration_s)
        measuring = False
        elapsed = time.perf_counter() - started
        after = _usage()
        broker_after = _proc_cpu_seconds(args.broker_pid) if args.broker_pid else None
    finally:
        measuring = False
        await client.disconnect()
        _buffered.BufferedSocketProtocol.buffer_updated = original_updated
        _buffered.BufferedSocketProtocol._maybe_resume_reading = original_resume

    recv_callbacks = int(metrics["recv_callbacks"])
    return {
        "role": "subscriber",
        "recv_chunk": args.recv_chunk,
        "high_water": args.high_water,
        "low_water": args.low_water,
        "payload_size": args.payload_size,
        "elapsed_s": elapsed,
        "messages": messages,
        "payload_bytes": payload_bytes,
        "payload_mib_s": payload_bytes / elapsed / (1024 * 1024),
        "messages_s": messages / elapsed,
        "recv_callbacks": recv_callbacks,
        "recv_bytes": metrics["recv_bytes"],
        "avg_recv_bytes": (metrics["recv_bytes"] / recv_callbacks) if recv_callbacks else 0.0,
        "pause_count": metrics["pause_count"],
        "resume_count": metrics["resume_count"],
        "peak_buffered_bytes": metrics["peak_buffered_bytes"],
        "ru_minflt": after[0] - before[0],
        "ru_majflt": after[1] - before[1],
        "ru_stime_s": after[2] - before[2],
        "ru_utime_s": after[3] - before[3],
        "sut_cpu_pct": 100.0 * ((after[2] - before[2]) + (after[3] - before[3])) / elapsed,
        "broker_cpu_pct": (
            100.0 * (broker_after - broker_before) / elapsed
            if broker_before is not None and broker_after is not None
            else None
        ),
    }


async def run_publisher(args: argparse.Namespace) -> dict:
    from mqttium.api import AsyncClient

    payload = b"P" * args.payload_size
    client = AsyncClient(
        f"sat-pub-{os.getpid()}",
        max_outbound_bytes=16 * 1024 * 1024,
        max_pending_outbound_bytes=256 * 1024 * 1024,
    )
    await client.connect(args.host, args.port)
    before = _usage()
    started = time.perf_counter()
    sent = 0
    deadline = started + args.duration_s
    try:
        while time.perf_counter() < deadline:
            await client.publish(args.topic, payload, qos=0)
            sent += 1
    finally:
        elapsed = time.perf_counter() - started
        after = _usage()
        await client.disconnect()
    return {
        "role": "publisher",
        "payload_size": args.payload_size,
        "elapsed_s": elapsed,
        "messages": sent,
        "payload_mib_s": sent * args.payload_size / elapsed / (1024 * 1024),
        "ru_minflt": after[0] - before[0],
        "ru_majflt": after[1] - before[1],
        "ru_stime_s": after[2] - before[2],
        "ru_utime_s": after[3] - before[3],
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=("subscriber", "publisher"))
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=11883)
    p.add_argument("--topic", required=True)
    p.add_argument("--payload-size", type=int, required=True)
    p.add_argument("--duration-s", type=float, required=True)
    p.add_argument("--warmup-s", type=float, default=0.5)
    p.add_argument("--recv-chunk", type=int, default=64 * 1024)
    p.add_argument("--high-water", type=int, default=128 * 1024)
    p.add_argument("--low-water", type=int, default=64 * 1024)
    p.add_argument("--ready-file")
    p.add_argument("--broker-pid", type=int, default=0)
    p.add_argument("--output", required=True)
    return p


async def _main_async(args: argparse.Namespace) -> dict:
    if args.role == "subscriber":
        if not args.ready_file:
            raise SystemExit("subscriber requires --ready-file")
        return await run_subscriber(args)
    return await run_publisher(args)


def main() -> int:
    args = build_parser().parse_args()
    payload = asyncio.run(_main_async(args))
    Path(args.output).write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
