#!/usr/bin/env python3
"""Symmetric one-way saturated MQTT receive probe for #445 vs #446.

This is deliberately derived from the final #445 sizing probe, but removes
architecture-specific monkeypatch instrumentation.  The same code measures both
subscriber arms; the workflow selects the MQTTium checkout only via PYTHONPATH.
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


def _proc_snapshot(pid: int) -> tuple[int, int, float] | None:
    """Return (minflt, majflt, cpu_seconds) for a Linux process."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    except OSError:
        return None
    ticks = float(os.sysconf("SC_CLK_TCK"))
    return int(fields[9]), int(fields[11]), (int(fields[13]) + int(fields[14])) / ticks


async def run_subscriber(args: argparse.Namespace) -> dict:
    from mqttium.api import AsyncClient

    first_message = asyncio.Event()
    messages = 0
    payload_bytes = 0
    measuring = False

    def on_message(message) -> None:
        nonlocal messages, payload_bytes
        first_message.set()
        if measuring:
            messages += 1
            payload_bytes += len(message.payload)

    client = AsyncClient(
        f"sat-final-{args.arm}-{os.getpid()}",
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
        broker_before = _proc_snapshot(args.broker_pid) if args.broker_pid else None
        started = time.perf_counter()
        measuring = True
        await asyncio.sleep(args.duration_s)
        measuring = False
        elapsed = time.perf_counter() - started
        after = _usage()
        broker_after = _proc_snapshot(args.broker_pid) if args.broker_pid else None
    finally:
        measuring = False
        await client.disconnect()

    broker_cpu_pct = None
    broker_minflt = None
    broker_majflt = None
    if broker_before is not None and broker_after is not None:
        broker_minflt = broker_after[0] - broker_before[0]
        broker_majflt = broker_after[1] - broker_before[1]
        broker_cpu_pct = 100.0 * (broker_after[2] - broker_before[2]) / elapsed

    stime = after[2] - before[2]
    utime = after[3] - before[3]
    return {
        "role": "subscriber",
        "arm": args.arm,
        "payload_size": args.payload_size,
        "elapsed_s": elapsed,
        "messages": messages,
        "payload_bytes": payload_bytes,
        "payload_mib_s": payload_bytes / elapsed / (1024 * 1024),
        "messages_s": messages / elapsed,
        "ru_minflt": after[0] - before[0],
        "ru_majflt": after[1] - before[1],
        "ru_stime_s": stime,
        "ru_utime_s": utime,
        "ru_cpu_s": stime + utime,
        "sut_cpu_pct": 100.0 * (stime + utime) / elapsed,
        "broker_cpu_pct": broker_cpu_pct,
        "broker_minflt": broker_minflt,
        "broker_majflt": broker_majflt,
    }


async def run_publisher(args: argparse.Namespace) -> dict:
    from mqttium.api import AsyncClient

    payload = b"P" * args.payload_size
    client = AsyncClient(
        f"sat-fixed-pub-{os.getpid()}",
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

    stime = after[2] - before[2]
    utime = after[3] - before[3]
    return {
        "role": "publisher",
        "payload_size": args.payload_size,
        "elapsed_s": elapsed,
        "messages": sent,
        "payload_mib_s": sent * args.payload_size / elapsed / (1024 * 1024),
        "ru_minflt": after[0] - before[0],
        "ru_majflt": after[1] - before[1],
        "ru_stime_s": stime,
        "ru_utime_s": utime,
        "ru_cpu_s": stime + utime,
        "cpu_pct": 100.0 * (stime + utime) / elapsed,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=("subscriber", "publisher"))
    p.add_argument("--arm", default="fixed-publisher")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=11883)
    p.add_argument("--topic", required=True)
    p.add_argument("--payload-size", type=int, required=True)
    p.add_argument("--duration-s", type=float, required=True)
    p.add_argument("--warmup-s", type=float, default=0.5)
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
