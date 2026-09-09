"""Saturated receive cell with neutral receive-path instrumentation.

Extends the saturated-finalists probe with the statistics needed to
characterise a layout-sensitive allocator regime: receive callbacks, reader
wakeups, pause/resume, decoder capacity and slab reallocations, alongside the
usual throughput and rusage figures.

Instrumentation is symmetric and diagnostic-only. Counters that a candidate
publishes are read through its own ``receive_stats()``; slab growth and shrink
are derived by wrapping ``IncrementalDecoder._reallocate`` inside this process,
identically for either candidate, so neither runtime is modified and neither
gets observability the other lacks. Anything a candidate does not expose is
reported as null rather than approximated.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import time
from pathlib import Path
from typing import Any


def _usage() -> tuple[int, int, float, float]:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return int(r.ru_minflt), int(r.ru_majflt), float(r.ru_stime), float(r.ru_utime)


def _proc_snapshot(pid: int) -> tuple[int, int, float] | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    except (OSError, IndexError):
        return None
    ticks = os.sysconf("SC_CLK_TCK")
    minflt, majflt = int(fields[7]), int(fields[9])
    cpu = (int(fields[11]) + int(fields[12])) / ticks
    return minflt, majflt, cpu


class _SlabWatch:
    """Count slab reallocations without altering allocation policy."""

    def __init__(self) -> None:
        self.growths = 0
        self.shrinks = 0
        self.peak_capacity = 0
        self._original: Any = None
        self._cls: Any = None

    def install(self) -> None:
        from mqttium.codec.buffer import IncrementalDecoder

        self._cls = IncrementalDecoder
        self._original = IncrementalDecoder._reallocate
        original = self._original
        watch = self

        def counting(decoder: Any, capacity: int) -> None:
            current = getattr(decoder, "capacity", 0)
            if capacity > current:
                watch.growths += 1
            elif capacity < current:
                watch.shrinks += 1
            if capacity > watch.peak_capacity:
                watch.peak_capacity = capacity
            original(decoder, capacity)

        IncrementalDecoder._reallocate = counting

    def remove(self) -> None:
        if self._cls is not None and self._original is not None:
            self._cls._reallocate = self._original


def _receive_snapshot(client: Any) -> dict[str, Any]:
    """Read whatever the transport and decoder publish, without assuming either."""
    keys = (
        "recv_callbacks",
        "recv_bytes",
        "reader_resumptions",
        "reader_waits",
        "pause_count",
        "resume_count",
        "receive_window_target",
    )
    out: dict[str, Any] = dict.fromkeys(keys)
    transport = getattr(client, "_transport", None)
    stats = getattr(transport, "receive_stats", None)
    if callable(stats):
        reported = stats()
        for key in keys:
            if key in reported:
                out[key] = reported[key]
    decoder = getattr(client, "_decoder", None)
    for name in ("capacity", "high_water", "capacity_peak"):
        out[f"decoder_{name}"] = getattr(decoder, name, None)
    # Private, best effort, and null when a candidate names it differently.
    for name in ("_target_window", "_receive_window_target"):
        value = getattr(decoder, name, None)
        if value is not None and out["receive_window_target"] is None:
            out["receive_window_target"] = value
    return out


def _delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, end in after.items():
        start = before.get(key)
        if key.startswith("decoder_") or key == "receive_window_target":
            out[key] = end  # levels, not counters
        elif isinstance(end, int) and isinstance(start, int):
            out[key] = end - start
        else:
            out[key] = None
    return out


async def run_subscriber(args: argparse.Namespace) -> dict[str, Any]:
    from mqttium.api import AsyncClient

    first_message = asyncio.Event()
    messages = 0
    payload_bytes = 0
    measuring = False

    def on_message(message: Any) -> None:
        nonlocal messages, payload_bytes
        first_message.set()
        if measuring:
            messages += 1
            payload_bytes += len(message.payload)

    watch = _SlabWatch()
    watch.install()
    client = AsyncClient(
        f"layout-{args.arm}-{os.getpid()}",
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
        receive_before = _receive_snapshot(client)
        growths_before, shrinks_before = watch.growths, watch.shrinks
        before = _usage()
        broker_before = _proc_snapshot(args.broker_pid) if args.broker_pid else None
        started = time.perf_counter()
        measuring = True
        await asyncio.sleep(args.duration_s)
        measuring = False
        elapsed = time.perf_counter() - started
        after = _usage()
        broker_after = _proc_snapshot(args.broker_pid) if args.broker_pid else None
        receive_after = _receive_snapshot(client)
        growths = watch.growths - growths_before
        shrinks = watch.shrinks - shrinks_before
    finally:
        measuring = False
        watch.remove()
        await client.disconnect()

    broker_cpu_pct = broker_minflt = broker_majflt = None
    if broker_before is not None and broker_after is not None:
        broker_minflt = broker_after[0] - broker_before[0]
        broker_majflt = broker_after[1] - broker_before[1]
        broker_cpu_pct = 100.0 * (broker_after[2] - broker_before[2]) / elapsed

    stime = after[2] - before[2]
    utime = after[3] - before[3]
    minflt = after[0] - before[0]
    mib = payload_bytes / (1024 * 1024)
    row: dict[str, Any] = {
        "role": "subscriber",
        "arm": args.arm,
        "position": args.position,
        "cell": args.cell,
        "payload_size": args.payload_size,
        "elapsed_s": elapsed,
        "messages": messages,
        "payload_bytes": payload_bytes,
        "payload_mib_s": mib / elapsed,
        "messages_s": messages / elapsed,
        "ru_minflt": minflt,
        "ru_majflt": after[1] - before[1],
        "minflt_per_message": (minflt / messages) if messages else None,
        "ru_stime_s": stime,
        "ru_utime_s": utime,
        "ru_cpu_s": stime + utime,
        "sut_cpu_pct": 100.0 * (stime + utime) / elapsed,
        "cpu_per_mib": (stime + utime) / mib if mib else None,
        "utime_per_mib": utime / mib if mib else None,
        "stime_per_mib": stime / mib if mib else None,
        "broker_cpu_pct": broker_cpu_pct,
        "broker_minflt": broker_minflt,
        "broker_majflt": broker_majflt,
        "decoder_growths": growths,
        "decoder_shrinks": shrinks,
        "decoder_peak_capacity": watch.peak_capacity or None,
    }
    row.update(_delta(receive_before, receive_after))
    callbacks = row.get("recv_callbacks")
    recv_bytes = row.get("recv_bytes")
    row["bytes_per_callback"] = (
        recv_bytes / callbacks if isinstance(callbacks, int) and callbacks else None
    )
    return row


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--arm", required=True)
    p.add_argument("--cell", type=int, required=True)
    p.add_argument("--position", type=int, required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--topic", required=True)
    p.add_argument("--payload-size", type=int, default=65536)
    p.add_argument("--warmup-s", type=float, default=2.0)
    p.add_argument("--duration-s", type=float, default=3.0)
    p.add_argument("--broker-pid", type=int)
    p.add_argument("--ready-file", required=True)
    p.add_argument("--output", required=True)
    return p


def main() -> None:
    args = build_parser().parse_args()
    row = asyncio.run(run_subscriber(args))
    Path(args.output).write_text(json.dumps(row, indent=2, sort_keys=True) + "\n")
    print(json.dumps(row, sort_keys=True))


if __name__ == "__main__":
    main()
