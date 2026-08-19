"""Temporary causal study of writer cooperation and transport write granularity.

This harness deliberately keeps the production source tree immutable. Worker
processes import one exact mqttium checkout, install one process-local experiment
mode, and run the same mixed QoS0-flood/QoS1-receipt workload in ABBA order.

The modes are causal probes, not production policy selections:

* base: instrument current StreamTransport.write_many without changing policy;
* cap-yield: yield once while the writer still owns a full 256-item contiguous
  group and backlog remains;
* large-yield: yield after a large contiguous group while backlog remains;
* chunk-drain: split one logical write_many into transport sub-bursts, checking
  drain after each, without an explicit scheduler yield;
* chunk-yield: chunk-drain plus sleep(0) between transport sub-bursts.

Logical WritePump batching, resident accounting, queue order and waiter release
remain unchanged in every non-base mode.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

_MODES = ("base", "cap-yield", "large-yield", "chunk-drain", "chunk-yield")
_TRACE_LIMIT = 20_000
_TELEMETRY: dict[str, Any] = {}


def _reset_telemetry() -> None:
    global _TELEMETRY
    _TELEMETRY = {
        "write_many_calls": 0,
        "transport_subwrites": 0,
        "drain_calls": 0,
        "drain_block_ms": 0.0,
        "sync_submit_ms": 0.0,
        "max_pending_before": 0,
        "max_pending_after": 0,
        "cooperative_yields": 0,
        "events": [],
    }


def _record_event(event: dict[str, Any]) -> None:
    events = _TELEMETRY["events"]
    if len(events) < _TRACE_LIMIT:
        events.append(event)


def _pending_bytes(stream_transport: Any) -> int:
    transport = stream_transport._writer.transport
    return 0 if transport is None else int(transport.get_write_buffer_size())


async def _submit_parts(stream_transport: Any, parts: list[bytes], *, more: bool, do_yield: bool) -> None:
    from mqttium.transport._stream import write_buffer_needs_drain

    if not parts:
        return
    before = _pending_bytes(stream_transport)
    _TELEMETRY["max_pending_before"] = max(_TELEMETRY["max_pending_before"], before)
    started = time.perf_counter()
    if len(parts) == 1:
        stream_transport._writer.write(parts[0])
    else:
        stream_transport._writer.writelines(parts)
    submit_ms = (time.perf_counter() - started) * 1000.0
    _TELEMETRY["sync_submit_ms"] += submit_ms
    _TELEMETRY["transport_subwrites"] += 1
    after = _pending_bytes(stream_transport)
    _TELEMETRY["max_pending_after"] = max(_TELEMETRY["max_pending_after"], after)
    drain_ms = 0.0
    drained = False
    if write_buffer_needs_drain(stream_transport._writer):
        drained = True
        _TELEMETRY["drain_calls"] += 1
        drain_started = time.perf_counter()
        await stream_transport._writer.drain()
        drain_ms = (time.perf_counter() - drain_started) * 1000.0
        _TELEMETRY["drain_block_ms"] += drain_ms
    yielded = False
    if do_yield and more:
        yielded = True
        _TELEMETRY["cooperative_yields"] += 1
        await asyncio.sleep(0)
    _record_event(
        {
            "kind": "transport-submit",
            "t": time.perf_counter(),
            "items": len(parts),
            "bytes": sum(map(len, parts)),
            "pending_before": before,
            "pending_after": after,
            "submit_ms": submit_ms,
            "drained": drained,
            "drain_ms": drain_ms,
            "yielded": yielded,
        }
    )


def _install_mode(mode: str, *, chunk_bytes: int, large_yield_bytes: int) -> None:
    """Install one experiment mode before constructing AsyncClient."""
    _reset_telemetry()

    from mqttium.api._writer import WritePump
    from mqttium.transport._stream import StreamTransport

    original_write_many = StreamTransport.write_many

    async def instrumented_write_many(self: Any, parts: list[bytes]) -> None:
        if not parts:
            return
        _TELEMETRY["write_many_calls"] += 1

        if mode not in ("chunk-drain", "chunk-yield"):
            await _submit_parts(self, parts, more=False, do_yield=False)
            return

        chunks: list[list[bytes]] = []
        current: list[bytes] = []
        current_bytes = 0
        for part in parts:
            size = len(part)
            if current and current_bytes + size > chunk_bytes:
                chunks.append(current)
                current = []
                current_bytes = 0
            current.append(part)
            current_bytes += size
            if current_bytes >= chunk_bytes:
                chunks.append(current)
                current = []
                current_bytes = 0
        if current:
            chunks.append(current)

        for index, chunk in enumerate(chunks):
            await _submit_parts(
                self,
                chunk,
                more=index + 1 < len(chunks),
                do_yield=mode == "chunk-yield",
            )

    StreamTransport.write_many = instrumented_write_many

    if mode in ("cap-yield", "large-yield"):
        original_write_contiguous = WritePump._write_contiguous

        async def cooperative_write_contiguous(
            self: Any,
            transport: Any,
            write_many: Any,
            parts: list[bytes],
        ) -> None:
            item_count = len(parts)
            byte_count = sum(map(len, parts))
            await original_write_contiguous(self, transport, write_many, parts)
            backlog = not self.queue.empty()
            should_yield = (
                mode == "cap-yield" and item_count >= 256 and backlog
            ) or (
                mode == "large-yield"
                and byte_count >= large_yield_bytes
                and backlog
            )
            if should_yield:
                _TELEMETRY["cooperative_yields"] += 1
                _record_event(
                    {
                        "kind": "writer-yield",
                        "t": time.perf_counter(),
                        "items": item_count,
                        "bytes": byte_count,
                        "queued_messages": self.queue.qsize(),
                        "resident_messages": self.resident_messages,
                    }
                )
                await asyncio.sleep(0)

        WritePump._write_contiguous = cooperative_write_contiguous

    _TELEMETRY["_original_write_many_name"] = getattr(original_write_many, "__qualname__", "")


def _pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = q * (len(ordered) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _median_ratio(pairs: list[dict[str, Any]], key: str) -> float:
    ratios = []
    for pair in pairs:
        base = pair["base"][key]
        candidate = pair["candidate"][key]
        if base:
            ratios.append(candidate / base)
    return statistics.median(ratios) if ratios else 0.0


async def _worker(args: argparse.Namespace) -> dict[str, Any]:
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            raise RuntimeError(f"cannot pin worker to CPU {args.cpu}") from exc

    _install_mode(
        args.mode,
        chunk_bytes=args.chunk_bytes,
        large_yield_bytes=args.large_yield_bytes,
    )

    from mqttium.api import AsyncClient
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"writer-coop-{args.mode}-{os.getpid()}-{time.time_ns()}",
        max_outbound_bytes=args.max_outbound_bytes,
        max_outbound_messages=args.max_outbound_messages,
        max_outbound_inflight=args.inflight,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)

    flood_payload = b"f" * args.flood_bytes
    probe_payload = b"p" * args.probe_bytes
    suffix = f"{os.getpid()}/{time.time_ns()}"
    flood_topic = f"bench/writer-coop/flood/{suffix}"
    probe_topic = f"bench/writer-coop/probe/{suffix}"
    gate = asyncio.Event()
    stop_heartbeat = asyncio.Event()
    probes: list[dict[str, Any]] = []
    heartbeat_late_ms: list[float] = []

    async def heartbeat() -> None:
        if not args.heartbeat:
            return
        loop = asyncio.get_running_loop()
        interval = args.heartbeat_interval
        target = loop.time() + interval
        while not stop_heartbeat.is_set():
            await asyncio.sleep(max(0.0, target - loop.time()))
            now = loop.time()
            heartbeat_late_ms.append(max(0.0, (now - target) * 1000.0))
            target += interval

    async def flood() -> None:
        await gate.wait()
        for _ in range(args.flood_count):
            await client.publish(flood_topic, flood_payload, qos=0)

    async def probe_loop() -> None:
        await gate.wait()
        await asyncio.sleep(0)
        pump = client._write_pump
        for index in range(args.probes):
            started = time.perf_counter()
            before = {
                "queue": pump.queue.qsize(),
                "resident": pump.resident_messages,
                "queued_bytes": pump.queued_bytes,
            }
            receipt = await client.publish(probe_topic, probe_payload, qos=1)
            admitted = time.perf_counter()
            after_admit = {
                "queue": pump.queue.qsize(),
                "resident": pump.resident_messages,
                "queued_bytes": pump.queued_bytes,
            }
            await receipt.wait()
            finished = time.perf_counter()
            probes.append(
                {
                    "index": index,
                    "started": started,
                    "admitted": admitted,
                    "finished": finished,
                    "admission_ms": (admitted - started) * 1000.0,
                    "completion_ms": (finished - started) * 1000.0,
                    "before": before,
                    "after_admit": after_admit,
                }
            )

    heartbeat_task = asyncio.create_task(heartbeat())
    flood_task = asyncio.create_task(flood())
    probe_task = asyncio.create_task(probe_loop())
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    gate.set()
    try:
        await asyncio.wait_for(
            asyncio.gather(flood_task, probe_task),
            timeout=args.timeout,
        )
        await asyncio.wait_for(client._write_pump.join(), timeout=args.timeout)
    finally:
        stop_heartbeat.set()
        await heartbeat_task
    elapsed = time.perf_counter() - wall_started
    cpu_seconds = time.process_time() - cpu_started

    pump = client._write_pump
    stats = pump.stats()
    resident_after_join = pump.resident_messages
    queue_after_join = pump.queue.qsize()
    queued_bytes_after_join = pump.queued_bytes
    await client.disconnect()

    if resident_after_join != 0 or queue_after_join != 0 or queued_bytes_after_join != 0:
        raise AssertionError(
            "writer accounting leak after join: "
            f"resident={resident_after_join} queue={queue_after_join} bytes={queued_bytes_after_join}"
        )

    completion = [row["completion_ms"] for row in probes]
    admission = [row["admission_ms"] for row in probes]
    return {
        "mode": args.mode,
        "flood_bytes": args.flood_bytes,
        "probe_bytes": args.probe_bytes,
        "max_outbound_bytes": args.max_outbound_bytes,
        "elapsed_seconds": elapsed,
        "flood_rate": args.flood_count / max(elapsed, 1e-9),
        "cpu_seconds": cpu_seconds,
        "probe_p50_ms": _pct(completion, 0.50),
        "probe_p95_ms": _pct(completion, 0.95),
        "probe_p99_ms": _pct(completion, 0.99),
        "probe_p999_ms": _pct(completion, 0.999),
        "probe_max_ms": max(completion, default=0.0),
        "probe_admit_p95_ms": _pct(admission, 0.95),
        "heartbeat_p95_ms": _pct(heartbeat_late_ms, 0.95),
        "heartbeat_p99_ms": _pct(heartbeat_late_ms, 0.99),
        "heartbeat_max_ms": max(heartbeat_late_ms, default=0.0),
        "writer_batches": stats.batches,
        "writer_batched_items": stats.batched_items,
        "writer_batched_bytes": stats.batched_bytes,
        "writer_enqueue_suspensions": stats.enqueue_suspensions,
        "writer_eager_writes": stats.eager_writes,
        "telemetry": {k: v for k, v in _TELEMETRY.items() if not k.startswith("_")},
        "probes": probes,
    }


def _run_worker(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    *,
    variant: str,
    mode: str,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--mode",
        mode,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--flood-bytes",
        str(args.flood_bytes),
        "--probe-bytes",
        str(args.probe_bytes),
        "--flood-count",
        str(args.flood_count),
        "--probes",
        str(args.probes),
        "--inflight",
        str(args.inflight),
        "--max-outbound-bytes",
        str(args.max_outbound_bytes),
        "--max-outbound-messages",
        str(args.max_outbound_messages),
        "--chunk-bytes",
        str(args.chunk_bytes),
        "--large-yield-bytes",
        str(args.large_yield_bytes),
        "--timeout",
        str(args.timeout),
        "--heartbeat-interval",
        str(args.heartbeat_interval),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    if args.heartbeat:
        command.append("--heartbeat")
    completed = subprocess.run(
        command,
        env=env,
        capture_output=True,
        text=True,
        timeout=args.timeout + 30,
        check=False,
    )
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no output")[-6000:]
        raise RuntimeError(f"{variant}/{mode} worker failed rc={completed.returncode}: {diagnostic}")
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"{variant}/{mode} worker produced no JSON")
    return json.loads(lines[-1])


def _parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    root = args.root.resolve()
    pairs: list[dict[str, Any]] = []
    for index in range(args.repeat):
        order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
        measured: dict[str, Any] = {}
        for variant in order:
            mode = "base" if variant == "base" else args.mode
            measured[variant] = _run_worker(script, root, args, variant=variant, mode=mode)
        pairs.append({"order": list(order), **measured})

    ratio_keys = (
        "flood_rate",
        "cpu_seconds",
        "probe_p50_ms",
        "probe_p95_ms",
        "probe_p99_ms",
        "probe_p999_ms",
        "probe_max_ms",
        "probe_admit_p95_ms",
        "heartbeat_p95_ms",
        "heartbeat_p99_ms",
        "heartbeat_max_ms",
        "writer_batches",
        "writer_enqueue_suspensions",
    )
    result = {
        "mode": args.mode,
        "repeat": args.repeat,
        "root": str(root),
        "workload": {
            "flood_bytes": args.flood_bytes,
            "probe_bytes": args.probe_bytes,
            "flood_count": args.flood_count,
            "probes": args.probes,
            "inflight": args.inflight,
            "max_outbound_bytes": args.max_outbound_bytes,
            "max_outbound_messages": args.max_outbound_messages,
            "chunk_bytes": args.chunk_bytes,
            "large_yield_bytes": args.large_yield_bytes,
            "heartbeat": args.heartbeat,
        },
        "ratios": {key: _median_ratio(pairs, key) for key in ratio_keys},
        "pairs": pairs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"mode": args.mode, "workload": result["workload"], "ratios": result["ratios"]}))
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--mode", choices=_MODES, default="cap-yield")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--flood-bytes", type=int, default=32 * 1024)
    parser.add_argument("--probe-bytes", type=int, default=64)
    parser.add_argument("--flood-count", type=int, default=1500)
    parser.add_argument("--probes", type=int, default=200)
    parser.add_argument("--inflight", type=int, default=20)
    parser.add_argument("--max-outbound-bytes", type=int, default=64 << 20)
    parser.add_argument("--max-outbound-messages", type=int, default=100_000)
    parser.add_argument("--chunk-bytes", type=int, default=256 * 1024)
    parser.add_argument("--large-yield-bytes", type=int, default=1 << 20)
    parser.add_argument("--repeat", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--heartbeat", action="store_true")
    parser.add_argument("--heartbeat-interval", type=float, default=0.001)
    parser.add_argument("--output", type=Path, default=Path("/tmp/writer-cooperation.json"))
    args = parser.parse_args()
    if not args.worker and args.root is None:
        parser.error("--root is required")
    if args.repeat <= 0 or args.repeat % 2:
        parser.error("--repeat must be a positive even number")
    if args.chunk_bytes <= 0 or args.large_yield_bytes <= 0:
        parser.error("byte thresholds must be positive")
    return args


if __name__ == "__main__":
    parsed = _parse_args()
    if parsed.worker:
        print(json.dumps(asyncio.run(_worker(parsed))))
    else:
        raise SystemExit(_parent(parsed))
