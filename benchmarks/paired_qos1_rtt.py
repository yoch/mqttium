#!/usr/bin/env python3
"""Paired QoS 1 callback-response and nowait-microbatch diagnostics.

The callback suite measures the writer scheduling policy without a broker or
socket: one persistent client receives a paced QoS 1 PUBLISH, auto-ACKs it,
and synchronously publishes a QoS 1 reply from ``on_message``.  The measured
interval starts immediately before the callback returns and ends when the
reply reaches the transport.  A fake broker schedules reply PUBACKs with
``call_soon`` so receipt completion cannot re-enter the writer synchronously.

The parent runs fresh source-isolated workers in ABBA order.  Deterministic
writer-policy counters are collected once per root.  The microbatch suite is
separate by design: it must never be stacked with a callback-response policy
when attributing a result.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


def percentile(values: list[float], pct: float) -> float:
    """Return a linearly interpolated percentile for a non-empty sample."""
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return ordered[low] * (1.0 - fraction) + ordered[high] * fraction


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


@dataclass(slots=True)
class CallbackResult:
    samples: int
    elapsed_seconds: float
    operations_per_second: float
    latency_p50_ns: float
    latency_p95_ns: float
    latency_p99_ns: float
    latency_mean_ns: float
    eager_replies: int
    queued_replies: int
    eager_acks: int
    queued_acks: int


@dataclass(slots=True)
class MicrobatchResult:
    burst: int
    payload_bytes: int
    trials: int
    latency_p50_ns: float
    latency_p95_ns: float
    latency_p99_ns: float
    operations_per_second: float
    median_eager_writes: float
    median_queued_after_submit: float
    median_batches_after_submit: float
    median_batched_items_after_submit: float


class _FakeBroker:
    """Packet-aware in-process transport with observable write paths."""

    def __init__(self) -> None:
        from mqttium.codec.buffer import IncrementalDecoder
        from mqttium.enums import PacketType
        from mqttium.packets import PubAckPacket, PublishPacket, encode_frame

        self._decoder = IncrementalDecoder()
        self._PacketType = PacketType
        self._PubAckPacket = PubAckPacket
        self._PublishPacket = PublishPacket
        self._encode_frame = encode_frame
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closed = False
        self.reply_started_ns: dict[int, int] = {}
        self.reply_seen: asyncio.Queue[tuple[int, int, str]] = asyncio.Queue()
        self.ack_paths: list[str] = []

    def _feed_client_bytes(self, data: bytes, path: str) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            packet_type = raw.packet_type
            if packet_type is self._PacketType.CONNECT:
                self._rx.put_nowait(self._encode_frame(self._PacketType.CONNACK, 0, b"\x00\x00"))
                continue
            if packet_type is self._PacketType.PUBACK:
                self.ack_paths.append(path)
                continue
            if packet_type is not self._PacketType.PUBLISH:
                continue
            publish = self._PublishPacket.decode(raw.flags, raw.remaining)
            if publish.topic != "bench/reply":
                continue
            sequence = int.from_bytes(publish.payload, "big")
            started = self.reply_started_ns.pop(sequence, None)
            if started is not None:
                self.reply_seen.put_nowait((sequence, time.perf_counter_ns() - started, path))
            if publish.mid is not None:
                ack = self._PubAckPacket(mid=publish.mid).encode()
                asyncio.get_running_loop().call_soon(self._rx.put_nowait, ack)

    def write_nowait(self, data: bytes) -> bool:
        self._feed_client_bytes(data, "eager")
        return True

    async def write(self, data: bytes) -> None:
        self._feed_client_bytes(data, "queued")

    async def write_many(self, parts: list[bytes]) -> None:
        for part in parts:
            self._feed_client_bytes(part, "queued")

    async def read(self, _n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closed = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closed

    def push_qos1(self, sequence: int) -> None:
        from mqttium.enums import QoS
        from mqttium.packets import PublishPacket

        packet = PublishPacket(
            topic="bench/request",
            payload=sequence.to_bytes(8, "big"),
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=sequence + 1,
        )
        self._rx.put_nowait(packet.encode())


async def _callback_sample(*, warmup: int, count: int, timeout: float) -> CallbackResult:
    from mqttium.api import AsyncClient

    broker = _FakeBroker()

    async def factory(_host: str, _port: int, *, ssl: object = None) -> _FakeBroker:
        del ssl
        return broker

    client = AsyncClient(
        client_id=f"paired-qos1-rtt-{os.getpid()}",
        message_delivery="callback",
        max_outbound_inflight=1,
        max_pending_outbound_messages=128,
    )
    client._transport_factory = factory  # type: ignore[assignment]
    receipts: asyncio.Queue[Any] = asyncio.Queue()

    def on_message(message: Any) -> None:
        sequence = int.from_bytes(message.payload, "big")
        receipt = client.publish_nowait("bench/reply", message.payload, qos=1)
        # Nested effect collection has completed, but the callback has not yet
        # returned to the owning EffectPump.  This is the scheduling interval
        # the candidate policies are intended to shorten.
        broker.reply_started_ns[sequence] = time.perf_counter_ns()
        receipts.put_nowait(receipt)

    client.on_message = on_message
    await client.connect("in-process", 1883, timeout=timeout)
    await asyncio.sleep(0)

    latencies: list[float] = []
    reply_paths: list[str] = []
    ack_start = len(broker.ack_paths)
    started = 0.0
    try:
        async with asyncio.timeout(timeout):
            for sequence in range(warmup + count):
                if sequence == warmup:
                    started = time.perf_counter()
                    ack_start = len(broker.ack_paths)
                broker.push_qos1(sequence)
                receipt = await receipts.get()
                seen_sequence, latency_ns, path = await broker.reply_seen.get()
                if seen_sequence != sequence:
                    raise AssertionError(
                        f"reply order changed: expected {sequence}, got {seen_sequence}"
                    )
                await receipt.wait()
                if sequence >= warmup:
                    latencies.append(float(latency_ns))
                    reply_paths.append(path)
    finally:
        elapsed = time.perf_counter() - started if started else 0.0
        await client.disconnect()

    if len(latencies) != count:
        raise AssertionError(f"incomplete callback sample: {len(latencies)}/{count}")
    measured_ack_paths = broker.ack_paths[ack_start:]
    return CallbackResult(
        samples=count,
        elapsed_seconds=elapsed,
        operations_per_second=count / max(elapsed, 1e-9),
        latency_p50_ns=percentile(latencies, 50),
        latency_p95_ns=percentile(latencies, 95),
        latency_p99_ns=percentile(latencies, 99),
        latency_mean_ns=statistics.fmean(latencies),
        eager_replies=reply_paths.count("eager"),
        queued_replies=reply_paths.count("queued"),
        eager_acks=measured_ack_paths.count("eager"),
        queued_acks=measured_ack_paths.count("queued"),
    )


class _CountingTransport:
    def __init__(self) -> None:
        self.writes: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.writes.append(data)
        return True


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


async def _writer_policy_counters() -> dict[str, object]:
    """Collect deterministic eager/queue counts without scheduling a task."""
    from mqttium.api._writer import WritePump

    def new_pump(armed: bool) -> tuple[Any, _CountingTransport]:
        transport = _CountingTransport()
        pump = WritePump(max_bytes=1 << 20, max_messages=256, on_failure=_no_failure)
        pump._write_nowait = transport.write_nowait
        pump._eager_armed = armed
        # Experimental two-permit candidates expose this internal bit.  Set it
        # to the same initial condition without making the harness depend on it.
        if hasattr(pump, "_ack_eager_armed"):
            pump._ack_eager_armed = armed
        return pump, transport

    qos0_pump, qos0_transport = new_pump(True)
    for _ in range(16):
        if not qos0_pump.try_enqueue(b"\x30\x00"):
            raise AssertionError("QoS 0 invariant probe was refused")
    qos0 = {
        "burst": 16,
        "eager": len(qos0_transport.writes),
        "queued": qos0_pump.queue.qsize(),
    }
    qos0_pump.discard()

    ack_bursts: list[dict[str, int | bool]] = []
    for armed in (True, False):
        for burst in (1, 16, 64):
            pump, transport = new_pump(armed)
            for mid in range(1, burst + 1):
                ack = b"\x40\x02" + mid.to_bytes(2, "big")
                if not pump.try_enqueue(ack):
                    raise AssertionError("ACK invariant probe was refused")
            eager_before_data = len(transport.writes)
            queued_after_acks = pump.queue.qsize()
            if not pump.try_enqueue(b"\x30\x00"):
                raise AssertionError("post-ACK data probe was refused")
            ack_bursts.append(
                {
                    "initially_armed": armed,
                    "burst": burst,
                    "eager_acks": eager_before_data,
                    "queued_after_acks": queued_after_acks,
                    "data_eager": len(transport.writes) - eager_before_data,
                    "queued_after_data": pump.queue.qsize(),
                }
            )
            pump.discard()
    return {"qos0_burst": qos0, "ack_bursts": ack_bursts}


async def _microbatch_sample(
    *, burst: int, payload_bytes: int, trials: int, timeout: float
) -> MicrobatchResult:
    from mqttium.api import AsyncClient

    latencies: list[float] = []
    eager_counts: list[float] = []
    queued_counts: list[float] = []
    batch_counts: list[float] = []
    batched_item_counts: list[float] = []
    operations = burst * trials
    overall_started = time.perf_counter()
    for trial in range(trials):
        broker = _FakeBroker()

        async def factory(
            _host: str,
            _port: int,
            *,
            ssl: object = None,
            _broker: _FakeBroker = broker,
        ) -> _FakeBroker:
            del ssl
            return _broker

        client = AsyncClient(
            client_id=f"microbatch-{os.getpid()}-{trial}",
            max_outbound_inflight=max(20, burst),
            max_pending_outbound_messages=burst + 32,
        )
        client._transport_factory = factory  # type: ignore[assignment]
        await client.connect("in-process", 1883, timeout=timeout)
        await asyncio.sleep(0)
        pump = client._write_pump
        eager_start = pump.eager_writes
        batches_start = pump.batches
        batched_items_start = pump.batched_items
        receipts = []
        payload = b"x" * payload_bytes
        started = time.perf_counter_ns()
        try:
            for _ in range(burst):
                receipts.append(client.publish_nowait("bench/reply", payload, qos=1))
            eager_counts.append(float(pump.eager_writes - eager_start))
            queued_counts.append(float(pump.queue.qsize()))
            batch_counts.append(float(pump.batches - batches_start))
            batched_item_counts.append(float(pump.batched_items - batched_items_start))
            async with asyncio.timeout(timeout):
                await asyncio.gather(*(receipt.wait() for receipt in receipts))
            latencies.append(float(time.perf_counter_ns() - started))
        finally:
            await client.disconnect()
    elapsed = time.perf_counter() - overall_started
    return MicrobatchResult(
        burst=burst,
        payload_bytes=payload_bytes,
        trials=trials,
        latency_p50_ns=percentile(latencies, 50),
        latency_p95_ns=percentile(latencies, 95),
        latency_p99_ns=percentile(latencies, 99),
        operations_per_second=operations / max(elapsed, 1e-9),
        median_eager_writes=statistics.median(eager_counts),
        median_queued_after_submit=statistics.median(queued_counts),
        median_batches_after_submit=statistics.median(batch_counts),
        median_batched_items_after_submit=statistics.median(batched_item_counts),
    )


def _pin(cpu: int | None) -> None:
    if cpu is None:
        return
    try:
        os.sched_setaffinity(0, {cpu})
    except (AttributeError, OSError) as exc:
        raise RuntimeError(f"cannot pin diagnostic worker to CPU {cpu}") from exc


def _worker(args: argparse.Namespace) -> None:
    _pin(args.cpu)
    if args.cell == "callback":
        result: object = asdict(
            asyncio.run(
                _callback_sample(warmup=args.warmup, count=args.count, timeout=args.timeout)
            )
        )
    elif args.cell == "policy":
        result = asyncio.run(_writer_policy_counters())
    else:
        result = asdict(
            asyncio.run(
                _microbatch_sample(
                    burst=args.burst,
                    payload_bytes=args.payload_bytes,
                    trials=args.trials,
                    timeout=args.timeout,
                )
            )
        )
    print(json.dumps(result, allow_nan=False))


def _run_worker(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    *,
    cell: str,
    burst: int | None = None,
    payload_bytes: int | None = None,
) -> dict[str, Any]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--cell",
        cell,
        "--warmup",
        str(args.warmup),
        "--count",
        str(args.count),
        "--trials",
        str(args.trials),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    if burst is not None:
        command.extend(("--burst", str(burst)))
    if payload_bytes is not None:
        command.extend(("--payload-bytes", str(payload_bytes)))
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        timeout=args.timeout + 30.0,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def assess_callback_pairs(
    pairs: list[dict[str, object]], *, aa_control: bool, max_cv: float, max_aa_drift: float
) -> tuple[dict[str, object], list[str]]:
    """Summarize callback pairs and return statistical invalidations."""
    fields = ("latency_p50_ns", "latency_p95_ns", "latency_p99_ns")
    summary: dict[str, object] = {}
    invalidations: list[str] = []
    for field in fields:
        base = [float(pair["base"][field]) for pair in pairs]  # type: ignore[index]
        candidate = [float(pair["candidate"][field]) for pair in pairs]  # type: ignore[index]
        ratios = [right / left for left, right in zip(base, candidate, strict=True)]
        base_cv = _cv(base)
        candidate_cv = _cv(candidate)
        median_ratio = statistics.median(ratios)
        summary[field] = {
            "median_candidate_over_base": median_ratio,
            "base_cv": base_cv,
            "candidate_cv": candidate_cv,
            "min_candidate_over_base": min(ratios),
            "max_candidate_over_base": max(ratios),
        }
        # Tail percentiles remain decision outputs, but gating their across-run
        # CV would reject sound runs because a small number of scheduler
        # outliers legitimately moves p95/p99.  The paired p50 and fixed-work
        # rate are the repeatability controls.
        if field == "latency_p50_ns":
            if base_cv > max_cv:
                invalidations.append(f"{field}: baseline CV {base_cv:.2%}")
            if candidate_cv > max_cv:
                invalidations.append(f"{field}: candidate CV {candidate_cv:.2%}")
            if aa_control and abs(median_ratio - 1.0) > max_aa_drift:
                invalidations.append(
                    f"{field}: A/A ratio {median_ratio:.4f} outside 1+/-{max_aa_drift:.2%}"
                )

    base_rates = [float(pair["base"]["operations_per_second"]) for pair in pairs]  # type: ignore[index]
    candidate_rates = [
        float(pair["candidate"]["operations_per_second"])
        for pair in pairs  # type: ignore[index]
    ]
    rate_ratios = [right / left for left, right in zip(base_rates, candidate_rates, strict=True)]
    base_rate_cv = _cv(base_rates)
    candidate_rate_cv = _cv(candidate_rates)
    median_rate_ratio = statistics.median(rate_ratios)
    summary["operations_per_second"] = {
        "median_candidate_over_base": median_rate_ratio,
        "base_cv": base_rate_cv,
        "candidate_cv": candidate_rate_cv,
        "min_candidate_over_base": min(rate_ratios),
        "max_candidate_over_base": max(rate_ratios),
    }
    if base_rate_cv > max_cv:
        invalidations.append(f"operations_per_second: baseline CV {base_rate_cv:.2%}")
    if candidate_rate_cv > max_cv:
        invalidations.append(f"operations_per_second: candidate CV {candidate_rate_cv:.2%}")
    if aa_control and abs(median_rate_ratio - 1.0) > max_aa_drift:
        invalidations.append(
            "operations_per_second: "
            f"A/A ratio {median_rate_ratio:.4f} outside 1+/-{max_aa_drift:.2%}"
        )
    return summary, invalidations


def _callback_parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    policy = {
        variant: _run_worker(script, root, args, cell="policy") for variant, root in roots.items()
    }
    base_qos0 = policy["base"]["qos0_burst"]
    candidate_qos0 = policy["candidate"]["qos0_burst"]
    invariant_failures = []
    for label, counters in (("base", base_qos0), ("candidate", candidate_qos0)):
        if counters != {"burst": 16, "eager": 1, "queued": 15}:
            invariant_failures.append(f"{label}: QoS0 burst counters {counters!r}")

    pairs: list[dict[str, object]] = []
    for index in range(args.repeat):
        order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
        measured = {
            variant: _run_worker(script, roots[variant], args, cell="callback") for variant in order
        }
        pairs.append(
            {
                "order": list(order),
                "base": measured["base"],
                "candidate": measured["candidate"],
            }
        )
    summary, invalidations = assess_callback_pairs(
        pairs,
        aa_control=args.aa_control,
        max_cv=args.max_cv,
        max_aa_drift=args.max_aa_drift,
    )
    invalidations.extend(invariant_failures)
    payload = {
        "suite": "callback",
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "repeat": args.repeat,
        "warmup": args.warmup,
        "count": args.count,
        "cpu": args.cpu,
        "aa_control": args.aa_control,
        "policy_counters": policy,
        "summary": summary,
        "invalidations": invalidations,
        "status": "invalid" if invalidations else "valid",
        "pairs": pairs,
    }
    _write_output(args.output, payload)
    for field, values in summary.items():
        assert isinstance(values, dict)
        print(
            f"{field}: candidate/base={values['median_candidate_over_base']:.4f} "
            f"CV={values['base_cv']:.2%}/{values['candidate_cv']:.2%}"
        )
    if invalidations:
        for reason in invalidations:
            print(f"INVALID: {reason}", file=sys.stderr)
        return 2
    return 0


def _microbatch_parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    cells = ((1, 64), (5, 16 * 1024), (17, 64), (64, 256))
    output_cells: list[dict[str, object]] = []
    for burst, payload_bytes in cells:
        pairs: list[dict[str, object]] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured = {
                variant: _run_worker(
                    script,
                    roots[variant],
                    args,
                    cell="microbatch",
                    burst=burst,
                    payload_bytes=payload_bytes,
                )
                for variant in order
            }
            pairs.append(
                {
                    "order": list(order),
                    "base": measured["base"],
                    "candidate": measured["candidate"],
                    "latency_candidate_over_base": (
                        measured["candidate"]["latency_p50_ns"] / measured["base"]["latency_p50_ns"]
                    ),
                    "rate_candidate_over_base": (
                        measured["candidate"]["operations_per_second"]
                        / measured["base"]["operations_per_second"]
                    ),
                }
            )
        output_cells.append(
            {
                "burst": burst,
                "payload_bytes": payload_bytes,
                "median_latency_candidate_over_base": statistics.median(
                    float(pair["latency_candidate_over_base"]) for pair in pairs
                ),
                "median_rate_candidate_over_base": statistics.median(
                    float(pair["rate_candidate_over_base"]) for pair in pairs
                ),
                "pairs": pairs,
            }
        )
    payload = {
        "suite": "microbatch",
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "repeat": args.repeat,
        "trials": args.trials,
        "cpu": args.cpu,
        "status": "diagnostic",
        "cells": output_cells,
    }
    _write_output(args.output, payload)
    for cell in output_cells:
        print(
            f"burst={cell['burst']} payload={cell['payload_bytes']}: "
            f"latency candidate/base={cell['median_latency_candidate_over_base']:.4f} "
            f"rate candidate/base={cell['median_rate_candidate_over_base']:.4f}"
        )
    return 0


def _write_output(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--cell", choices=("callback", "policy", "microbatch"))
    parser.add_argument("--suite", choices=("callback", "microbatch"), default="callback")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--repeat", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=1_000)
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--trials", type=int, default=40)
    parser.add_argument("--burst", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--aa-control", action="store_true")
    parser.add_argument("--max-cv", type=float, default=0.05)
    parser.add_argument("--max-aa-drift", type=float, default=0.05)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-qos1-rtt.json"))
    args = parser.parse_args()
    if args.worker and args.cell is None:
        parser.error("--cell is required with --worker")
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    for name in ("repeat", "count", "trials", "burst", "payload_bytes"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.warmup < 0:
        parser.error("--warmup must be non-negative")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        _worker(arguments)
        raise SystemExit(0)
    if arguments.suite == "callback":
        raise SystemExit(_callback_parent(arguments))
    raise SystemExit(_microbatch_parent(arguments))
