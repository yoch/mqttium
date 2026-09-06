#!/usr/bin/env python3
"""Single-arm rate sweep that instruments the pacer, the writer and the latency mix.

`paired_open_loop.py` answers "is arm B slower than arm A at this rate". It cannot
answer "why does one arm change behaviour between two rates", because it reports
only aggregate offered rate and three percentiles. Two runs can report the same
average offered rate while presenting a different arrival process to MQTTium, the
write pump and the broker.

This probe runs one source root at a time and records, per publication, the
schedule deadline it was due at, the moment it was actually admitted, whether the
pacer was already behind (a catch-up send), the acknowledgement latency and the
subscriber-observed delivery latency. Everything is aggregated in the worker; no
per-message trace reaches stdout.

Diagnostic only: it changes no runtime and asserts no A/B claim.
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
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from paired_network import start_subscriber

# Fixed across every rate so the histograms are comparable.
DELIVERY_BINS_MS = (0.10, 0.125, 0.150, 0.175, 0.200, 0.250, 0.350)
PCTS = (10, 25, 50, 75, 90, 95, 99)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] * (1.0 - (rank - low)) + ordered[high] * (rank - low)


def spread(values: list[float]) -> dict[str, float]:
    if not values:
        return {f"p{p}": math.nan for p in PCTS} | {"mean": math.nan, "cv": math.nan}
    mean = statistics.fmean(values)
    cv = statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0
    out = {f"p{p}": percentile(values, p) for p in PCTS}
    out["mean"] = mean
    out["cv"] = cv
    return out


def histogram(values: list[float], edges: tuple[float, ...]) -> list[float]:
    """Fraction of samples per fixed bin, plus one overflow bin."""
    counts = [0] * (len(edges) + 1)
    for value in values:
        placed = False
        for index, edge in enumerate(edges):
            if value < edge:
                counts[index] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1
    total = len(values) or 1
    return [count / total for count in counts]


def pearson(xs: list[float], ys: list[float]) -> float:
    pairs = [(x, y) for x, y in zip(xs, ys, strict=True) if not (math.isnan(x) or math.isnan(y))]
    if len(pairs) < 3:
        return math.nan
    n = len(pairs)
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((x - mx) * (y - my) for x, y in pairs)
    dx = math.sqrt(sum((x - mx) ** 2 for x, _ in pairs))
    dy = math.sqrt(sum((y - my) ** 2 for _, y in pairs))
    return num / (dx * dy) if dx and dy else math.nan


@dataclass
class ProbeResult:
    rate: float
    count: int
    protocol: str
    window: int
    payload_bytes: int
    completion: str
    offered_rate: float
    completed_rate: float
    completion_ratio: float
    elapsed_s: float
    cpu_us_per_msg: float
    # pacer
    interval_ms: dict[str, float] = field(default_factory=dict)
    lateness_ms: dict[str, float] = field(default_factory=dict)
    lateness_max_ms: float = 0.0
    late_fraction: float = 0.0
    late_quarter_fraction: float = 0.0
    late_half_fraction: float = 0.0
    late_full_fraction: float = 0.0
    catchup_fraction: float = 0.0
    burst_count: int = 0
    burst_mean: float = 0.0
    burst_p50: float = 0.0
    burst_p95: float = 0.0
    burst_max: int = 0
    # latency
    ack_ms: dict[str, float] = field(default_factory=dict)
    delivery_ms: dict[str, float] = field(default_factory=dict)
    delivery_hist: list[float] = field(default_factory=list)
    # per-class delivery
    class_stats: dict[str, dict[str, float]] = field(default_factory=dict)
    # per-sequence correlation
    corr_lateness_delivery: float = math.nan
    corr_interval_delivery: float = math.nan
    corr_lateness_ack: float = math.nan
    # writer
    writer: dict[str, float] = field(default_factory=dict)
    # effect pump
    effects: dict[str, float] = field(default_factory=dict)


async def _run(args: argparse.Namespace, topic: str) -> ProbeResult:  # noqa: C901 -- one paced loop plus three completion modes, kept in one frame on purpose
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    count = args.count
    client = AsyncClient(
        client_id=f"regime-{os.getpid()}-{time.time_ns()}",
        protocol=(
            MQTTProtocolVersion.MQTTv5 if args.protocol == "5" else MQTTProtocolVersion.MQTTv311
        ),
        max_outbound_inflight=args.window,
        max_pending_outbound_messages=None,
        max_pending_outbound_bytes=None,
        reconnect=ReconnectPolicy(enabled=False),
    )

    ack_ms: list[float] = [math.nan] * count
    lateness_s: list[float] = [0.0] * count
    interval_s: list[float] = [math.nan] * count
    catchup: list[bool] = [False] * count
    receipts: list[Any] = []
    tasks: list[asyncio.Task[None]] = []

    async def observe(receipt: Any, seq: int, sent_ns: int) -> None:
        await receipt.wait()
        ack_ms[seq] = (time.monotonic_ns() - sent_ns) / 1_000_000

    completed = asyncio.Queue[tuple[int, int]]()
    pending_mid: dict[int, tuple[int, int]] = {}
    if args.completion == "callback":

        def on_publish(mid: int | None, *_unused: object) -> None:
            assert mid is not None
            entry = pending_mid.pop(mid, None)
            if entry is not None:
                completed.put_nowait((entry[0], time.monotonic_ns() - entry[1]))

        client.on_publish = on_publish

    await client.connect(args.host, args.port, timeout=args.timeout)
    loop = asyncio.get_running_loop()
    interval = 1.0 / args.rate
    payload_tail = bytes(max(args.payload_bytes - 32, 0))

    cpu0 = time.process_time()
    started = loop.time() + 0.05
    offered_started = 0.0
    prev_actual = 0.0
    for seq in range(count):
        deadline = started + seq * interval
        delay = deadline - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        else:
            catchup[seq] = True
        actual = loop.time()
        if seq == 0:
            offered_started = actual
        else:
            interval_s[seq] = actual - prev_actual
        prev_actual = actual
        lateness_s[seq] = actual - deadline
        sent_ns = time.monotonic_ns()
        header = f"{seq:016x}{sent_ns:016x}".encode("ascii")
        receipt = await client.publish(topic, header + payload_tail, qos=1)
        if args.completion == "receipt":
            tasks.append(loop.create_task(observe(receipt, seq, sent_ns)))
        elif args.completion == "callback":
            assert receipt.mid is not None
            pending_mid[receipt.mid] = (seq, sent_ns)
        else:  # late attachment: hold the receipts, await them after the offered phase
            receipts.append((receipt, seq, sent_ns))
    offered_elapsed = max(loop.time() - offered_started, 1e-9)

    if args.completion == "receipt":
        await asyncio.gather(*tasks)
    elif args.completion == "callback":
        for _ in range(count):
            seq, delta_ns = await completed.get()
            ack_ms[seq] = delta_ns / 1_000_000
    else:
        for receipt, seq, sent_ns in receipts:
            await receipt.wait()
            ack_ms[seq] = (time.monotonic_ns() - sent_ns) / 1_000_000
    completed_elapsed = max(loop.time() - offered_started, 1e-9)
    cpu_s = time.process_time() - cpu0

    snapshot = client.stats()
    writer, effects = snapshot.writer, snapshot.effects
    await client.disconnect()

    result = ProbeResult(
        rate=args.rate,
        count=count,
        protocol=args.protocol,
        window=args.window,
        payload_bytes=args.payload_bytes,
        completion=args.completion,
        offered_rate=count / offered_elapsed,
        completed_rate=count / completed_elapsed,
        completion_ratio=sum(not math.isnan(v) for v in ack_ms) / count,
        elapsed_s=completed_elapsed,
        cpu_us_per_msg=cpu_s * 1_000_000 / count,
    )

    intervals_ms = [v * 1000 for v in interval_s[1:] if not math.isnan(v)]
    lateness_ms_all = [v * 1000 for v in lateness_s]
    result.interval_ms = spread(intervals_ms)
    result.lateness_ms = spread(lateness_ms_all)
    result.lateness_max_ms = max(lateness_ms_all)
    interval_ms_target = interval * 1000
    result.late_fraction = sum(v > 0 for v in lateness_ms_all) / count
    result.late_quarter_fraction = (
        sum(v > 0.25 * interval_ms_target for v in lateness_ms_all) / count
    )
    result.late_half_fraction = sum(v > 0.50 * interval_ms_target for v in lateness_ms_all) / count
    result.late_full_fraction = sum(v > 1.00 * interval_ms_target for v in lateness_ms_all) / count

    bursts: list[int] = []
    run = 0
    for flag in catchup:
        if flag:
            run += 1
        elif run:
            bursts.append(run)
            run = 0
    if run:
        bursts.append(run)
    result.catchup_fraction = sum(catchup) / count
    result.burst_count = len(bursts)
    result.burst_mean = statistics.fmean(bursts) if bursts else 0.0
    result.burst_p50 = percentile([float(b) for b in bursts], 50) if bursts else 0.0
    result.burst_p95 = percentile([float(b) for b in bursts], 95) if bursts else 0.0
    result.burst_max = max(bursts) if bursts else 0

    result.writer = {
        "batches_per_msg": writer.batches / count,
        "items_per_batch": writer.batched_items / writer.batches if writer.batches else 0.0,
        "batched_bytes_per_msg": writer.batched_bytes / count,
        "eager_per_msg": writer.eager_writes / count,
        "eager_bytes_per_msg": writer.eager_bytes / count,
        "segmented_writes": float(writer.segmented_writes),
        "enqueue_suspensions_per_msg": writer.enqueue_suspensions / count,
        "high_water_messages": float(writer.high_water_messages),
        "high_water_bytes": float(writer.high_water_bytes),
    }
    result.effects = {
        "batches_per_msg": effects.batches / count,
        "multi_batches_per_msg": effects.multi_effect_batches / count,
        "multi_share": effects.multi_effect_batches / effects.batches if effects.batches else 0.0,
        "enqueued_per_msg": effects.enqueued / count,
        "inline_per_msg": effects.inline_effects / count,
        "apply_suspensions": float(effects.apply_suspensions),
        "pending_high_water": float(effects.pending_high_water),
    }
    # Per-sequence arrays travel on the instance, not in the dataclass fields:
    # the worker needs them to join against subscriber sequences, and `asdict`
    # must not carry 15k-element lists into the parent.
    result.__dict__["_lateness_seq"] = lateness_ms_all
    result.__dict__["_catchup_seq"] = catchup
    result.__dict__["_ack_seq"] = ack_ms
    result.__dict__["_interval_seq"] = [
        v * 1000 if not math.isnan(v) else math.nan for v in interval_s
    ]
    return result


def worker(args: argparse.Namespace) -> None:
    topic = f"bench/regime/{os.getpid()}/{time.time_ns()}"
    subscriber = start_subscriber(args.host, args.port, topic, args.count)
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            subscriber.abort()
            raise RuntimeError(f"cannot pin probe worker to CPU {args.cpu}") from exc
    try:
        result = asyncio.run(_run(args, topic))
    except BaseException:
        subscriber.abort()
        raise
    delivery_latencies, sequences = subscriber.finish(args.timeout + 30)
    if sorted(sequences) != list(range(args.count)):
        raise RuntimeError(f"subscriber sequence mismatch: {len(sequences)}/{args.count}")

    by_seq = [math.nan] * args.count
    for seq, latency in zip(sequences, delivery_latencies, strict=True):
        by_seq[seq] = latency

    # Recover the per-sequence pacer classification the worker computed inline.
    interval_target_ms = 1000.0 / args.rate
    lateness = result.__dict__.pop("_lateness_seq")
    catchup = result.__dict__.pop("_catchup_seq")
    ack = result.__dict__.pop("_ack_seq")
    intervals = result.__dict__.pop("_interval_seq")

    result.ack_ms = spread([v for v in ack if not math.isnan(v)])
    result.delivery_ms = spread([v for v in by_seq if not math.isnan(v)])
    result.delivery_hist = histogram([v for v in by_seq if not math.isnan(v)], DELIVERY_BINS_MS)
    result.corr_lateness_delivery = pearson(lateness, by_seq)
    result.corr_interval_delivery = pearson(intervals, by_seq)
    result.corr_lateness_ack = pearson(lateness, ack)

    classes: dict[str, list[float]] = {"on_time": [], "late": [], "catchup": []}
    ack_classes: dict[str, list[float]] = {"on_time": [], "late": [], "catchup": []}
    for index in range(args.count):
        if catchup[index]:
            key = "catchup"
        elif lateness[index] > 0.25 * interval_target_ms:
            key = "late"
        else:
            key = "on_time"
        if not math.isnan(by_seq[index]):
            classes[key].append(by_seq[index])
        if not math.isnan(ack[index]):
            ack_classes[key].append(ack[index])
    result.class_stats = {
        key: {
            "share": len(classes[key]) / args.count,
            "delivery_p50": percentile(classes[key], 50),
            "delivery_p95": percentile(classes[key], 95),
            "ack_p50": percentile(ack_classes[key], 50),
            "ack_p95": percentile(ack_classes[key], 95),
        }
        for key in classes
    }
    sys.stdout.write(json.dumps(asdict(result)))


def _spawn(root: Path, args: argparse.Namespace, rate: float, count: int) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocol",
        args.protocol,
        "--payload-bytes",
        str(args.payload_bytes),
        "--window",
        str(args.window),
        "--completion",
        args.completion,
        "--rate",
        str(rate),
        "--count",
        str(count),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        command += ["--cpu", str(args.cpu)]
    proc = subprocess.run(
        command, env=env, capture_output=True, text=True, timeout=args.timeout + 240
    )
    if proc.returncode != 0:
        raise SystemExit(f"probe worker failed at {rate} msg/s:\n{proc.stderr[-3000:]}")
    return json.loads(proc.stdout)


def _median_of(runs: list[dict[str, Any]], path: str) -> float:
    values = []
    for run in runs:
        node: Any = run
        for part in path.split("."):
            node = node[part]
        if isinstance(node, (int, float)) and not (isinstance(node, float) and math.isnan(node)):
            values.append(float(node))
    return statistics.median(values) if values else math.nan


def parent(args: argparse.Namespace) -> None:
    root = args.root.resolve()
    rates = [float(v) for v in args.rates.split(",")]
    payload: dict[str, Any] = {
        "root": str(root),
        "protocol": args.protocol,
        "window": args.window,
        "payload_bytes": args.payload_bytes,
        "completion": args.completion,
        "repeat": args.repeat,
        "rates": rates,
        "cells": [],
    }
    header = (
        f"{'rate':>6s} {'offered':>8s} {'d p25':>7s} {'d p50':>7s} {'d p75':>7s} {'d p95':>7s} "
        f"{'ack p50':>8s} {'cpu/msg':>8s} {'lag p50':>8s} {'lag p95':>8s} "
        f"{'catch%':>7s} {'brst95':>7s} {'eagr/m':>7s} {'itm/bat':>8s} {'wbat/m':>7s} {'enq/m':>6s}"
    )
    print(header)
    print("-" * len(header))
    for rate in rates:
        count = max(int(rate * args.sample_seconds), 500)
        runs = [_spawn(root, args, rate, count) for _ in range(args.repeat)]
        cell = {
            "rate": rate,
            "count": count,
            "offered_rate": _median_of(runs, "offered_rate"),
            "completion_ratio": _median_of(runs, "completion_ratio"),
            "cpu_us_per_msg": _median_of(runs, "cpu_us_per_msg"),
            "delivery": {f"p{p}": _median_of(runs, f"delivery_ms.p{p}") for p in PCTS},
            "ack": {f"p{p}": _median_of(runs, f"ack_ms.p{p}") for p in PCTS},
            "lateness": {f"p{p}": _median_of(runs, f"lateness_ms.p{p}") for p in PCTS},
            "interval": {f"p{p}": _median_of(runs, f"interval_ms.p{p}") for p in PCTS}
            | {"cv": _median_of(runs, "interval_ms.cv")},
            "late_fraction": _median_of(runs, "late_fraction"),
            "late_quarter_fraction": _median_of(runs, "late_quarter_fraction"),
            "late_half_fraction": _median_of(runs, "late_half_fraction"),
            "late_full_fraction": _median_of(runs, "late_full_fraction"),
            "catchup_fraction": _median_of(runs, "catchup_fraction"),
            "burst_mean": _median_of(runs, "burst_mean"),
            "burst_p95": _median_of(runs, "burst_p95"),
            "burst_max": _median_of(runs, "burst_max"),
            "corr_lateness_delivery": _median_of(runs, "corr_lateness_delivery"),
            "corr_lateness_ack": _median_of(runs, "corr_lateness_ack"),
            "writer": {k: _median_of(runs, f"writer.{k}") for k in runs[0]["writer"]},
            "effects": {k: _median_of(runs, f"effects.{k}") for k in runs[0]["effects"]},
            "delivery_hist": [
                statistics.median([run["delivery_hist"][i] for run in runs])
                for i in range(len(runs[0]["delivery_hist"]))
            ],
            "class_stats": {
                key: {
                    metric: _median_of(runs, f"class_stats.{key}.{metric}")
                    for metric in runs[0]["class_stats"][key]
                }
                for key in runs[0]["class_stats"]
            },
            "runs": runs,
        }
        payload["cells"].append(cell)
        print(
            f"{rate:6.0f} {cell['offered_rate']:8.1f} {cell['delivery']['p25']:7.3f} "
            f"{cell['delivery']['p50']:7.3f} {cell['delivery']['p75']:7.3f} {cell['delivery']['p95']:7.3f} "
            f"{cell['ack']['p50']:8.3f} {cell['cpu_us_per_msg']:8.1f} "
            f"{cell['lateness']['p50']:8.4f} {cell['lateness']['p95']:8.4f} "
            f"{cell['catchup_fraction'] * 100:6.1f}% {cell['burst_p95']:7.1f} "
            f"{cell['writer']['eager_per_msg']:7.3f} {cell['writer']['items_per_batch']:8.2f} "
            f"{cell['writer']['batches_per_msg']:7.3f} {cell['effects']['enqueued_per_msg']:6.3f}"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")
    print(f"\nwritten: {args.output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=21883)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--window", type=int, default=32)
    parser.add_argument("--completion", choices=("receipt", "callback", "late"), default="receipt")
    parser.add_argument("--rate", type=float, default=0.0)
    parser.add_argument(
        "--rates", default="3000,3250,3500,3750,4000,4250,4500,4750,5000,5250,5500,6000"
    )
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--sample-seconds", type=float, default=3.0)
    parser.add_argument("--repeat", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output", type=Path, default=Path("/tmp/rate-regime.json"))
    args = parser.parse_args()
    if args.worker and (args.rate <= 0 or args.count <= 0):
        parser.error("--worker needs --rate and --count")
    if not args.worker and args.root is None:
        parser.error("--root is required")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        parent(arguments)
