#!/usr/bin/env python3
"""Paired probe for a two-message coalesced callback-inline candidate.

This is experiment-only evidence. It measures the exact batch size that the
candidate targets and a larger batch as a negative control. Runtime code is
patched only in the candidate checkout by the workflow.
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
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return ordered[low] * (1.0 - fraction) + ordered[high] * fraction


def cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


@dataclass(slots=True)
class Result:
    batch_size: int
    messages: int
    elapsed_s: float
    operations_per_second: float
    callback_p50_ns: float
    callback_p95_ns: float
    first_callback_p50_ns: float
    second_callback_p50_ns: float
    batch_rtt_p50_ns: float
    batch_rtt_p95_ns: float
    eager_writes: int
    writer_batches: int
    effect_suspensions: int


class Broker:
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

    def _feed_client_bytes(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is self._PacketType.CONNECT:
                self._rx.put_nowait(self._encode_frame(self._PacketType.CONNACK, 0, b"\x00\x00"))
                continue
            if raw.packet_type is not self._PacketType.PUBLISH:
                continue
            publish = self._PublishPacket.decode(raw.flags, raw.remaining)
            if publish.topic != "bench/reply" or publish.mid is None:
                continue
            ack = self._PubAckPacket(mid=publish.mid).encode()
            asyncio.get_running_loop().call_soon(self._rx.put_nowait, ack)

    def write_nowait(self, data: bytes) -> bool:
        self._feed_client_bytes(data)
        return True

    async def write(self, data: bytes) -> None:
        self._feed_client_bytes(data)

    async def write_many(self, parts: list[bytes]) -> None:
        for part in parts:
            self._feed_client_bytes(part)

    async def read(self, _n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closed = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closed

    def push_batch(self, start: int, count: int) -> None:
        from mqttium.enums import QoS
        from mqttium.packets import PublishPacket

        parts = []
        for sequence in range(start, start + count):
            parts.append(
                PublishPacket(
                    topic="bench/request",
                    payload=sequence.to_bytes(8, "big"),
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    dup=False,
                    mid=(sequence % 65535) + 1,
                ).encode()
            )
        # One read() item containing N complete packets guarantees decoder
        # coalescence without relying on scheduler timing.
        self._rx.put_nowait(b"".join(parts))


async def sample(*, batch_size: int, batches: int, warmup_batches: int, timeout: float) -> Result:
    from mqttium.api import AsyncClient

    broker = Broker()

    async def factory(_host: str, _port: int, *, ssl: object = None) -> Broker:
        del ssl
        return broker

    client = AsyncClient(
        client_id=f"t3-coalesced-{os.getpid()}",
        message_delivery="callback",
        max_outbound_inflight=max(20, batch_size * 2),
        max_pending_outbound_messages=4096,
        max_pending_callbacks=4096,
    )
    client._transport_factory = factory  # type: ignore[assignment]
    receipts: asyncio.Queue[Any] = asyncio.Queue()
    sent_ns: dict[int, int] = {}
    callback_latencies: list[float] = []
    first_latencies: list[float] = []
    second_latencies: list[float] = []

    def on_message(message: Any) -> None:
        sequence = int.from_bytes(message.payload, "big")
        latency = float(time.perf_counter_ns() - sent_ns[sequence])
        callback_latencies.append(latency)
        if sequence % batch_size == 0:
            first_latencies.append(latency)
        elif sequence % batch_size == 1:
            second_latencies.append(latency)
        # Deliberately re-enter publish_nowait(): this is the important #402
        # shape, while the callback itself remains a plain sync callback -> None.
        receipts.put_nowait(client.publish_nowait("bench/reply", message.payload, qos=1))

    client.on_message = on_message
    await client.connect("in-process", 1883, timeout=timeout)
    await asyncio.sleep(0)

    sequence = 0
    batch_rtts: list[float] = []
    start_stats = client.stats()
    started = 0.0
    try:
        async with asyncio.timeout(timeout):
            for batch_index in range(warmup_batches + batches):
                now = time.perf_counter_ns()
                for item in range(batch_size):
                    sent_ns[sequence + item] = now
                if batch_index == warmup_batches:
                    callback_latencies.clear()
                    first_latencies.clear()
                    second_latencies.clear()
                    batch_rtts.clear()
                    start_stats = client.stats()
                    started = time.perf_counter()
                batch_started = time.perf_counter_ns()
                broker.push_batch(sequence, batch_size)
                current = [await receipts.get() for _ in range(batch_size)]
                await asyncio.gather(*(receipt.wait() for receipt in current))
                if batch_index >= warmup_batches:
                    batch_rtts.append(float(time.perf_counter_ns() - batch_started))
                sequence += batch_size
    finally:
        elapsed = time.perf_counter() - started if started else 0.0
        end_stats = client.stats()
        await client.disconnect()

    messages = batches * batch_size
    if len(callback_latencies) != messages:
        raise AssertionError(f"callback count {len(callback_latencies)} != {messages}")
    return Result(
        batch_size=batch_size,
        messages=messages,
        elapsed_s=elapsed,
        operations_per_second=messages / max(elapsed, 1e-9),
        callback_p50_ns=percentile(callback_latencies, 50),
        callback_p95_ns=percentile(callback_latencies, 95),
        first_callback_p50_ns=percentile(first_latencies, 50),
        second_callback_p50_ns=percentile(second_latencies, 50),
        batch_rtt_p50_ns=percentile(batch_rtts, 50),
        batch_rtt_p95_ns=percentile(batch_rtts, 95),
        eager_writes=end_stats.writer.eager_writes - start_stats.writer.eager_writes,
        writer_batches=end_stats.writer.batches - start_stats.writer.batches,
        effect_suspensions=(
            end_stats.effects.apply_suspensions - start_stats.effects.apply_suspensions
        ),
    )


def run_worker(args: argparse.Namespace) -> None:
    if args.cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {args.cpu})
    result = asyncio.run(
        sample(
            batch_size=args.batch_size,
            batches=args.batches,
            warmup_batches=args.warmup_batches,
            timeout=args.timeout,
        )
    )
    print(json.dumps(asdict(result), allow_nan=False))


def run_one(script: Path, root: Path, args: argparse.Namespace, batch_size: int) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    cmd = [
        sys.executable,
        str(script),
        "--worker",
        "--batch-size",
        str(batch_size),
        "--batches",
        str(args.batches),
        "--warmup-batches",
        str(args.warmup_batches),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        cmd.extend(("--cpu", str(args.cpu)))
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
    return json.loads(completed.stdout.splitlines()[-1])


def summarize(pairs: list[dict[str, Any]], field: str) -> dict[str, float]:
    base = [float(pair["base"][field]) for pair in pairs]
    candidate = [float(pair["candidate"][field]) for pair in pairs]
    ratios = [right / left for left, right in zip(base, candidate, strict=True)]
    return {
        "median_candidate_over_base": statistics.median(ratios),
        "min_candidate_over_base": min(ratios),
        "max_candidate_over_base": max(ratios),
        "base_cv": cv(base),
        "candidate_cv": cv(candidate),
    }


def run_parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    output: dict[str, Any] = {"repeat": args.repeat, "cells": {}}
    fields = (
        "operations_per_second",
        "callback_p50_ns",
        "callback_p95_ns",
        "first_callback_p50_ns",
        "second_callback_p50_ns",
        "batch_rtt_p50_ns",
        "batch_rtt_p95_ns",
    )
    for batch_size in (2, 8):
        pairs: list[dict[str, Any]] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured = {
                arm: run_one(script, roots[arm], args, batch_size)
                for arm in order
            }
            pairs.append({"order": list(order), "base": measured["base"], "candidate": measured["candidate"]})
        summary = {field: summarize(pairs, field) for field in fields}
        output["cells"][str(batch_size)] = {"summary": summary, "pairs": pairs}
        print(f"batch={batch_size}")
        for field in fields:
            value = summary[field]
            print(
                f"  {field}: candidate/base={value['median_candidate_over_base']:.5f} "
                f"CV={value['base_cv']:.2%}/{value['candidate_cv']:.2%} "
                f"range=[{value['min_candidate_over_base']:.5f},{value['max_candidate_over_base']:.5f}]"
            )
    args.output.write_text(json.dumps(output, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--batches", type=int, default=8_000)
    parser.add_argument("--warmup-batches", type=int, default=500)
    parser.add_argument("--repeat", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output", type=Path, default=Path("/tmp/t3-coalesced.json"))
    args = parser.parse_args()
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        run_worker(arguments)
    else:
        run_parent(arguments)
