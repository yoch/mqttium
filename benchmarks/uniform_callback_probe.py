"""Initial same-host worker-only screening; not a release or fixed-rate gate.

Closed-loop QoS1 bursts, A/A then A/B. Retains all fresh-process samples and
100ms/1s completion windows. No sample retry, noise subtraction or adaptive rate.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys
import time

CELLS = [("callback", n) for n in (1, 2, 8, 32)] + [
    ("async", 8),
    ("filtered", 8),
    ("iterator", 8),
    ("both", 8),
    ("publish", 8),
]


def quantile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def windows(timestamps, origin, duration, width):
    bins = [0] * int(round(duration / width))
    for t in timestamps:
        if t < origin:
            continue
        index = int((t - origin) / width)
        if 0 <= index < len(bins):
            bins[index] += 1
    mean = statistics.mean(bins)
    return {
        "width_s": width,
        "counts": bins,
        "mean_rate": mean / width,
        "p05_rate": quantile(bins, 0.05) / width,
        "minimum_rate": min(bins) / width,
        "cv": statistics.pstdev(bins) / mean if mean else None,
        "zero_windows": bins.count(0),
    }


def identity(root):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()

    assert not git("status", "--porcelain", "--untracked-files=no")
    files = ["src/mqttium/api/" + p for p in ("_delivery.py", "async_client.py", "_effects.py")]
    return {
        "sha": git("rev-parse", "HEAD"),
        "src_tree": git("rev-parse", "HEAD:src"),
        "runtime_sha256": {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in files},
    }


# Keep timing closures and all-path resource cleanup in one lexical cell.
async def cell(args, mode, size):  # noqa: C901
    from mqttium.api import AsyncClient

    client_mode = mode if mode in ("iterator", "both") else "callback"
    sub = AsyncClient(
        "probe-sub",
        message_delivery=client_mode,
        keepalive=0,
        max_pending_messages=256,
        max_pending_callbacks=256,
    )
    pub = AsyncClient("probe-pub", message_delivery="callback", keepalive=0)
    topic = "uniform-probe/" + str(os.getpid())
    begin = 0.0
    active = False
    batch_start = 0.0
    sequence = 0
    expected = 0
    batch_seen = 0
    first, tail, completion, timestamps, loop_lags = [], [], [], [], []
    done, iter_done = None, None
    iter_count = 0
    errors = []
    iterator_task = None
    lag_task = None
    max_queue = 0
    loop = asyncio.get_running_loop()
    handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(str(context)))

    def record(message):
        nonlocal expected, batch_seen, max_queue
        seq = int.from_bytes(message.payload[:8], "big")
        assert seq == expected, (seq, expected)
        expected += 1
        batch_seen += 1
        now = time.perf_counter()
        if active:
            timestamps.append(now)
            (first if batch_seen == 1 else tail).append((now - batch_start) * 1e6)
            max_queue = max(
                max_queue,
                sub._callback_queue.qsize() + getattr(sub._delivery, "_callback_batch_reserved", 0),
            )
        if batch_seen == size:
            done.set_result(None)

    async def async_record(message):
        await asyncio.sleep(0)
        record(message)

    if mode == "filtered":
        sub.message_callback_add("uniform-probe/#", record)
    elif mode not in ("iterator", "publish"):
        sub.on_message = async_record if mode == "async" else record

    async def lag_monitor():
        deadline = loop.time() + 0.01
        while True:
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            now = loop.time()
            if active:
                loop_lags.append(max(0.0, now - deadline) * 1e6)
            deadline = now + 0.01

    async def consume():
        nonlocal iter_count
        async for message in sub.messages():
            if mode == "iterator":
                record(message)
            else:
                assert int.from_bytes(message.payload[:8], "big") == iter_count
            iter_count += 1
            if iter_count % size == 0:
                iter_done.set_result(None)

    async def one_batch():
        nonlocal sequence, batch_seen, done, iter_done, batch_start
        batch_seen = 0
        done = loop.create_future()
        iter_done = loop.create_future()
        batch_start = time.perf_counter()
        receipts = []
        for _ in range(size):
            payload = sequence.to_bytes(8, "big") + b"x" * 56
            sequence += 1
            receipts.append(pub.publish_nowait(topic, payload, qos=1))
        if mode != "publish":
            await done
        for receipt in receipts:
            await receipt.wait()
            if active and mode == "publish":
                timestamps.append(time.perf_counter())
        if mode in ("iterator", "both"):
            await iter_done
        if active:
            completion.append((time.perf_counter() - batch_start) * 1e6)

    try:
        await pub.connect("127.0.0.1", args.port, timeout=3)
        if mode != "publish":
            await sub.connect("127.0.0.1", args.port, timeout=3)
            await sub.subscribe(topic, qos=1)
        if mode in ("iterator", "both"):
            iterator_task = asyncio.create_task(consume())
        lag_task = asyncio.create_task(lag_monitor())
        warmup_until = time.perf_counter() + 0.25
        while time.perf_counter() < warmup_until:
            await one_batch()
        start_sequence = sequence
        active = True
        begin = time.perf_counter()
        cpu = time.process_time()
        while time.perf_counter() - begin < args.seconds:
            await one_batch()
        end = time.perf_counter()
        cpu = time.process_time() - cpu
        active = False
        assert not errors, errors
        assert mode == "publish" or sequence == expected
        assert len(timestamps) == sequence - start_sequence
        assert end - begin < args.seconds + 1, "abnormally long final batch"
        await sub._callback_queue.join()
        assert sub.stats().delivery.callback_queued == 0

        def latency(values):
            return {
                "n": len(values),
                "p50_us": quantile(values, 0.5),
                "p95_us": quantile(values, 0.95),
                "p99_us": quantile(values, 0.99),
            }

        result = {
            "mode": mode,
            "batch": size,
            "workload": "closed_loop_qos1_64B",
            "seconds": args.seconds,
            "completed_including_drain": len(timestamps),
            "cpu_us_per_completed": cpu * 1e6 / len(timestamps),
            "windows_100ms": windows(timestamps, begin, args.seconds, 0.1),
            "windows_1s": windows(timestamps, begin, args.seconds, 1),
            "first": latency(first),
            "tail": latency(tail),
            "completion": latency(completion),
            "loop_lag": latency(loop_lags),
            "queue_peak_at_callback": max_queue,
            "delivery": dataclasses.asdict(sub.stats().delivery),
            "effect": dataclasses.asdict(sub.stats().effects),
        }
        assert not errors, errors
        return result
    finally:
        for task in (iterator_task, lag_task):
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        await pub.disconnect()
        await sub.disconnect()
        loop.set_exception_handler(handler)


def worker(args):
    root = Path(args.source).resolve()
    before = identity(root)
    sys.path.insert(0, str(root / "src"))
    import mqttium

    assert Path(mqttium.__file__).resolve().is_relative_to(root)
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {args.cpu})
    cells = list(CELLS)
    random.Random(args.seed).shuffle(cells)

    async def run():
        rows = []
        for mode, size in cells:
            rows.append(await asyncio.wait_for(cell(args, mode, size), args.seconds + 10))
        await asyncio.sleep(0)
        assert len(asyncio.all_tasks()) == 1, [t.get_name() for t in asyncio.all_tasks()]
        return rows

    rows = asyncio.run(run())
    assert identity(root) == before
    print(
        json.dumps(
            {
                "source": before,
                "source_path": str(root),
                "python": sys.version,
                "cells": rows,
                "diagnostic_only": True,
            }
        )
    )


def controller(args):
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    for phase in ("AA", "AB"):
        for pair in range(args.pairs):
            for label in ("A", "B") if pair % 2 == 0 else ("B", "A"):
                path = (
                    args.base
                    if label == "A"
                    else (args.control if phase == "AA" else args.candidate)
                )
                command = [
                    sys.executable,
                    __file__,
                    "--source",
                    path,
                    "--port",
                    str(args.port),
                    "--cpu",
                    str(args.cpu),
                    "--seed",
                    str(20260910 + pair),
                    "--seconds",
                    str(args.seconds),
                ]
                result = subprocess.run(
                    command,
                    text=True,
                    capture_output=True,
                    timeout=len(CELLS) * (args.seconds + 10) + 10,
                )
                prefix = out / f"{phase}-{pair:02}-{label}"
                prefix.with_suffix(".stderr").write_text(result.stderr)
                prefix.with_suffix(".stdout").write_text(result.stdout)
                result.check_returncode()
                doc = json.loads(result.stdout)
                doc.update(phase=phase, pair=pair, label=label)
                prefix.with_suffix(".json").write_text(json.dumps(doc, indent=2) + "\n")
            print(f"{phase} pair {pair + 1}/{args.pairs} completed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source")
    parser.add_argument("--base")
    parser.add_argument("--candidate")
    parser.add_argument("--control")
    parser.add_argument("--output")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--seconds", type=float, default=2.0)
    parser.add_argument("--pairs", type=int, default=4)
    parser.add_argument("--seed", type=int, default=20260910)
    parser.add_argument("--cpu", type=int, default=0)
    args = parser.parse_args()
    if args.seconds < 1 or not args.seconds.is_integer():
        parser.error("seconds must be a positive whole number for full 1s windows")
    if args.pairs < 2 or args.pairs % 2:
        parser.error("pairs must be positive, even, and at least two")
    if args.source:
        worker(args)
    else:
        controller(args)


if __name__ == "__main__":
    main()
