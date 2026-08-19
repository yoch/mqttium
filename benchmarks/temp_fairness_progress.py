"""Disposable fairness-progress diagnostic for scheduler PRs #284/#285."""
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


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    pos = q * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    frac = pos - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def jain(values: list[float]) -> float:
    if not values:
        return 1.0
    total = sum(values)
    squared = sum(value * value for value in values)
    return (total * total) / (len(values) * squared) if squared else 1.0


def fairness_metrics(
    positions: list[list[int]], latencies: list[list[float]], finish: list[float]
) -> dict[str, float]:
    gaps: list[int] = []
    producer_p99: list[float] = []
    producer_mean: list[float] = []
    first_positions: list[int] = []
    for pos, waits in zip(positions, latencies, strict=True):
        if pos:
            first_positions.append(pos[0])
            gaps.extend(b - a - 1 for a, b in zip(pos, pos[1:]))
        if waits:
            producer_p99.append(pct(waits, 0.99))
            producer_mean.append(statistics.fmean(waits))
    flat = [sample for part in latencies for sample in part]
    return {
        "latency_p50_ms": pct(flat, 0.50),
        "latency_p99_ms": pct(flat, 0.99),
        "latency_p999_ms": pct(flat, 0.999),
        "latency_max_ms": max(flat, default=0.0),
        "producer_p99_min_ms": min(producer_p99, default=0.0),
        "producer_p99_max_ms": max(producer_p99, default=0.0),
        "producer_mean_jain": jain([1.0 / max(value, 1e-9) for value in producer_mean]),
        "max_admission_gap": float(max(gaps, default=0)),
        "p99_admission_gap": pct([float(value) for value in gaps], 0.99),
        "max_initial_position": float(max(first_positions, default=0)),
        "finish_spread_ms": (max(finish, default=0.0) - min(finish, default=0.0)) * 1000.0,
        "finish_max_ms": max(finish, default=0.0) * 1000.0,
    }


class YieldTransport:
    async def write(self, data: bytes) -> None:
        await asyncio.sleep(0)

    async def write_many(self, parts: list[bytes]) -> None:
        await asyncio.sleep(0)

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def writer_worker(args: argparse.Namespace) -> dict[str, Any]:
    from mqttium.api._writer import WritePump

    async def fail(exc: BaseException) -> None:
        raise AssertionError(exc)

    pump = WritePump(
        max_bytes=args.max_bytes,
        max_messages=args.max_messages,
        on_failure=fail,
    )
    pump.start(YieldTransport())
    gate = asyncio.Event()
    positions: list[list[int]] = [[] for _ in range(args.producers)]
    latencies: list[list[float]] = [[] for _ in range(args.producers)]
    finish = [0.0] * args.producers
    sequence = 0
    payload = b"x" * args.payload_bytes
    start = time.perf_counter()
    cpu0 = time.process_time()

    async def producer(index: int) -> None:
        nonlocal sequence
        await gate.wait()
        out_pos = positions[index]
        out_lat = latencies[index]
        for _ in range(args.ops):
            before = time.perf_counter()
            await pump.enqueue(payload)
            out_lat.append((time.perf_counter() - before) * 1000.0)
            sequence += 1
            out_pos.append(sequence)
        finish[index] = time.perf_counter() - start

    tasks = [asyncio.create_task(producer(index)) for index in range(args.producers)]
    gate.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=args.timeout)
    await asyncio.wait_for(pump.join(), timeout=args.timeout)
    elapsed = time.perf_counter() - start
    cpu = time.process_time() - cpu0
    stats = pump.stats()
    result: dict[str, Any] = {
        "mode": "writer",
        "producers": args.producers,
        "ops": args.ops,
        "count": args.producers * args.ops,
        "rate": (args.producers * args.ops) / max(elapsed, 1e-9),
        "cpu_seconds": cpu,
        "enqueue_suspensions": stats.enqueue_suspensions,
    }
    result.update(fairness_metrics(positions, latencies, finish))
    await pump.stop()
    return result


async def publish_worker(args: argparse.Namespace) -> dict[str, Any]:
    from mqttium.api import AsyncClient
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"fairness-{os.getpid()}-{time.time_ns()}",
        max_outbound_inflight=args.inflight,
        max_pending_outbound_messages=max(args.inflight * 4, args.producers * 2),
        max_pending_outbound_bytes=64 << 20,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)
    gate = asyncio.Event()
    positions: list[list[int]] = [[] for _ in range(args.producers)]
    latencies: list[list[float]] = [[] for _ in range(args.producers)]
    finish = [0.0] * args.producers
    receipts: list[Any] = []
    sequence = 0
    payload = b"x" * args.payload_bytes
    start = time.perf_counter()
    cpu0 = time.process_time()

    async def producer(index: int) -> None:
        nonlocal sequence
        await gate.wait()
        out_pos = positions[index]
        out_lat = latencies[index]
        topic = f"bench/fairness/{index}"
        for _ in range(args.ops):
            before = time.perf_counter()
            receipt = await client.publish(topic, payload, qos=1)
            out_lat.append((time.perf_counter() - before) * 1000.0)
            sequence += 1
            out_pos.append(sequence)
            receipts.append(receipt)
        finish[index] = time.perf_counter() - start

    tasks = [asyncio.create_task(producer(index)) for index in range(args.producers)]
    gate.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=args.timeout)
    admission_elapsed = time.perf_counter() - start
    await asyncio.wait_for(
        asyncio.gather(*(receipt.wait() for receipt in receipts)), timeout=args.timeout
    )
    true_elapsed = time.perf_counter() - start
    cpu = time.process_time() - cpu0
    result: dict[str, Any] = {
        "mode": "publish",
        "producers": args.producers,
        "ops": args.ops,
        "inflight": args.inflight,
        "count": args.producers * args.ops,
        "admission_rate": (args.producers * args.ops) / max(admission_elapsed, 1e-9),
        "true_completion_rate": (args.producers * args.ops) / max(true_elapsed, 1e-9),
        "cpu_seconds": cpu,
        "publish_wakeups": int(getattr(client, "_publish_wakeups", 0)),
        "publish_wait_retries": int(getattr(client, "_publish_wait_retries", 0)),
    }
    result.update(fairness_metrics(positions, latencies, finish))
    await client.disconnect()
    return result


def run_worker(root: Path, argv: list[str], timeout: float) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), *argv],
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError((completed.stderr or completed.stdout)[-4000:])
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def median_ratio(pairs: list[dict[str, Any]], key: str) -> float | None:
    values = [
        pair["candidate"][key] / pair["base"][key]
        for pair in pairs
        if isinstance(pair["base"].get(key), (int, float))
        and isinstance(pair["candidate"].get(key), (int, float))
        and pair["base"][key] > 0
    ]
    return statistics.median(values) if values else None


def parent(args: argparse.Namespace) -> dict[str, Any]:
    roots = {"base": args.base_root, "candidate": args.candidate_root}
    producer_values = [int(value) for value in args.producer_values.split(",")]
    inflight_values = (
        [int(value) for value in args.inflight_values.split(",")]
        if args.kind == "publish"
        else [0]
    )
    rows: list[dict[str, Any]] = []
    for producers in producer_values:
        for inflight in inflight_values:
            pairs: list[dict[str, Any]] = []
            for index in range(args.repeat):
                order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
                measured: dict[str, Any] = {}
                for variant in order:
                    argv = [
                        "worker",
                        "--kind", args.kind,
                        "--producers", str(producers),
                        "--ops", str(args.ops),
                        "--payload-bytes", str(args.payload_bytes),
                        "--timeout", str(args.timeout),
                    ]
                    if args.kind == "writer":
                        argv += [
                            "--max-messages", str(args.max_messages),
                            "--max-bytes", str(args.max_bytes),
                        ]
                    else:
                        argv += [
                            "--host", args.host,
                            "--port", str(args.port),
                            "--inflight", str(inflight),
                        ]
                    measured[variant] = run_worker(
                        roots[variant], argv, args.timeout + 20.0
                    )
                pairs.append({"order": list(order), **measured})
            keys = (
                "rate",
                "admission_rate",
                "true_completion_rate",
                "cpu_seconds",
                "latency_p99_ms",
                "latency_p999_ms",
                "latency_max_ms",
                "producer_p99_max_ms",
                "producer_mean_jain",
                "max_admission_gap",
                "p99_admission_gap",
                "finish_spread_ms",
                "finish_max_ms",
                "enqueue_suspensions",
                "publish_wakeups",
                "publish_wait_retries",
            )
            row: dict[str, Any] = {
                "producers": producers,
                "inflight": inflight if args.kind == "publish" else None,
                "pairs": pairs,
            }
            for key in keys:
                ratio = median_ratio(pairs, key)
                if ratio is not None:
                    row[key + "_ratio"] = ratio
            rows.append(row)
            summary = {k: v for k, v in row.items() if k != "pairs"}
            print(json.dumps(summary), flush=True)
    return {
        "mode": args.kind + "_fairness_progress",
        "repeat": args.repeat,
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser("worker")
    worker.add_argument("--kind", choices=("writer", "publish"), required=True)
    worker.add_argument("--producers", type=int, required=True)
    worker.add_argument("--ops", type=int, required=True)
    worker.add_argument("--payload-bytes", type=int, default=64)
    worker.add_argument("--max-messages", type=int, default=1)
    worker.add_argument("--max-bytes", type=int, default=1 << 20)
    worker.add_argument("--host", default="127.0.0.1")
    worker.add_argument("--port", type=int, default=11883)
    worker.add_argument("--inflight", type=int, default=4)
    worker.add_argument("--timeout", type=float, default=90.0)

    parent_parser = sub.add_parser("parent")
    parent_parser.add_argument("--kind", choices=("writer", "publish"), required=True)
    parent_parser.add_argument("--base-root", type=Path, required=True)
    parent_parser.add_argument("--candidate-root", type=Path, required=True)
    parent_parser.add_argument("--producer-values", default="16,32,64")
    parent_parser.add_argument("--inflight-values", default="1,4")
    parent_parser.add_argument("--ops", type=int, default=100)
    parent_parser.add_argument("--payload-bytes", type=int, default=64)
    parent_parser.add_argument("--max-messages", type=int, default=1)
    parent_parser.add_argument("--max-bytes", type=int, default=1 << 20)
    parent_parser.add_argument("--host", default="127.0.0.1")
    parent_parser.add_argument("--port", type=int, default=11883)
    parent_parser.add_argument("--repeat", type=int, default=4)
    parent_parser.add_argument("--timeout", type=float, default=90.0)
    parent_parser.add_argument("--output", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "worker":
        result = asyncio.run(writer_worker(args) if args.kind == "writer" else publish_worker(args))
    else:
        result = parent(args)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
