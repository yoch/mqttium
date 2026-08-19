"""Temporary ARM64 scheduler experiment supplements for PRs #284-#286.

This file lives only on a disposable benchmark branch. It deliberately uses
fresh worker processes and alternating pair order so each variant imports the
requested source tree through PYTHONPATH.
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
from pathlib import Path
from typing import Any


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    pos = q * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    f = pos - lo
    return xs[lo] * (1.0 - f) + xs[hi] * f


def median_ratio(pairs: list[dict[str, Any]], key: str) -> float:
    return statistics.median(
        p["candidate"][key] / p["base"][key]
        for p in pairs
        if p["base"][key] > 0
    )


def run_worker(root: Path, argv: list[str], timeout: float) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), *argv],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(
            f"worker failed rc={completed.returncode}: "
            f"{(completed.stderr or completed.stdout)[-4000:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def pair_order(index: int) -> tuple[str, str]:
    return ("base", "candidate") if index % 2 == 0 else ("candidate", "base")


def jain(values: list[float]) -> float:
    if not values:
        return 1.0
    s = sum(values)
    ss = sum(v * v for v in values)
    return (s * s) / (len(values) * ss) if ss else 1.0


async def publish_worker(args: argparse.Namespace) -> dict[str, Any]:
    from mqttium.api import AsyncClient
    from mqttium.protocol.reconnect import ReconnectPolicy

    if args.cpu is not None:
        os.sched_setaffinity(0, {args.cpu})
    client = AsyncClient(
        client_id=f"supp-publish-{os.getpid()}-{time.time_ns()}",
        max_outbound_inflight=args.inflight,
        max_pending_outbound_messages=max(args.inflight * 4, args.publishers * 4),
        max_pending_outbound_bytes=64 << 20,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)
    topic = f"bench/supp/publish/{os.getpid()}/{time.time_ns()}"
    payload = b"x" * args.payload_bytes
    counts = [args.count // args.publishers] * args.publishers
    for i in range(args.count % args.publishers):
        counts[i] += 1
    receipts: list[Any] = []
    producer_latencies: list[list[float]] = [[] for _ in range(args.publishers)]

    async def producer(index: int, n: int) -> None:
        out = producer_latencies[index]
        for _ in range(n):
            t0 = time.perf_counter()
            receipt = await client.publish(topic, payload, qos=1)
            out.append((time.perf_counter() - t0) * 1000.0)
            receipts.append(receipt)

    cpu0 = time.process_time()
    t0 = time.perf_counter()
    await asyncio.wait_for(
        asyncio.gather(*(producer(i, n) for i, n in enumerate(counts))),
        timeout=args.timeout,
    )
    admission_elapsed = time.perf_counter() - t0
    admission_cpu = time.process_time() - cpu0
    await asyncio.wait_for(
        asyncio.gather(*(receipt.wait() for receipt in receipts)),
        timeout=args.timeout,
    )
    true_elapsed = time.perf_counter() - t0
    true_cpu = time.process_time() - cpu0
    if getattr(client, "_write_pump", None) is not None:
        await asyncio.wait_for(client._write_pump.join(), timeout=args.timeout)
    flat = [x for part in producer_latencies for x in part]
    producer_p99 = [pct(part, 0.99) for part in producer_latencies if part]
    producer_mean = [statistics.fmean(part) for part in producer_latencies if part]
    result = {
        "publishers": args.publishers,
        "inflight": args.inflight,
        "count": args.count,
        "admission_rate": args.count / max(admission_elapsed, 1e-9),
        "true_completion_rate": args.count / max(true_elapsed, 1e-9),
        "admission_cpu_seconds": admission_cpu,
        "true_cpu_seconds": true_cpu,
        "admission_p50_ms": pct(flat, 0.50),
        "admission_p95_ms": pct(flat, 0.95),
        "admission_p99_ms": pct(flat, 0.99),
        "admission_p999_ms": pct(flat, 0.999),
        "admission_max_ms": max(flat, default=0.0),
        "producer_p99_min_ms": min(producer_p99, default=0.0),
        "producer_p99_max_ms": max(producer_p99, default=0.0),
        "producer_mean_jain": jain([1.0 / max(x, 1e-9) for x in producer_mean]),
        "publish_wakeups": int(getattr(client, "_publish_wakeups", 0)),
        "publish_wait_retries": int(getattr(client, "_publish_wait_retries", 0)),
    }
    await client.disconnect()
    return result


def publish_parent(args: argparse.Namespace) -> dict[str, Any]:
    roots = {"base": args.base_root, "candidate": args.candidate_root}
    scenarios: list[dict[str, Any]] = []
    for publishers in args.publisher_values:
        for inflight in args.inflight_values:
            pairs: list[dict[str, Any]] = []
            for i in range(args.repeat):
                order = pair_order(i)
                measured: dict[str, Any] = {}
                for variant in order:
                    measured[variant] = run_worker(
                        roots[variant],
                        [
                            "publish-worker", "--host", args.host, "--port", str(args.port),
                            "--publishers", str(publishers), "--inflight", str(inflight),
                            "--payload-bytes", str(args.payload_bytes), "--count", str(args.count),
                            "--timeout", str(args.timeout), "--cpu", str(args.cpu),
                        ],
                        args.timeout + 30,
                    )
                pairs.append({"order": list(order), **measured})
            scenarios.append({
                "publishers": publishers,
                "inflight": inflight,
                "pairs": pairs,
                "admission_rate_ratio": median_ratio(pairs, "admission_rate"),
                "true_completion_rate_ratio": median_ratio(pairs, "true_completion_rate"),
                "true_cpu_ratio": median_ratio(pairs, "true_cpu_seconds"),
                "p99_ratio": median_ratio(pairs, "admission_p99_ms"),
                "p999_ratio": median_ratio(pairs, "admission_p999_ms"),
                "max_ratio": median_ratio(pairs, "admission_max_ms"),
            })
            print(
                f"publish p={publishers} inf={inflight} "
                f"admission={scenarios[-1]['admission_rate_ratio']:.4f} "
                f"true={scenarios[-1]['true_completion_rate_ratio']:.4f} "
                f"p99={scenarios[-1]['p99_ratio']:.4f}",
                flush=True,
            )
    return {"mode": "publish_true_completion", "repeat": args.repeat, "scenarios": scenarios}


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


async def writer_hetero_worker(args: argparse.Namespace) -> dict[str, Any]:
    from mqttium.api._writer import WritePump

    if args.cpu is not None:
        os.sched_setaffinity(0, {args.cpu})
    async def fail(exc: BaseException) -> None:
        raise AssertionError(exc)
    pump = WritePump(max_bytes=args.max_bytes, max_messages=args.max_messages, on_failure=fail)
    pump.start(YieldTransport())
    counts = [args.count // args.producers] * args.producers
    for i in range(args.count % args.producers):
        counts[i] += 1
    per_latency: list[list[float]] = [[] for _ in range(args.producers)]
    finish = [0.0] * args.producers
    sizes = [args.large_bytes if i < (args.producers + 1) // 2 else args.small_bytes for i in range(args.producers)]
    start = time.perf_counter()
    cpu0 = time.process_time()

    async def producer(i: int, n: int) -> None:
        payload = b"x" * sizes[i]
        out = per_latency[i]
        for _ in range(n):
            t0 = time.perf_counter()
            await pump.enqueue(payload)
            out.append((time.perf_counter() - t0) * 1000.0)
        finish[i] = time.perf_counter() - start

    await asyncio.gather(*(producer(i, n) for i, n in enumerate(counts)))
    await pump.join()
    elapsed = time.perf_counter() - start
    cpu = time.process_time() - cpu0
    stats = pump.stats()
    small = [x for i, part in enumerate(per_latency) if sizes[i] == args.small_bytes for x in part]
    large = [x for i, part in enumerate(per_latency) if sizes[i] == args.large_bytes for x in part]
    pmean = [statistics.fmean(part) for part in per_latency if part]
    await pump.stop()
    return {
        "producers": args.producers,
        "count": args.count,
        "rate": args.count / max(elapsed, 1e-9),
        "cpu_seconds": cpu,
        "enqueue_suspensions": stats.enqueue_suspensions,
        "small_p99_ms": pct(small, 0.99),
        "small_p999_ms": pct(small, 0.999),
        "large_p99_ms": pct(large, 0.99),
        "large_p999_ms": pct(large, 0.999),
        "producer_finish_p99_s": pct(finish, 0.99),
        "producer_finish_max_s": max(finish, default=0.0),
        "producer_mean_jain": jain([1.0 / max(x, 1e-9) for x in pmean]),
    }


def writer_hetero_parent(args: argparse.Namespace) -> dict[str, Any]:
    roots = {"base": args.base_root, "candidate": args.candidate_root}
    scenarios = []
    for producers in args.producer_values:
        pairs = []
        for i in range(args.repeat):
            order = pair_order(i)
            measured = {}
            for variant in order:
                measured[variant] = run_worker(
                    roots[variant],
                    [
                        "writer-hetero-worker", "--producers", str(producers),
                        "--count", str(args.count), "--max-messages", str(args.max_messages),
                        "--max-bytes", str(args.max_bytes), "--small-bytes", str(args.small_bytes),
                        "--large-bytes", str(args.large_bytes), "--cpu", str(args.cpu),
                    ],
                    120.0,
                )
            pairs.append({"order": list(order), **measured})
        row = {
            "producers": producers,
            "pairs": pairs,
            "rate_ratio": median_ratio(pairs, "rate"),
            "cpu_ratio": median_ratio(pairs, "cpu_seconds"),
            "small_p99_ratio": median_ratio(pairs, "small_p99_ms"),
            "small_p999_ratio": median_ratio(pairs, "small_p999_ms"),
            "large_p99_ratio": median_ratio(pairs, "large_p99_ms"),
            "finish_max_ratio": median_ratio(pairs, "producer_finish_max_s"),
        }
        scenarios.append(row)
        print(
            f"writer hetero p={producers} rate={row['rate_ratio']:.4f} "
            f"small-p99={row['small_p99_ratio']:.4f} finish={row['finish_max_ratio']:.4f}",
            flush=True,
        )
    return {"mode": "writer_heterogeneous", "repeat": args.repeat, "scenarios": scenarios}


async def quantum_tail_worker(args: argparse.Namespace) -> dict[str, Any]:
    if args.cpu is not None:
        os.sched_setaffinity(0, {args.cpu})
    import mqttium.api._writer as writer_mod
    if args.quantum > 0:
        if not hasattr(writer_mod, "_WRITER_BATCH_MAX_BYTES"):
            raise RuntimeError("quantum override requested for root without byte-quantum candidate")
        writer_mod._WRITER_BATCH_MAX_BYTES = args.quantum
    from mqttium.api import AsyncClient
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"supp-quantum-{os.getpid()}-{time.time_ns()}",
        max_outbound_bytes=1 << 20,
        max_outbound_messages=4096,
        max_outbound_inflight=20,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)
    flood_payload = b"f" * args.flood_bytes
    probe_payload = b"p" * 64
    flood_topic = f"bench/supp/flood/{os.getpid()}"
    probe_topic = f"bench/supp/probe/{os.getpid()}"
    latencies: list[float] = []
    start = time.perf_counter()
    cpu0 = time.process_time()

    async def flood() -> None:
        for _ in range(args.flood_count):
            await client.publish(flood_topic, flood_payload, qos=0)

    async def probe() -> None:
        await asyncio.sleep(0)
        for _ in range(args.probes):
            t0 = time.perf_counter()
            r = await client.publish(probe_topic, probe_payload, qos=1)
            await r.wait()
            latencies.append((time.perf_counter() - t0) * 1000.0)

    await asyncio.wait_for(asyncio.gather(flood(), probe()), timeout=args.timeout)
    await asyncio.wait_for(client._write_pump.join(), timeout=args.timeout)
    elapsed = time.perf_counter() - start
    cpu = time.process_time() - cpu0
    stats = client._write_pump.stats()
    await client.disconnect()
    return {
        "quantum": args.quantum,
        "elapsed_seconds": elapsed,
        "flood_rate": args.flood_count / max(elapsed, 1e-9),
        "cpu_seconds": cpu,
        "probe_p50_ms": pct(latencies, 0.50),
        "probe_p95_ms": pct(latencies, 0.95),
        "probe_p99_ms": pct(latencies, 0.99),
        "probe_p999_ms": pct(latencies, 0.999),
        "probe_max_ms": max(latencies, default=0.0),
        "writer_batches": stats.batches,
        "max_batch_items": stats.max_batch_items,
    }


def quantum_tail_parent(args: argparse.Namespace) -> dict[str, Any]:
    scenarios = []
    for quantum in args.quantum_values:
        pairs = []
        for i in range(args.repeat):
            order = pair_order(i)
            measured = {}
            for variant in order:
                root = args.base_root if variant == "base" else args.candidate_root
                q = 0 if variant == "base" else quantum
                measured[variant] = run_worker(
                    root,
                    [
                        "quantum-tail-worker", "--host", args.host, "--port", str(args.port),
                        "--quantum", str(q), "--flood-count", str(args.flood_count),
                        "--flood-bytes", str(args.flood_bytes), "--probes", str(args.probes),
                        "--timeout", str(args.timeout), "--cpu", str(args.cpu),
                    ],
                    args.timeout + 30,
                )
            pairs.append({"order": list(order), **measured})
        row = {
            "quantum": quantum,
            "pairs": pairs,
            "flood_rate_ratio": median_ratio(pairs, "flood_rate"),
            "cpu_ratio": median_ratio(pairs, "cpu_seconds"),
            "p50_ratio": median_ratio(pairs, "probe_p50_ms"),
            "p95_ratio": median_ratio(pairs, "probe_p95_ms"),
            "p99_ratio": median_ratio(pairs, "probe_p99_ms"),
            "p999_ratio": median_ratio(pairs, "probe_p999_ms"),
            "max_ratio": median_ratio(pairs, "probe_max_ms"),
        }
        scenarios.append(row)
        print(
            f"quantum={quantum} flood={row['flood_rate_ratio']:.4f} "
            f"p99={row['p99_ratio']:.4f} p999={row['p999_ratio']:.4f}",
            flush=True,
        )
    return {"mode": "quantum_mixed_tail", "repeat": args.repeat, "scenarios": scenarios}


def csv_ints(text: str) -> list[int]:
    return [int(x) for x in text.split(",") if x]


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="mode", required=True)

    def common_worker(sp):
        sp.add_argument("--cpu", type=int, default=1)

    pw = sub.add_parser("publish-worker")
    common_worker(pw)
    pw.add_argument("--host", default="127.0.0.1"); pw.add_argument("--port", type=int, default=11883)
    pw.add_argument("--publishers", type=int, required=True); pw.add_argument("--inflight", type=int, required=True)
    pw.add_argument("--payload-bytes", type=int, default=64); pw.add_argument("--count", type=int, default=4000)
    pw.add_argument("--timeout", type=float, default=120.0)

    pp = sub.add_parser("publish-parent")
    pp.add_argument("--base-root", type=Path, required=True); pp.add_argument("--candidate-root", type=Path, required=True)
    pp.add_argument("--host", default="127.0.0.1"); pp.add_argument("--port", type=int, default=11883)
    pp.add_argument("--publisher-values", type=csv_ints, default=csv_ints("1,4,16,64,256"))
    pp.add_argument("--inflight-values", type=csv_ints, default=csv_ints("1,4,20"))
    pp.add_argument("--payload-bytes", type=int, default=64); pp.add_argument("--count", type=int, default=4000)
    pp.add_argument("--repeat", type=int, default=8); pp.add_argument("--timeout", type=float, default=120.0); pp.add_argument("--cpu", type=int, default=1)
    pp.add_argument("--output", type=Path, required=True)

    whw = sub.add_parser("writer-hetero-worker")
    common_worker(whw)
    whw.add_argument("--producers", type=int, required=True); whw.add_argument("--count", type=int, default=20000)
    whw.add_argument("--max-messages", type=int, default=8); whw.add_argument("--max-bytes", type=int, default=16384)
    whw.add_argument("--small-bytes", type=int, default=64); whw.add_argument("--large-bytes", type=int, default=4096)

    whp = sub.add_parser("writer-hetero-parent")
    whp.add_argument("--base-root", type=Path, required=True); whp.add_argument("--candidate-root", type=Path, required=True)
    whp.add_argument("--producer-values", type=csv_ints, default=csv_ints("4,16,64,256")); whp.add_argument("--count", type=int, default=20000)
    whp.add_argument("--max-messages", type=int, default=8); whp.add_argument("--max-bytes", type=int, default=16384)
    whp.add_argument("--small-bytes", type=int, default=64); whp.add_argument("--large-bytes", type=int, default=4096)
    whp.add_argument("--repeat", type=int, default=8); whp.add_argument("--cpu", type=int, default=1); whp.add_argument("--output", type=Path, required=True)

    qtw = sub.add_parser("quantum-tail-worker")
    common_worker(qtw)
    qtw.add_argument("--host", default="127.0.0.1"); qtw.add_argument("--port", type=int, default=11883); qtw.add_argument("--quantum", type=int, default=0)
    qtw.add_argument("--flood-count", type=int, default=1000); qtw.add_argument("--flood-bytes", type=int, default=32768); qtw.add_argument("--probes", type=int, default=100)
    qtw.add_argument("--timeout", type=float, default=120.0)

    qtp = sub.add_parser("quantum-tail-parent")
    qtp.add_argument("--base-root", type=Path, required=True); qtp.add_argument("--candidate-root", type=Path, required=True)
    qtp.add_argument("--host", default="127.0.0.1"); qtp.add_argument("--port", type=int, default=11883)
    qtp.add_argument("--quantum-values", type=csv_ints, default=csv_ints("32768,65536,131072,262144"))
    qtp.add_argument("--flood-count", type=int, default=1000); qtp.add_argument("--flood-bytes", type=int, default=32768); qtp.add_argument("--probes", type=int, default=100)
    qtp.add_argument("--repeat", type=int, default=8); qtp.add_argument("--timeout", type=float, default=120.0); qtp.add_argument("--cpu", type=int, default=1); qtp.add_argument("--output", type=Path, required=True)

    args = p.parse_args()
    if args.mode == "publish-worker": result = asyncio.run(publish_worker(args))
    elif args.mode == "publish-parent": result = publish_parent(args)
    elif args.mode == "writer-hetero-worker": result = asyncio.run(writer_hetero_worker(args))
    elif args.mode == "writer-hetero-parent": result = writer_hetero_parent(args)
    elif args.mode == "quantum-tail-worker": result = asyncio.run(quantum_tail_worker(args))
    elif args.mode == "quantum-tail-parent": result = quantum_tail_parent(args)
    else: raise AssertionError(args.mode)
    if hasattr(args, "output"):
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))

if __name__ == "__main__":
    main()
