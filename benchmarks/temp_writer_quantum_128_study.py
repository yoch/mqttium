"""Paired true-completion screen for the 128 KiB writer-quantum hypothesis.

Temporary diagnostic harness. Each pair runs fresh worker processes in alternating
order. The workload keeps a QoS 0 flood concurrent with sequential QoS 1 probes
and records true PublishReceipt completion latency plus writer decision counters.
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


def pct(values: list[float], q: float) -> float:
    xs = sorted(values)
    if not xs:
        return 0.0
    pos = q * (len(xs) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    f = pos - lo
    return xs[lo] * (1.0 - f) + xs[hi] * f


def median_ratio(pairs: list[dict[str, Any]], key: str) -> float:
    vals = [
        p["candidate"][key] / p["base"][key]
        for p in pairs
        if p["base"][key] > 0
    ]
    return statistics.median(vals) if vals else 0.0


def order_for(i: int) -> tuple[str, str]:
    return ("base", "candidate") if i % 2 == 0 else ("candidate", "base")


async def worker(args: argparse.Namespace) -> dict[str, Any]:
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            raise RuntimeError(f"cannot pin worker to CPU {args.cpu}") from exc

    from mqttium.api import AsyncClient
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"quantum128-{os.getpid()}-{time.time_ns()}",
        max_outbound_bytes=64 << 20,
        max_outbound_messages=100_000,
        max_outbound_inflight=args.inflight,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)
    flood_payload = b"f" * args.flood_bytes
    probe_payload = b"p" * args.probe_bytes
    suffix = f"{os.getpid()}/{time.time_ns()}"
    flood_topic = f"bench/quantum128/flood/{suffix}"
    probe_topic = f"bench/quantum128/probe/{suffix}"
    probe_latencies: list[float] = []
    flood_admission_latencies: list[float] = []
    gate = asyncio.Event()

    async def flood() -> None:
        await gate.wait()
        for _ in range(args.flood_count):
            t0 = time.perf_counter()
            await client.publish(flood_topic, flood_payload, qos=0)
            flood_admission_latencies.append((time.perf_counter() - t0) * 1000.0)

    async def probes() -> None:
        await gate.wait()
        await asyncio.sleep(0)
        for _ in range(args.probes):
            t0 = time.perf_counter()
            receipt = await client.publish(probe_topic, probe_payload, qos=1)
            await receipt.wait()
            probe_latencies.append((time.perf_counter() - t0) * 1000.0)

    cpu0 = time.process_time()
    t0 = time.perf_counter()
    flood_task = asyncio.create_task(flood())
    probe_task = asyncio.create_task(probes())
    gate.set()
    await asyncio.wait_for(asyncio.gather(flood_task, probe_task), timeout=args.timeout)
    write_pump = client._write_pump
    await asyncio.wait_for(write_pump.join(), timeout=args.timeout)
    elapsed = time.perf_counter() - t0
    cpu = time.process_time() - cpu0
    stats = write_pump.stats()
    resident = write_pump.resident_messages
    held = getattr(write_pump, "_held", None)
    await client.disconnect()

    if resident != 0:
        raise AssertionError(f"writer resident leak after join: {resident}")
    if held is not None:
        raise AssertionError("writer held item remains after join")

    return {
        "flood_bytes": args.flood_bytes,
        "probe_bytes": args.probe_bytes,
        "flood_count": args.flood_count,
        "probes": args.probes,
        "elapsed_seconds": elapsed,
        "flood_rate": args.flood_count / max(elapsed, 1e-9),
        "flood_mib_s": args.flood_count * args.flood_bytes / (1 << 20) / max(elapsed, 1e-9),
        "cpu_seconds": cpu,
        "probe_p50_ms": pct(probe_latencies, 0.50),
        "probe_p95_ms": pct(probe_latencies, 0.95),
        "probe_p99_ms": pct(probe_latencies, 0.99),
        "probe_p999_ms": pct(probe_latencies, 0.999),
        "probe_max_ms": max(probe_latencies, default=0.0),
        "flood_admit_p95_ms": pct(flood_admission_latencies, 0.95),
        "writer_batches": stats.batches,
        "writer_batched_items": stats.batched_items,
        "writer_batched_bytes": stats.batched_bytes,
        "writer_enqueue_suspensions": stats.enqueue_suspensions,
        "writer_eager_writes": stats.eager_writes,
    }


def run_worker(script: Path, root: Path, args: argparse.Namespace, variant: str) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--host", args.host,
        "--port", str(args.port),
        "--flood-bytes", str(args.flood_bytes),
        "--probe-bytes", str(args.probe_bytes),
        "--flood-count", str(args.flood_count),
        "--probes", str(args.probes),
        "--inflight", str(args.inflight),
        "--timeout", str(args.timeout),
    ]
    if args.cpu is not None:
        command += ["--cpu", str(args.cpu)]
    completed = subprocess.run(
        command,
        env=env,
        capture_output=True,
        text=True,
        timeout=args.timeout + 30,
        check=False,
    )
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no output")[-4000:]
        raise RuntimeError(f"{variant} worker failed rc={completed.returncode}: {diagnostic}")
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"{variant} worker produced no JSON")
    return json.loads(lines[-1])


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    pairs: list[dict[str, Any]] = []
    for i in range(args.repeat):
        measured: dict[str, Any] = {}
        order = order_for(i)
        for variant in order:
            measured[variant] = run_worker(script, roots[variant], args, variant)
        pairs.append({"order": list(order), **measured})

    result = {
        "mode": "writer_quantum_128_true_completion",
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "repeat": args.repeat,
        "workload": {
            "flood_bytes": args.flood_bytes,
            "probe_bytes": args.probe_bytes,
            "flood_count": args.flood_count,
            "probes": args.probes,
            "inflight": args.inflight,
        },
        "ratios": {
            "flood_rate": median_ratio(pairs, "flood_rate"),
            "flood_mib_s": median_ratio(pairs, "flood_mib_s"),
            "cpu": median_ratio(pairs, "cpu_seconds"),
            "probe_p50": median_ratio(pairs, "probe_p50_ms"),
            "probe_p95": median_ratio(pairs, "probe_p95_ms"),
            "probe_p99": median_ratio(pairs, "probe_p99_ms"),
            "probe_p999": median_ratio(pairs, "probe_p999_ms"),
            "probe_max": median_ratio(pairs, "probe_max_ms"),
            "flood_admit_p95": median_ratio(pairs, "flood_admit_p95_ms"),
            "writer_batches": median_ratio(pairs, "writer_batches"),
            "writer_batched_items": median_ratio(pairs, "writer_batched_items"),
            "writer_eager_writes": median_ratio(pairs, "writer_eager_writes"),
        },
        "pairs": pairs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"workload": result["workload"], "ratios": result["ratios"]}, indent=2))
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--worker", action="store_true")
    p.add_argument("--base-root", type=Path)
    p.add_argument("--candidate-root", type=Path)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=11883)
    p.add_argument("--flood-bytes", type=int, default=32 * 1024)
    p.add_argument("--probe-bytes", type=int, default=64)
    p.add_argument("--flood-count", type=int, default=1500)
    p.add_argument("--probes", type=int, default=200)
    p.add_argument("--inflight", type=int, default=20)
    p.add_argument("--repeat", type=int, default=6)
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument("--cpu", type=int)
    p.add_argument("--output", type=Path, default=Path("/tmp/quantum128.json"))
    args = p.parse_args()
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        p.error("--base-root and --candidate-root are required")
    if args.repeat <= 0 or args.repeat % 2:
        p.error("--repeat must be a positive even number")
    return args


if __name__ == "__main__":
    args = parse_args()
    if args.worker:
        print(json.dumps(asyncio.run(worker(args))))
    else:
        raise SystemExit(parent(args))
