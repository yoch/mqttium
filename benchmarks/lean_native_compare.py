#!/usr/bin/env python3
"""Diagnostic exact-commit ABBA comparison for the lean-native experiment.

Each fresh worker runs one self-subscribed native MQTT client against Mosquitto.
Completion requires every payload in order and all publication receipts. Both
MQTT directions therefore participate in the QoS/backend/delivery-mode cell.
Performance runs have no tracemalloc overhead; a separate phase measures Python
allocation peak. Raw JSON output belongs outside the repository.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import hashlib
import itertools
import json
import math
import os
import platform
import resource
import statistics
import struct
import subprocess
import sys
import tempfile
import time
import tracemalloc
from array import array
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def _percentile(values: list[int], percentile: float) -> float:
    return float(values[min(len(values) - 1, math.ceil(len(values) * percentile) - 1)]) / 1000


def _rss_peak_kib() -> int:
    # VmHWM belongs to the current address space, excluding pre-exec peaks
    # inherited from a large benchmark controller on some POSIX spawn paths.
    status = Path("/proc/self/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


async def _phase(  # noqa: C901 - one lifecycle brackets each measured phase
    args: argparse.Namespace, *, count: int, trace: bool
) -> dict[str, Any]:
    # The selected source root, not the installed editable distribution, wins.
    from mqttium.api import AsyncClient, PublishMessage
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.persistence import MemoryInflightStore, SqliteInflightStore

    spec = json.loads(args.scenario)
    protocol = (
        MQTTProtocolVersion.MQTTv311 if spec["protocol"] == 311 else MQTTProtocolVersion.MQTTv5
    )
    topic = f"lean/{os.getpid()}/{time.monotonic_ns()}"
    warmup = 32
    total = warmup + count
    latencies = array("Q", [0]) * total
    received = 0
    progress = asyncio.Event()
    errors: list[str] = []
    suffix = b"x" * (256 - 16)

    def observe(message: Any) -> None:
        nonlocal received
        sequence, sent = struct.unpack_from("!QQ", message.payload)
        if sequence != received or len(message.payload) != 256 or message.payload[16:] != suffix:
            errors.append(f"delivery {received}: invalid sequence/payload {sequence}")
        if sequence >= total:
            errors.append(f"unexpected delivery {sequence}")
        else:
            latencies[sequence] = time.perf_counter_ns() - sent
        received += 1
        progress.set()

    async def wait_received(target: int) -> None:
        while received < target:
            progress.clear()
            await progress.wait()
        if errors:
            raise AssertionError(errors)

    with tempfile.TemporaryDirectory(prefix="mqttium-lean-bench-") as directory:
        store = (
            MemoryInflightStore()
            if spec["store"] == "memory"
            else SqliteInflightStore(Path(directory) / "session.db")
        )
        client = AsyncClient(
            f"lean-{os.getpid()}",
            protocol=protocol,
            store=store,
            message_delivery=spec["mode"],
            max_outbound_inflight=20,
            max_pending_outbound_messages=10_000,
            max_pending_outbound_bytes=64 * 1024**2,
            max_outbound_messages=10_000,
            max_outbound_bytes=1024**2,
            max_pending_messages=1024,
            max_pending_callbacks=1024,
            max_pending_delivery_bytes=64 * 1024**2,
            delivery_timeout=5.0,
            keepalive=0,
        )
        if spec["mode"] == "callback":
            client.on_message = observe
        consumer = None
        loop = asyncio.get_running_loop()
        prior_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _, context: errors.append(str(context)))

        async def consume() -> None:
            async for message in client.messages():
                observe(message)

        def request(index: int) -> Any:
            payload = struct.pack("!QQ", index, time.perf_counter_ns()) + suffix
            return PublishMessage(topic, payload, qos=spec["qos"])

        async def publish_range(start: int, stop: int) -> None:
            if spec["burst"] == "long":
                receipt = await client.publish_many(request(index) for index in range(start, stop))
                await receipt.wait()
                await wait_received(stop)
            else:
                burst = int(spec["burst"])
                for first in range(start, stop, burst):
                    end = min(first + burst, stop)
                    receipts = []
                    for index in range(first, end):
                        message = request(index)
                        receipts.append(
                            await client.publish(message.topic, message.payload, qos=message.qos)
                        )
                    for receipt in receipts:
                        await receipt.wait()
                    await wait_received(end)

        try:
            await client.connect(args.host, args.port, timeout=5)
            if spec["mode"] == "iterator":
                consumer = asyncio.create_task(consume())
            suback = await client.subscribe(topic, qos=spec["qos"])
            if list(suback.reason_codes) != [spec["qos"]]:
                raise AssertionError(f"unexpected SUBACK: {suback.reason_codes}")
            await publish_range(0, warmup)
            gc.collect()
            if trace:
                tracemalloc.start()
            cpu_start = time.process_time_ns()
            start = time.perf_counter_ns()
            await publish_range(warmup, total)
            # Delivery precedes final inbound QoS 2 acknowledgement. Include
            # its bounded tail so the timed phase leaves no protocol records.
            while client.stats().inbound.inflight:
                await asyncio.sleep(0)
            elapsed = (time.perf_counter_ns() - start) / 1e9
            cpu = (time.process_time_ns() - cpu_start) / 1e9
            traced_peak = tracemalloc.get_traced_memory()[1] if trace else None
            if trace:
                tracemalloc.stop()
            if received != total or errors:
                raise AssertionError(f"delivery mismatch: {received}/{total}: {errors}")
            snapshot = client.stats()
            if snapshot.outbound.pending_messages or snapshot.inbound.inflight:
                raise AssertionError("receipts completed with protocol records still pending")
            ordered = sorted(latencies[warmup:])
            return {
                "count": count,
                "delivered": received - warmup,
                "elapsed_s": elapsed,
                "delivered_per_s": count / elapsed,
                "cpu_s": cpu,
                "cpu_us_per_message": cpu * 1e6 / count,
                "latency_p50_us": _percentile(ordered, 0.50),
                "latency_p95_us": _percentile(ordered, 0.95),
                "latency_p99_us": _percentile(ordered, 0.99),
                "rss_peak_kib": _rss_peak_kib(),
                "python_peak_bytes": traced_peak,
                "outbound_high_water_messages": snapshot.outbound.pending_high_water_messages,
                "outbound_high_water_bytes": snapshot.outbound.pending_high_water_bytes,
                "writer_high_water_bytes": snapshot.writer.high_water_bytes,
                "delivery_high_water_bytes": snapshot.delivery.pending_high_water_bytes,
            }
        finally:
            if tracemalloc.is_tracing():
                tracemalloc.stop()
            await client.disconnect()
            if consumer is not None:
                await consumer
            if spec["store"] == "sqlite":
                store.close()
            loop.set_exception_handler(prior_handler)


def _worker(args: argparse.Namespace) -> None:
    sys.path.insert(0, str(Path(args.source_root).resolve() / "src"))
    import mqttium

    if not Path(mqttium.__file__).resolve().is_relative_to(Path(args.source_root).resolve()):
        raise AssertionError("worker imported a different source tree")
    if args.cpu is not None:
        os.sched_setaffinity(0, {args.cpu})

    async def run() -> dict[str, Any]:
        async with asyncio.timeout(120):
            performance = await _phase(args, count=args.count, trace=False)
            memory = (
                None if args.pilot else await _phase(args, count=min(args.count, 2048), trace=True)
            )
            return {"performance": performance, "memory": memory}

    print(json.dumps(asyncio.run(run())))


def _run_worker(
    args: argparse.Namespace, root: Path, spec: dict[str, Any], count: int, *, pilot: bool = False
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--source-root",
        str(root),
        "--scenario",
        json.dumps(spec),
        "--count",
        str(count),
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    if args.cpu is not None:
        command += ["--cpu", str(args.cpu)]
    if pilot:
        command.append("--pilot")
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONHASHSEED": "0"}
    result = subprocess.run(
        command, cwd="/tmp", env=env, capture_output=True, text=True, timeout=150
    )
    if result.returncode:
        raise RuntimeError(f"worker {root} {spec} failed:\n{result.stdout}\n{result.stderr}")
    return json.loads(result.stdout)


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result = {}
    metrics = [
        "delivered_per_s",
        "cpu_us_per_message",
        "latency_p50_us",
        "latency_p95_us",
        "latency_p99_us",
        "rss_peak_kib",
    ]
    for metric in metrics + ["python_peak_bytes"]:
        phase = "memory" if metric == "python_peak_bytes" else "performance"
        arms = {
            arm: [r["result"][phase][metric] for r in rows if r["arm"] == arm] for arm in ("A", "B")
        }
        cycle_ids = sorted({r["cycle"] for r in rows})
        ratios = []
        for cycle in cycle_ids:
            cycle_arms = {
                arm: [
                    r["result"][phase][metric]
                    for r in rows
                    if r["arm"] == arm and r["cycle"] == cycle
                ]
                for arm in ("A", "B")
            }
            ratios.append(
                statistics.geometric_mean(cycle_arms["B"])
                / statistics.geometric_mean(cycle_arms["A"])
            )
        result[metric] = {
            "A_median": statistics.median(arms["A"]),
            "B_median": statistics.median(arms["B"]),
            "cycle_ratio_geomean": statistics.geometric_mean(ratios),
            "cycle_ratios": ratios,
            "A_cv": statistics.stdev(arms["A"]) / statistics.mean(arms["A"])
            if len(arms["A"]) > 1
            else 0,
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--protocols", default="311,5")
    parser.add_argument("--qos-values", default="0,1,2")
    parser.add_argument("--stores", default="memory,sqlite")
    parser.add_argument("--modes", default="iterator,callback")
    parser.add_argument("--bursts", default="1,2,8,long")
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--aa-cycles", type=int, default=1)
    parser.add_argument("--target-seconds", type=float, default=0.2)
    parser.add_argument("--count", type=int)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--source-root")
    parser.add_argument("--scenario")
    parser.add_argument("--pilot", action="store_true")
    args = parser.parse_args()
    if args.worker:
        _worker(args)
        return
    if args.base_root is None or args.candidate_root is None or args.output is None:
        parser.error("base-root, candidate-root and output are required")
    if args.cycles < 1 or args.aa_cycles < 1 or args.target_seconds <= 0:
        parser.error("positive complete A/A and A/B cycles and target duration are required")
    roots = {"A": args.base_root.resolve(), "B": args.candidate_root.resolve()}
    for root in roots.values():
        _git(root, "diff", "--exit-code", "HEAD", "--", "src")
    metadata = {
        "started_utc": datetime.now(UTC).isoformat(),
        "commits": {arm: _git(root, "rev-parse", "HEAD") for arm, root in roots.items()},
        "source_roots": {arm: str(root) for arm, root in roots.items()},
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu": args.cpu,
        "affinity": sorted(os.sched_getaffinity(0)),
        "load_start": os.getloadavg(),
        "order": "ABBA",
        "ab_cycles": args.cycles,
        "aa_cycles": args.aa_cycles,
        "payload_bytes": 256,
        "flow_limit": 20,
        "broker": f"{args.host}:{args.port}",
        "interpretation": "diagnostic self-subscribed combined native publish/receive workload; no performance gate",
    }
    output: dict[str, Any] = {"metadata": metadata, "cells": []}
    specs = [
        dict(zip(("protocol", "qos", "store", "mode", "burst"), values, strict=True))
        for values in itertools.product(
            map(int, args.protocols.split(",")),
            map(int, args.qos_values.split(",")),
            args.stores.split(","),
            args.modes.split(","),
            args.bursts.split(","),
        )
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for index, spec in enumerate(specs):
        pilot_count = 512 if spec["burst"] == "long" else 64
        pilot = _run_worker(args, roots["A"], spec, pilot_count, pilot=True)
        floor = 2048 if spec["burst"] == "long" else 128
        count = args.count or max(
            floor,
            min(
                8192,
                math.ceil(args.target_seconds * pilot_count / pilot["performance"]["elapsed_s"] / 8)
                * 8,
            ),
        )
        cell: dict[str, Any] = {"spec": spec, "count": count, "pilot": pilot, "AA": [], "AB": []}
        for stage, cycles in (("AA", args.aa_cycles), ("AB", args.cycles)):
            for cycle in range(cycles):
                for position, arm in enumerate("ABBA"):
                    root = roots["A"] if stage == "AA" else roots[arm]
                    result = _run_worker(args, root, spec, count)
                    cell[stage].append(
                        {"cycle": cycle, "position": position, "arm": arm, "result": result}
                    )
        cell["summary_AA"] = _summarize(cell["AA"])
        cell["summary_AB"] = _summarize(cell["AB"])
        output["cells"].append(cell)
        metadata["updated_utc"] = datetime.now(UTC).isoformat()
        args.output.write_text(json.dumps(output, indent=2) + "\n")
        ratio = cell["summary_AB"]["delivered_per_s"]["cycle_ratio_geomean"]
        print(
            f"{index + 1}/{len(specs)} {spec} n={count} delivered-rate B/A={ratio:.3f}", flush=True
        )
    metadata["completed_utc"] = datetime.now(UTC).isoformat()
    metadata["load_end"] = os.getloadavg()
    args.output.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
