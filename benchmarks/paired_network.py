"""Paired MQTTium network benchmark with latency/window diagnostics.

Each sample starts a fresh publisher process and a fresh ``mosquitto_sub``
subscriber. The parent alternates the base and candidate source trees on the
same runner and broker, which makes small source-level regressions distinguishable
from cross-runner noise.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path


HEADER_HEX_BYTES = 32


@dataclass
class WorkerResult:
    protocol: str
    payload_bytes: int
    count: int
    window: int
    publisher_ack_msg_s: float
    delivered_msg_s: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    cpu_seconds: float


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def make_payload(sequence: int, size: int) -> bytes:
    header = f"{sequence:016x}{time.monotonic_ns():016x}".encode("ascii")
    return header + b"x" * max(0, size - len(header))


def start_subscriber(host: str, port: int, topic: str, count: int):
    command = [
        "mosquitto_sub",
        "-h",
        host,
        "-p",
        str(port),
        "-t",
        topic,
        "-q",
        "1",
        "-C",
        str(count),
        "-F",
        "%p",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    latencies: list[float] = []
    sequences: list[int] = []

    def read() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            arrived_ns = time.monotonic_ns()
            sequences.append(int(line[:16], 16))
            sent_ns = int(line[16:HEADER_HEX_BYTES], 16)
            latencies.append((arrived_ns - sent_ns) / 1_000_000)

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    time.sleep(0.2)
    if process.poll() is not None:
        assert process.stderr is not None
        raise RuntimeError(process.stderr.read().decode(errors="replace"))
    return process, latencies, sequences, thread


async def publish(
    host: str,
    port: int,
    topic: str,
    count: int,
    size: int,
    window: int,
    protocol: str,
) -> float:
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"paired-net-{os.getpid()}-{time.time_ns()}",
        protocol=(MQTTProtocolVersion.MQTTv5 if protocol == "5" else MQTTProtocolVersion.MQTTv311),
        max_outbound_inflight=window,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(host, port, timeout=10.0)
    pending = []
    started = time.perf_counter()
    for sequence in range(count):
        pending.append(await client.publish(topic, make_payload(sequence, size), qos=1))
        if len(pending) >= window:
            await pending.pop(0).wait()
    await asyncio.gather(*(receipt.wait() for receipt in pending))
    elapsed = time.perf_counter() - started
    await client.disconnect()
    return elapsed


def worker(args: argparse.Namespace) -> None:
    topic = f"bench/paired/{os.getpid()}/{time.time_ns()}"
    process, latencies, sequences, thread = start_subscriber(
        args.host, args.port, topic, args.count
    )
    delivery_started = time.perf_counter()
    cpu_started = time.process_time()
    ack_seconds = asyncio.run(
        publish(
            args.host,
            args.port,
            topic,
            args.count,
            args.payload_bytes,
            args.window,
            args.protocol,
        )
    )
    try:
        process.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    thread.join(timeout=2.0)
    if len(latencies) != args.count:
        assert process.stderr is not None
        detail = process.stderr.read().decode(errors="replace")
        raise TimeoutError(f"subscriber incomplete {len(latencies)}/{args.count}: {detail}")

    if sorted(sequences) != list(range(args.count)):
        raise RuntimeError("subscriber payload sequence mismatch")

    delivery_seconds = time.perf_counter() - delivery_started
    result = WorkerResult(
        protocol=args.protocol,
        payload_bytes=args.payload_bytes,
        count=args.count,
        window=args.window,
        publisher_ack_msg_s=args.count / ack_seconds,
        delivered_msg_s=args.count / delivery_seconds,
        latency_p50_ms=statistics.median(latencies),
        latency_p95_ms=percentile(latencies, 0.95),
        latency_p99_ms=percentile(latencies, 0.99),
        cpu_seconds=time.process_time() - cpu_started,
    )
    print(json.dumps(asdict(result)))


def run_worker(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    *,
    payload_bytes: int,
    count: int,
    window: int,
    protocol: str,
) -> WorkerResult:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--payload-bytes",
        str(payload_bytes),
        "--count",
        str(count),
        "--window",
        str(window),
        "--protocol",
        protocol,
        "--timeout",
        str(args.timeout),
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
        timeout=args.timeout + 30,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return WorkerResult(**json.loads(lines[-1]))


def median(values: list[float]) -> float:
    return statistics.median(values)


def coefficient_of_variation(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    base = args.base_root.resolve()
    candidate = args.candidate_root.resolve()
    windows = [int(value) for value in args.windows.split(",")]
    payloads = [int(value) for value in args.payloads.split(",")]
    protocols = args.protocols.split(",")
    payload: dict[str, object] = {
        "base_root": str(base),
        "candidate_root": str(candidate),
        "repeat": args.repeat,
        "windows": windows,
        "payloads": payloads,
        "protocols": protocols,
        "scenarios": [],
    }

    failures: list[str] = []
    for protocol, payload_bytes in product(protocols, payloads):
        count = args.count_small if payload_bytes <= 256 else args.count_large
        for window in windows:
            pairs: list[dict[str, object]] = []
            ack_ratios: list[float] = []
            base_ack_rates: list[float] = []
            candidate_ack_rates: list[float] = []
            p50_deltas: list[float] = []
            p95_deltas: list[float] = []
            base_p50: list[float] = []
            candidate_p50: list[float] = []
            base_p95: list[float] = []
            candidate_p95: list[float] = []
            for index in range(args.repeat):
                order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
                measured: dict[str, WorkerResult] = {}
                for variant in order:
                    root = base if variant == "base" else candidate
                    measured[variant] = run_worker(
                        script,
                        root,
                        args,
                        payload_bytes=payload_bytes,
                        count=count,
                        window=window,
                        protocol=protocol,
                    )
                base_result = measured["base"]
                candidate_result = measured["candidate"]
                ack_ratio = candidate_result.publisher_ack_msg_s / base_result.publisher_ack_msg_s
                p50_delta = candidate_result.latency_p50_ms - base_result.latency_p50_ms
                p95_delta = candidate_result.latency_p95_ms - base_result.latency_p95_ms
                ack_ratios.append(ack_ratio)
                base_ack_rates.append(base_result.publisher_ack_msg_s)
                candidate_ack_rates.append(candidate_result.publisher_ack_msg_s)
                p50_deltas.append(p50_delta)
                p95_deltas.append(p95_delta)
                base_p50.append(base_result.latency_p50_ms)
                candidate_p50.append(candidate_result.latency_p50_ms)
                base_p95.append(base_result.latency_p95_ms)
                candidate_p95.append(candidate_result.latency_p95_ms)
                pairs.append(
                    {
                        "order": list(order),
                        "base": asdict(base_result),
                        "candidate": asdict(candidate_result),
                        "candidate_over_base_ack": ack_ratio,
                        "candidate_minus_base_p50_ms": p50_delta,
                        "candidate_minus_base_p95_ms": p95_delta,
                    }
                )

            ratio = median(ack_ratios)
            baseline_cv = coefficient_of_variation(base_ack_rates)
            scenario = {
                "protocol": protocol,
                "payload_bytes": payload_bytes,
                "count": count,
                "window": window,
                "median_candidate_over_base_ack": ratio,
                "base_ack_cv": baseline_cv,
                "candidate_ack_cv": coefficient_of_variation(candidate_ack_rates),
                "median_candidate_minus_base_p50_ms": median(p50_deltas),
                "median_candidate_minus_base_p95_ms": median(p95_deltas),
                "base_median_p50_ms": median(base_p50),
                "candidate_median_p50_ms": median(candidate_p50),
                "base_median_p95_ms": median(base_p95),
                "candidate_median_p95_ms": median(candidate_p95),
                "pairs": pairs,
            }
            payload["scenarios"].append(scenario)  # type: ignore[index]
            label = f"protocol={protocol} payload={payload_bytes} window={window}"
            if baseline_cv > args.max_baseline_cv:
                failures.append(f"{label}: baseline CV {baseline_cv:.2%}")
            if ratio < args.min_ack_ratio:
                failures.append(f"{label}: candidate/base {ratio:.4f}")
            print(
                f"protocol={protocol:3s} payload={payload_bytes:4d} window={window:3d} "
                f"ack candidate/base={ratio:.4f} base_cv={baseline_cv:.2%} "
                f"p50 base={median(base_p50):7.2f}ms "
                f"candidate={median(candidate_p50):7.2f}ms "
                f"delta={median(p50_deltas):+7.2f}ms"
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    if failures:
        raise SystemExit("paired network gate failed:\n" + "\n".join(failures))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--window", type=int, default=20)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--windows", default="1,8,32,64,128")
    parser.add_argument("--protocols", default="311,5")
    parser.add_argument("--payloads", default="64,4096")
    parser.add_argument("--count-small", type=int, default=1_500)
    parser.add_argument("--count-large", type=int, default=750)
    parser.add_argument("--max-baseline-cv", type=float, default=0.05)
    parser.add_argument("--min-ack-ratio", type=float, default=0.95)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-network.json"))
    args = parser.parse_args()
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        parent(arguments)
