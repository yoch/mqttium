"""Paired, paced QoS 1 latency benchmark for local release evidence."""

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
from itertools import product
from pathlib import Path

from paired_network import (
    _eligibility,
    _evaluation,
    _write_evaluation,
    calibrated_sample_count,
    percentile,
    start_subscriber,
)


@dataclass
class OpenLoopResult:
    mode: str
    completion: str
    protocol: str
    payload_bytes: int
    count: int
    target_rate: float
    offered_rate: float
    completed_rate: float
    completion_ratio: float
    ack_latency_p50_ms: float
    ack_latency_p95_ms: float
    ack_latency_p99_ms: float
    delivery_latency_p50_ms: float
    delivery_latency_p95_ms: float
    delivery_latency_p99_ms: float
    loop_lag_p95_ms: float
    loop_lag_p99_ms: float
    cpu_seconds: float
    effect_inline: int
    effect_enqueued: int
    effect_suspensions: int


def _payload(sequence: int, size: int) -> bytes:
    header = f"{sequence:016x}{time.monotonic_ns():016x}".encode("ascii")
    return header + b"x" * max(0, size - len(header))


async def _connected_client(protocol: str, window: int):
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"open-loop-{os.getpid()}-{time.time_ns()}",
        protocol=(MQTTProtocolVersion.MQTTv5 if protocol == "5" else MQTTProtocolVersion.MQTTv311),
        max_outbound_inflight=window,
        max_pending_outbound_messages=None,
        max_pending_outbound_bytes=None,
        reconnect=ReconnectPolicy(enabled=False),
    )
    return client


async def sample(args: argparse.Namespace, topic: str) -> OpenLoopResult:
    client = await _connected_client(args.protocol, args.window)
    completions: asyncio.Queue[tuple[int, int]] = asyncio.Queue()
    published_ns: dict[int, int] = {}
    early: dict[int, int] = {}
    latencies: list[float] = []
    receipt_tasks: list[asyncio.Task[None]] = []

    async def observe_receipt(receipt, sent_ns: int) -> None:
        await receipt.wait()
        latencies.append((time.monotonic_ns() - sent_ns) / 1_000_000)

    if args.completion == "callback":

        def on_publish(mid: int | None, *_unused: object) -> None:
            assert mid is not None
            finished = time.monotonic_ns()
            if mid in published_ns:
                completions.put_nowait((mid, finished))
            else:
                early[mid] = finished

        client.on_publish = on_publish

    await client.connect(args.host, args.port, timeout=args.timeout)
    loop = asyncio.get_running_loop()
    paced = args.target_rate > 0
    interval = 1.0 / args.target_rate if paced else 0.0
    schedule_lag: list[float] = []
    cpu_started = time.process_time()
    started = loop.time() + (0.05 if paced else 0.0)
    offered_started = 0.0
    try:
        for sequence in range(args.count):
            deadline = started + sequence * interval if paced else loop.time()
            if paced:
                delay = deadline - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
            actual = loop.time()
            if sequence == 0:
                offered_started = actual
            schedule_lag.append(max(0.0, actual - deadline) * 1000)
            sent_ns = time.monotonic_ns()
            receipt = await client.publish(topic, _payload(sequence, args.payload_bytes), qos=1)
            assert receipt.mid is not None
            if args.completion == "receipt":
                receipt_tasks.append(asyncio.create_task(observe_receipt(receipt, sent_ns)))
            else:
                published_ns[receipt.mid] = sent_ns
                if receipt.mid in early:
                    completions.put_nowait((receipt.mid, early.pop(receipt.mid)))
        offered_elapsed = max(loop.time() - offered_started, 1e-9)
        if args.completion == "receipt":
            await asyncio.gather(*receipt_tasks)
        else:
            for _ in range(args.count):
                mid, finished = await asyncio.wait_for(completions.get(), timeout=args.timeout)
                latencies.append((finished - published_ns.pop(mid)) / 1_000_000)
        completed_elapsed = max(loop.time() - offered_started, 1e-9)
        effects = client.stats().effects
        return OpenLoopResult(
            mode="sample",
            completion=args.completion,
            protocol=args.protocol,
            payload_bytes=args.payload_bytes,
            count=args.count,
            target_rate=args.target_rate,
            offered_rate=args.count / offered_elapsed,
            completed_rate=args.count / completed_elapsed,
            completion_ratio=len(latencies) / args.count,
            ack_latency_p50_ms=statistics.median(latencies),
            ack_latency_p95_ms=percentile(latencies, 0.95),
            ack_latency_p99_ms=percentile(latencies, 0.99),
            delivery_latency_p50_ms=0.0,
            delivery_latency_p95_ms=0.0,
            delivery_latency_p99_ms=0.0,
            loop_lag_p95_ms=percentile(schedule_lag, 0.95),
            loop_lag_p99_ms=percentile(schedule_lag, 0.99),
            cpu_seconds=time.process_time() - cpu_started,
            effect_inline=effects.inline_effects,
            effect_enqueued=effects.enqueued,
            effect_suspensions=effects.apply_suspensions,
        )
    finally:
        await client.disconnect()


def worker(args: argparse.Namespace) -> None:
    topic = f"bench/open-loop/{os.getpid()}/{time.time_ns()}"
    subscriber = start_subscriber(args.host, args.port, topic, args.count)
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            subscriber.abort()
            raise RuntimeError(f"cannot pin publisher worker to CPU {args.cpu}") from exc
    try:
        result = asyncio.run(sample(args, topic))
    except BaseException:
        subscriber.abort()
        raise
    delivery_latencies, sequences = subscriber.finish(args.timeout)
    if sorted(sequences) != list(range(args.count)):
        raise RuntimeError(f"subscriber sequence mismatch: {len(sequences)}/{args.count}")
    result.delivery_latency_p50_ms = statistics.median(delivery_latencies)
    result.delivery_latency_p95_ms = percentile(delivery_latencies, 0.95)
    result.delivery_latency_p99_ms = percentile(delivery_latencies, 0.99)
    output = asdict(result)
    if args.mode == "calibrate":
        output["capacity"] = result.completed_rate
    print(json.dumps(output))


def _run_worker(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    *,
    mode: str,
    protocol: str,
    payload_bytes: int,
    completion: str,
    count: int,
    target_rate: float = 0.0,
) -> dict[str, object]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
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
        "--protocol",
        protocol,
        "--payload-bytes",
        str(payload_bytes),
        "--completion",
        completion,
        "--count",
        str(count),
        "--window",
        str(args.window),
        "--target-rate",
        str(target_rate),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
        timeout=args.timeout + max(30, count / max(target_rate, 1.0) * 2),
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _number(mapping: dict[str, object], key: str) -> float:
    value = mapping[key]
    if not isinstance(value, (int, float)):
        raise TypeError(f"worker field {key!r} is not numeric")
    return float(value)


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    protocols = args.protocols.split(",")
    payloads = [int(value) for value in args.payloads.split(",")]
    completions = args.completions.split(",")
    fractions = [float(value) for value in args.fractions.split(",")]
    eligibility = _eligibility(args.preflight_report)
    output: dict[str, object] = {
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "policy": args.policy,
        "eligibility": eligibility,
        "thresholds": {
            "max_baseline_cv": args.max_baseline_cv,
            "min_completed_ratio": args.min_completed_ratio,
            "max_loop_lag_ratio": args.max_loop_lag_ratio,
        },
        "harness": {
            "subscriber_reader": "separate_process",
            "observer_qos": 0,
            "calibration_matches_sample_path": True,
            "publisher_cpu": args.cpu,
            "target_sample_seconds": args.target_sample_seconds,
            "maximum_count": args.max_count,
        },
        "scenarios": [],
        "failures": [],
        "invalidations": [],
        "regressions": [],
        "status": "running",
    }
    if args.policy == "strict" and eligibility["eligible"] is not True:
        output["status"] = "invalid"
        raw_failures = eligibility["failures"]
        output["failures"] = list(raw_failures) if isinstance(raw_failures, list) else []
        _write_evaluation(args, output)
        return 2

    capacities: dict[tuple[str, str, int, str], float] = {}
    for protocol, payload_bytes, completion in product(protocols, payloads, completions):
        calibration_count = args.count_small if payload_bytes <= 256 else args.count_large
        for variant, root in roots.items():
            calibration = _run_worker(
                script,
                root,
                args,
                mode="calibrate",
                protocol=protocol,
                payload_bytes=payload_bytes,
                completion=completion,
                count=calibration_count,
            )
            capacities[(variant, protocol, payload_bytes, completion)] = _number(
                calibration, "capacity"
            )

    invalidations: list[str] = []
    regressions: list[str] = []
    scenarios = output["scenarios"]
    assert isinstance(scenarios, list)
    for protocol, payload_bytes, completion, fraction in product(
        protocols, payloads, completions, fractions
    ):
        count = args.count_small if payload_bytes <= 256 else args.count_large
        target = (
            min(
                capacities[("base", protocol, payload_bytes, completion)],
                capacities[("candidate", protocol, payload_bytes, completion)],
            )
            * fraction
        )
        requested_count = count
        count = calibrated_sample_count(
            requested_count,
            [target],
            args.target_sample_seconds,
            args.max_count,
        )
        samples: dict[str, list[dict[str, object]]] = {"base": [], "candidate": []}
        pairs: list[dict[str, object]] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            pair_results: dict[str, dict[str, object]] = {
                variant: _run_worker(
                    script,
                    roots[variant],
                    args,
                    mode="sample",
                    protocol=protocol,
                    payload_bytes=payload_bytes,
                    completion=completion,
                    count=count,
                    target_rate=target,
                )
                for variant in order
            }
            for variant, result in pair_results.items():
                samples[variant].append(result)
            pairs.append({"order": list(order), **pair_results})
        base_rates = [_number(item, "completed_rate") for item in samples["base"]]
        candidate_rates = [_number(item, "completed_rate") for item in samples["candidate"]]
        throughput_ratios = [
            candidate / base for base, candidate in zip(base_rates, candidate_rates, strict=True)
        ]
        lag_ratios = [
            _number(candidate, "loop_lag_p95_ms") / max(_number(base, "loop_lag_p95_ms"), 1e-9)
            for base, candidate in zip(samples["base"], samples["candidate"], strict=True)
        ]
        base_cv = _cv(base_rates)
        completed_ratio = statistics.median(throughput_ratios)
        loop_lag_ratio = statistics.median(lag_ratios)
        latency_wins = sum(
            _number(candidate, "ack_latency_p50_ms") < _number(base, "ack_latency_p50_ms")
            for base, candidate in zip(samples["base"], samples["candidate"], strict=True)
        )
        scenario: dict[str, object] = {
            "protocol": protocol,
            "payload_bytes": payload_bytes,
            "completion": completion,
            "load_fraction": fraction,
            "requested_count": requested_count,
            "count": count,
            "target_sample_seconds": args.target_sample_seconds,
            "target_rate": target,
            "base_capacity": capacities[("base", protocol, payload_bytes, completion)],
            "candidate_capacity": capacities[("candidate", protocol, payload_bytes, completion)],
            "base_completed_cv": base_cv,
            "median_candidate_over_base_completed": completed_ratio,
            "median_candidate_over_base_loop_lag_p95": loop_lag_ratio,
            "pairs_favouring_candidate_latency": latency_wins,
            "pairs": pairs,
        }
        scenarios.append(scenario)
        label = f"protocol={protocol} payload={payload_bytes} completion={completion} load={fraction:.2f}"
        if base_cv > args.max_baseline_cv:
            invalidations.append(f"{label}: baseline CV {base_cv:.2%}")
        if completed_ratio < args.min_completed_ratio:
            regressions.append(f"{label}: completed ratio {completed_ratio:.4f}")
        if loop_lag_ratio > args.max_loop_lag_ratio:
            regressions.append(f"{label}: loop-lag ratio {loop_lag_ratio:.4f}")
        print(
            f"{label} completed={completed_ratio:.4f} lag={loop_lag_ratio:.4f}",
            flush=True,
        )

    failures = invalidations + regressions
    status, exit_code = _evaluation(
        policy=args.policy,
        eligibility=eligibility,
        failures=failures,
        invalid=bool(invalidations),
    )
    output["status"] = status
    output["failures"] = failures
    output["invalidations"] = invalidations
    output["regressions"] = regressions
    _write_evaluation(args, output)
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--mode", choices=("calibrate", "sample"), default="sample")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--protocols", default="311,5")
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--payloads", default="64,4096")
    parser.add_argument("--completion", choices=("receipt", "callback"), default="receipt")
    parser.add_argument("--completions", default="receipt,callback")
    parser.add_argument("--fractions", default="0.50,0.75,0.90,1.00")
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--count-small", type=int, default=2_000)
    parser.add_argument("--count-large", type=int, default=1_000)
    parser.add_argument("--window", type=int, default=100)
    parser.add_argument("--target-rate", type=float, default=1_000.0)
    parser.add_argument("--target-sample-seconds", type=float, default=1.5)
    parser.add_argument("--max-count", type=int, default=50_000)
    parser.add_argument("--repeat", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--max-baseline-cv", type=float, default=0.05)
    parser.add_argument("--min-completed-ratio", type=float, default=0.97)
    parser.add_argument("--max-loop-lag-ratio", type=float, default=1.05)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-open-loop.json"))
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    if args.repeat <= 0 or args.repeat % 2:
        parser.error("--repeat must be a positive even number")
    if args.worker and args.mode == "sample" and args.target_rate <= 0:
        parser.error("--target-rate must be positive")
    if args.target_sample_seconds <= 0:
        parser.error("--target-sample-seconds must be positive")
    if args.max_count <= 0:
        parser.error("--max-count must be positive")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        raise SystemExit(parent(arguments))
