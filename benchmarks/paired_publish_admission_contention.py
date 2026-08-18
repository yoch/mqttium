"""Paired native ``publish()`` admission-contention benchmark.

Exercises protocol-admission wait (several producer tasks, a small inflight
window) rather than the closed-loop ``publish_nowait`` writer-capacity path in
``paired_writer_capacity.py``. The parent always compares fresh base/candidate
worker processes in alternating order.

Hosted runners are diagnostic only; strict evidence additionally needs an
eligible runner preflight and, for a harness change, a separate A/A control.
Do not treat a 256-publisher cell as a default: pass it explicitly.
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

from paired_network import InvalidMeasurement, _eligibility, _evaluation, _write_evaluation


@dataclass(slots=True)
class AdmissionResult:
    protocol: str
    qos: int
    payload_bytes: int
    inflight: int
    publishers: int
    count: int
    elapsed_seconds: float
    completed_rate: float
    cpu_seconds: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    publish_wakeups: int
    publish_wait_retries: int
    min_producer_completed: int
    max_producer_completed: int


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


async def _run_phase(
    client,
    *,
    topic: str,
    payload: bytes,
    qos: int,
    count: int,
    publishers: int,
    timeout: float,
) -> AdmissionResult:
    assigned = 0
    completed = [0] * publishers
    latencies: list[float] = []

    async def producer(index: int) -> None:
        nonlocal assigned
        while True:
            if assigned >= count:
                return
            assigned += 1
            started = time.perf_counter()
            await client.publish(topic, payload, qos=qos)
            latencies.append((time.perf_counter() - started) * 1000.0)
            completed[index] += 1

    loop = asyncio.get_running_loop()
    cpu_started = time.process_time()
    started = time.perf_counter()
    await asyncio.wait_for(
        asyncio.gather(*(producer(index) for index in range(publishers))),
        timeout=timeout,
    )
    elapsed = time.perf_counter() - started
    cpu_seconds = time.process_time() - cpu_started

    write_pump = getattr(client, "_write_pump", None)
    if write_pump is not None:
        await asyncio.wait_for(write_pump.join(), timeout=timeout)

    deadline = loop.time() + timeout
    while client._receipts:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError("timed out draining QoS receipts after admission")
        await asyncio.sleep(0)

    return AdmissionResult(
        protocol="",
        qos=qos,
        payload_bytes=len(payload),
        inflight=0,
        publishers=publishers,
        count=count,
        elapsed_seconds=elapsed,
        completed_rate=count / max(elapsed, 1e-9),
        cpu_seconds=cpu_seconds,
        latency_p50_ms=statistics.median(latencies) if latencies else 0.0,
        latency_p95_ms=percentile(latencies, 0.95),
        latency_p99_ms=percentile(latencies, 0.99),
        publish_wakeups=int(getattr(client, "_publish_wakeups", 0)),
        publish_wait_retries=int(getattr(client, "_publish_wait_retries", 0)),
        min_producer_completed=min(completed) if completed else 0,
        max_producer_completed=max(completed) if completed else 0,
    )


async def _sample(args: argparse.Namespace) -> AdmissionResult:
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    protocol = MQTTProtocolVersion.MQTTv5 if args.protocol == "5" else MQTTProtocolVersion.MQTTv311
    client = AsyncClient(
        client_id=f"publish-admission-{os.getpid()}-{time.time_ns()}",
        protocol=protocol,
        max_outbound_inflight=args.inflight,
        max_pending_outbound_messages=max(args.inflight * 4, args.publishers * 2),
        max_pending_outbound_bytes=64 << 20,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)
    payload = b"x" * args.payload_bytes
    topic = f"bench/publish-admission/{os.getpid()}/{time.time_ns()}"
    try:
        if args.warmup_count:
            await _run_phase(
                client,
                topic=topic,
                payload=payload,
                qos=args.qos,
                count=args.warmup_count,
                publishers=args.publishers,
                timeout=args.timeout,
            )
        result = await _run_phase(
            client,
            topic=topic,
            payload=payload,
            qos=args.qos,
            count=args.count,
            publishers=args.publishers,
            timeout=args.timeout,
        )
        result.protocol = args.protocol
        result.inflight = args.inflight
        return result
    finally:
        await client.disconnect()


def worker(args: argparse.Namespace) -> None:
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            raise RuntimeError(f"cannot pin publisher worker to CPU {args.cpu}") from exc
    print(json.dumps(asdict(asyncio.run(_sample(args)))))


def run_worker(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    *,
    qos: int,
    publishers: int,
    inflight: int,
    count: int,
) -> AdmissionResult:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocol",
        args.protocol,
        "--qos",
        str(qos),
        "--payload-bytes",
        str(args.payload_bytes),
        "--inflight",
        str(inflight),
        "--publishers",
        str(publishers),
        "--warmup-count",
        str(args.warmup_count),
        "--count",
        str(count),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=args.timeout + 30,
        )
    except subprocess.TimeoutExpired as exc:
        raise InvalidMeasurement(
            f"publish-admission worker timed out after {args.timeout + 30:.1f}s"
        ) from exc
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no worker output").strip()
        raise InvalidMeasurement(
            f"publish-admission worker exited {completed.returncode}: {diagnostic[-2000:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        return AdmissionResult(**json.loads(lines[-1]))
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidMeasurement(
            f"publish-admission worker returned malformed output: {completed.stdout[-2000:]!r}"
        ) from exc


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def assess_rates(
    base_rates: list[float],
    candidate_rates: list[float],
    *,
    max_baseline_cv: float,
    min_completed_ratio: float,
    aa_control: bool,
    max_aa_ratio_deviation: float,
    label: str,
) -> tuple[float, float, float, list[str], list[str]]:
    ratios = [candidate / base for base, candidate in zip(base_rates, candidate_rates, strict=True)]
    ratio = statistics.median(ratios)
    baseline_cv = _cv(base_rates)
    candidate_cv = _cv(candidate_rates)
    invalidations: list[str] = []
    regressions: list[str] = []
    if baseline_cv > max_baseline_cv:
        invalidations.append(f"{label}: baseline CV {baseline_cv:.2%}")
    if candidate_cv > max_baseline_cv:
        invalidations.append(f"{label}: candidate CV {candidate_cv:.2%}")
    if aa_control and abs(ratio - 1.0) > max_aa_ratio_deviation:
        invalidations.append(
            f"{label}: A/A completed ratio {ratio:.4f} outside 1+/-{max_aa_ratio_deviation:.2%}"
        )
    if not aa_control and ratio < min_completed_ratio:
        regressions.append(f"{label}: candidate/base completed ratio {ratio:.4f}")
    return ratio, baseline_cv, candidate_cv, invalidations, regressions


def _csv_ints(value: str, *, flag: str) -> list[int]:
    try:
        parsed = [int(item) for item in value.split(",") if item.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{flag} accepts comma-separated integers") from exc
    if not parsed or any(item <= 0 for item in parsed):
        raise argparse.ArgumentTypeError(f"{flag} accepts positive comma-separated integers")
    return parsed


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {
        "base": args.base_root.resolve(),
        "candidate": args.candidate_root.resolve(),
    }
    publisher_values = _csv_ints(args.publisher_values, flag="--publisher-values")
    inflight_values = _csv_ints(args.inflight_values, flag="--inflight-values")
    eligibility = _eligibility(args.preflight_report)
    aa_control = roots["base"] == roots["candidate"]
    output: dict[str, object] = {
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "policy": args.policy,
        "eligibility": eligibility,
        "thresholds": {
            "max_baseline_cv": args.max_baseline_cv,
            "min_completed_ratio": args.min_completed_ratio,
            "max_aa_ratio_deviation": args.max_aa_ratio_deviation,
        },
        "harness": {
            "mode": "native_async_publish_admission",
            "protocol": args.protocol,
            "qos": args.qos,
            "payload_bytes": args.payload_bytes,
            "publisher_values": publisher_values,
            "inflight_values": inflight_values,
            "publisher_cpu": args.cpu,
            "aa_control": aa_control,
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
        output["invalidations"] = output["failures"]
        _write_evaluation(args, output)
        return 2

    invalidations: list[str] = []
    regressions: list[str] = []
    scenarios = output["scenarios"]
    assert isinstance(scenarios, list)

    for publishers in publisher_values:
        for inflight in inflight_values:
            base_rates: list[float] = []
            candidate_rates: list[float] = []
            pairs: list[dict[str, object]] = []
            for index in range(args.repeat):
                order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
                measured: dict[str, AdmissionResult] = {}
                for variant in order:
                    try:
                        measured[variant] = run_worker(
                            script,
                            roots[variant],
                            args,
                            qos=args.qos,
                            publishers=publishers,
                            inflight=inflight,
                            count=args.count,
                        )
                    except InvalidMeasurement as exc:
                        failure = (
                            f"publishers={publishers} inflight={inflight} "
                            f"pair={index + 1} variant={variant}: {exc}"
                        )
                        output["status"] = "invalid"
                        output["failures"] = [failure]
                        output["invalidations"] = [failure]
                        _write_evaluation(args, output)
                        print(
                            f"publish admission measurement invalid: {failure}",
                            file=sys.stderr,
                        )
                        return 2 if args.policy == "strict" else 0
                base_rates.append(measured["base"].completed_rate)
                candidate_rates.append(measured["candidate"].completed_rate)
                pairs.append(
                    {
                        "order": list(order),
                        "base": asdict(measured["base"]),
                        "candidate": asdict(measured["candidate"]),
                        "candidate_over_base_completed": (
                            measured["candidate"].completed_rate / measured["base"].completed_rate
                        ),
                    }
                )

            label = (
                f"protocol={args.protocol} qos={args.qos} payload={args.payload_bytes} "
                f"publishers={publishers} inflight={inflight}"
            )
            ratio, baseline_cv, candidate_cv, cell_invalid, cell_regressions = assess_rates(
                base_rates,
                candidate_rates,
                max_baseline_cv=args.max_baseline_cv,
                min_completed_ratio=args.min_completed_ratio,
                aa_control=aa_control,
                max_aa_ratio_deviation=args.max_aa_ratio_deviation,
                label=label,
            )
            invalidations.extend(cell_invalid)
            regressions.extend(cell_regressions)
            scenarios.append(
                {
                    "protocol": args.protocol,
                    "qos": args.qos,
                    "payload_bytes": args.payload_bytes,
                    "publishers": publishers,
                    "inflight": inflight,
                    "count": args.count,
                    "repeat": args.repeat,
                    "median_candidate_over_base_completed": ratio,
                    "base_completed_cv": baseline_cv,
                    "candidate_completed_cv": candidate_cv,
                    "base_median_completed_rate": statistics.median(base_rates),
                    "candidate_median_completed_rate": statistics.median(candidate_rates),
                    "pairs": pairs,
                }
            )
            print(
                f"{label} candidate/base={ratio:.4f} base_cv={baseline_cv:.2%} "
                f"candidate_cv={candidate_cv:.2%}",
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
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--qos", type=int, choices=(1, 2), default=1)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--inflight", type=int, default=1)
    parser.add_argument("--publishers", type=int, default=4)
    parser.add_argument("--publisher-values", default="1,4,16")
    parser.add_argument("--inflight-values", default="1,4,20")
    parser.add_argument("--warmup-count", type=int, default=200)
    parser.add_argument("--count", type=int, default=4_000)
    parser.add_argument("--repeat", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--max-baseline-cv", type=float, default=0.05)
    parser.add_argument("--min-completed-ratio", type=float, default=0.95)
    parser.add_argument("--max-aa-ratio-deviation", type=float, default=0.02)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/tmp/paired-publish-admission-contention.json"),
    )
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()

    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    if args.repeat <= 0 or args.repeat % 2:
        parser.error("--repeat must be a positive even count (complete ABBA cycles)")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.inflight <= 0 or args.publishers <= 0:
        parser.error("--inflight and --publishers must be positive")
    if args.warmup_count < 0 or args.count <= 0:
        parser.error("counts must be positive (warmup may be zero)")
    if args.max_aa_ratio_deviation < 0:
        parser.error("--max-aa-ratio-deviation must be non-negative")
    try:
        _csv_ints(args.publisher_values, flag="--publisher-values")
        _csv_ints(args.inflight_values, flag="--inflight-values")
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        raise SystemExit(parent(arguments))
