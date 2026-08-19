"""Paired writer-waiter contention microbenchmark.

``WritePump`` producers block on ``enqueue()`` against a deliberately tight
message window. The parent compares fresh base/candidate worker processes in
alternating order. This isolates waiter wakeup policy; it does not replace
``paired_writer_capacity.py``.

Hosted runners are diagnostic only. Strict evidence needs an eligible-runner
preflight, a harness A/A control, and a base-vs-candidate A/B on the same host.
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
class WaiterResult:
    producers: int
    max_messages: int
    payload_bytes: int
    count: int
    elapsed_seconds: float
    completed_rate: float
    cpu_seconds: float
    enqueue_suspensions: int
    wait_p50_ms: float
    wait_p95_ms: float
    wait_p99_ms: float


class _NullTransport:
    async def write(self, data: bytes) -> None:
        return None

    async def write_many(self, parts: list[bytes]) -> None:
        return None

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


def _percentile(ordered: list[float], q: float) -> float:
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    rank = q * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    frac = rank - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


async def _run_phase(
    *,
    producers: int,
    max_messages: int,
    payload_bytes: int,
    count: int,
) -> WaiterResult:
    from mqttium.api._writer import WritePump

    async def _no_failure(exc: BaseException) -> None:
        raise AssertionError(f"unexpected writer failure: {exc!r}")

    pump = WritePump(
        max_bytes=max(1 << 20, payload_bytes * max_messages * 2),
        max_messages=max_messages,
        on_failure=_no_failure,
    )
    payload = b"x" * payload_bytes
    base, extra = divmod(count, producers)

    async def produce(n: int) -> list[float]:
        waits: list[float] = []
        waits_append = waits.append
        enqueue = pump.enqueue
        perf_counter = time.perf_counter
        for _ in range(n):
            started = perf_counter()
            await enqueue(payload)
            waits_append(perf_counter() - started)
        return waits

    pump.start(_NullTransport())
    cpu_started = time.process_time()
    started = time.perf_counter()
    try:
        parts = await asyncio.gather(
            *(produce(base + (1 if index < extra else 0)) for index in range(producers))
        )
        await pump.join()
        elapsed = time.perf_counter() - started
        cpu_seconds = time.process_time() - cpu_started
        stats = pump.stats()
    finally:
        await pump.stop()

    waits = [sample * 1000.0 for part in parts for sample in part]
    waits.sort()
    return WaiterResult(
        producers=producers,
        max_messages=max_messages,
        payload_bytes=payload_bytes,
        count=count,
        elapsed_seconds=elapsed,
        completed_rate=count / max(elapsed, 1e-9),
        cpu_seconds=cpu_seconds,
        enqueue_suspensions=stats.enqueue_suspensions,
        wait_p50_ms=_percentile(waits, 0.50),
        wait_p95_ms=_percentile(waits, 0.95),
        wait_p99_ms=_percentile(waits, 0.99),
    )


async def _sample(args: argparse.Namespace) -> WaiterResult:
    if args.warmup_count:
        await _run_phase(
            producers=args.producers,
            max_messages=args.max_messages,
            payload_bytes=args.payload_bytes,
            count=args.warmup_count,
        )
    return await _run_phase(
        producers=args.producers,
        max_messages=args.max_messages,
        payload_bytes=args.payload_bytes,
        count=args.count,
    )


def worker(args: argparse.Namespace) -> None:
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            raise RuntimeError(f"cannot pin waiter worker to CPU {args.cpu}") from exc
    print(json.dumps(asdict(asyncio.run(_sample(args)))))


def run_worker(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    *,
    producers: int,
    count: int,
) -> WaiterResult:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--producers",
        str(producers),
        "--max-messages",
        str(args.max_messages),
        "--payload-bytes",
        str(args.payload_bytes),
        "--warmup-count",
        str(args.warmup_count),
        "--count",
        str(count),
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
            timeout=args.timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise InvalidMeasurement(
            f"writer-waiter worker timed out after {args.timeout:.1f}s"
        ) from exc
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no worker output").strip()
        raise InvalidMeasurement(
            f"writer-waiter worker exited {completed.returncode}: {diagnostic[-2000:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        return WaiterResult(**json.loads(lines[-1]))
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidMeasurement(
            f"writer-waiter worker returned malformed output: {completed.stdout[-2000:]!r}"
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


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {
        "base": args.base_root.resolve(),
        "candidate": args.candidate_root.resolve(),
    }
    producer_values = [int(value) for value in args.producer_values.split(",")]
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
            "mode": "write_pump_enqueue_waiters",
            "max_messages": args.max_messages,
            "payload_bytes": args.payload_bytes,
            "count": args.count,
            "warmup_count": args.warmup_count,
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

    for producers in producer_values:
        base_rates: list[float] = []
        candidate_rates: list[float] = []
        pairs: list[dict[str, object]] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured: dict[str, WaiterResult] = {}
            for variant in order:
                try:
                    measured[variant] = run_worker(
                        script,
                        roots[variant],
                        args,
                        producers=producers,
                        count=args.count,
                    )
                except InvalidMeasurement as exc:
                    failure = f"producers={producers} pair={index + 1} variant={variant}: {exc}"
                    output["status"] = "invalid"
                    output["failures"] = [failure]
                    output["invalidations"] = [failure]
                    _write_evaluation(args, output)
                    print(f"writer waiter measurement invalid: {failure}", file=sys.stderr)
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
            f"producers={producers} max_messages={args.max_messages} "
            f"payload={args.payload_bytes} count={args.count}"
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
                "producers": producers,
                "max_messages": args.max_messages,
                "payload_bytes": args.payload_bytes,
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
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument(
        "--producer-values",
        default="1,4,16",
        help="Comma-separated concurrency points. 64,256 are optional campaign sizes.",
    )
    parser.add_argument("--max-messages", type=int, default=8)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--warmup-count", type=int, default=2_000)
    parser.add_argument("--count", type=int, default=20_000)
    parser.add_argument("--repeat", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--max-baseline-cv", type=float, default=0.05)
    parser.add_argument("--min-completed-ratio", type=float, default=0.97)
    parser.add_argument("--max-aa-ratio-deviation", type=float, default=0.02)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-writer-waiter.json"))
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()

    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    if args.repeat <= 0 or args.repeat % 2:
        parser.error("--repeat must be a positive even count (complete ABBA cycles)")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.producers <= 0 or args.max_messages <= 0:
        parser.error("--producers and --max-messages must be positive")
    if args.warmup_count < 0 or args.count <= 0:
        parser.error("counts must be positive (warmup may be zero)")
    if args.max_aa_ratio_deviation < 0:
        parser.error("--max-aa-ratio-deviation must be non-negative")
    try:
        producer_values = [int(value) for value in args.producer_values.split(",")]
    except ValueError as exc:
        parser.error("--producer-values accepts comma-separated positive integers")
        raise AssertionError from exc
    if not producer_values or any(value <= 0 for value in producer_values):
        parser.error("--producer-values accepts comma-separated positive integers")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        raise SystemExit(parent(arguments))
