"""Paired closed-loop ``publish_nowait`` capacity benchmark.

This benchmark exists to protect the writer regime that paced/open-loop tests do
not exercise.  A capacity producer submits synchronously on the client's own
event loop, yields once per application outstanding window, and yields/retries
when MQTTium applies synchronous backpressure.  That is the same scheduling
shape used by the external mqtt-python-client-bench native adapter.

The parent always compares fresh base/candidate worker processes in alternating
order.  Hosted runners are diagnostic only; strict evidence additionally needs
an eligible runner preflight and, for a harness change, a separate A/A control.
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
class CapacityResult:
    protocol: str
    qos: int
    payload_bytes: int
    inflight: int
    outstanding: int
    count: int
    elapsed_seconds: float
    completed_rate: float
    cpu_seconds: float
    sync_rejected: int
    drain_seconds: float
    writer_batches: int
    writer_batched_items: int
    writer_batched_bytes: int
    writer_eager_writes: int
    writer_eager_bytes: int


@dataclass(slots=True)
class _PhaseState:
    submitted: int = 0
    completed: int = 0
    sync_rejected: int = 0


def _configure_completion_tracking(
    client, *, qos: int, state: _PhaseState, progress: asyncio.Event
) -> None:
    if qos:

        def on_publish(mid: int | None, *_unused: object) -> None:
            if mid is None:
                return
            state.completed += 1
            progress.set()

        client.on_publish = on_publish
    else:
        # Installing on_publish disables the native direct-QoS0 fast path.
        client.on_publish = None


async def _wait_outstanding_below(
    state: _PhaseState,
    progress: asyncio.Event,
    limit: int,
) -> None:
    while state.submitted - state.completed >= limit:
        # A completion can race the clear. Recheck afterwards, exactly as other
        # event-based progress waits in the client do.
        progress.clear()
        if state.submitted - state.completed < limit:
            break
        await progress.wait()


async def _run_phase(
    client,
    *,
    topic: str,
    payload: bytes,
    qos: int,
    count: int,
    outstanding: int,
    timeout: float,
) -> CapacityResult:
    """Run one fixed-work capacity phase on the already-connected client."""
    from mqttium.errors import FlowControlError

    state = _PhaseState()
    progress = asyncio.Event()
    _configure_completion_tracking(client, qos=qos, state=state, progress=progress)
    write_pump = getattr(client, "_write_pump", None)
    writer_before = write_pump.stats() if write_pump is not None else None

    loop = asyncio.get_running_loop()
    cpu_started = time.process_time()
    started = time.perf_counter()
    since_yield = 0
    while state.submitted < count:
        if qos and state.submitted - state.completed >= outstanding:
            await _wait_outstanding_below(state, progress, outstanding)
            since_yield = 0
            continue
        try:
            client.publish_nowait(topic, payload, qos=qos)
        except FlowControlError:
            # Closed-loop backpressure means "not admitted yet", not a failed
            # publication. Yield once so the reader/writer can make progress,
            # then retry the same unit of work.
            state.sync_rejected += 1
            await asyncio.sleep(0)
            since_yield = 0
            continue

        state.submitted += 1
        if qos == 0:
            # MQTTium's native QoS0 completion contract is successful local
            # admission/handoff, which is also what the external capacity
            # harness counts. The untimed drain below verifies that queued
            # writes are nevertheless drainable.
            state.completed += 1
        since_yield += 1
        if since_yield >= outstanding:
            since_yield = 0
            await asyncio.sleep(0)

    if qos:
        deadline = loop.time() + timeout
        while state.completed < count:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(f"QoS {qos} completion timeout {state.completed}/{count}")
            progress.clear()
            if state.completed >= count:
                break
            await asyncio.wait_for(progress.wait(), timeout=remaining)

    elapsed = time.perf_counter() - started
    cpu_seconds = time.process_time() - cpu_started

    drain_started = time.perf_counter()
    if write_pump is not None:
        await asyncio.wait_for(write_pump.join(), timeout=timeout)
    drain_seconds = time.perf_counter() - drain_started
    writer_after = write_pump.stats() if write_pump is not None else None

    def writer_delta(name: str) -> int:
        if writer_before is None or writer_after is None:
            return 0
        return int(getattr(writer_after, name) - getattr(writer_before, name))

    return CapacityResult(
        protocol="",
        qos=qos,
        payload_bytes=len(payload),
        inflight=0,
        outstanding=outstanding,
        count=count,
        elapsed_seconds=elapsed,
        completed_rate=count / max(elapsed, 1e-9),
        cpu_seconds=cpu_seconds,
        sync_rejected=state.sync_rejected,
        drain_seconds=drain_seconds,
        writer_batches=writer_delta("batches"),
        writer_batched_items=writer_delta("batched_items"),
        writer_batched_bytes=writer_delta("batched_bytes"),
        writer_eager_writes=writer_delta("eager_writes"),
        writer_eager_bytes=writer_delta("eager_bytes"),
    )


async def _sample(args: argparse.Namespace) -> CapacityResult:
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    protocol = MQTTProtocolVersion.MQTTv5 if args.protocol == "5" else MQTTProtocolVersion.MQTTv311
    client = AsyncClient(
        client_id=f"writer-capacity-{os.getpid()}-{time.time_ns()}",
        protocol=protocol,
        max_outbound_inflight=args.inflight,
        max_pending_outbound_messages=args.max_queued,
        max_pending_outbound_bytes=64 << 20,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=args.timeout)
    payload = b"x" * args.payload_bytes
    topic = f"bench/writer-capacity/{os.getpid()}/{time.time_ns()}"
    try:
        if args.warmup_count:
            await _run_phase(
                client,
                topic=topic,
                payload=payload,
                qos=args.qos,
                count=args.warmup_count,
                outstanding=args.outstanding,
                timeout=args.timeout,
            )
        result = await _run_phase(
            client,
            topic=topic,
            payload=payload,
            qos=args.qos,
            count=args.count,
            outstanding=args.outstanding,
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
    count: int,
) -> CapacityResult:
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
        str(args.inflight),
        "--outstanding",
        str(args.outstanding),
        "--max-queued",
        str(args.max_queued),
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
            f"writer-capacity worker timed out after {args.timeout + 30:.1f}s"
        ) from exc
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no worker output").strip()
        raise InvalidMeasurement(
            f"writer-capacity worker exited {completed.returncode}: {diagnostic[-2000:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        return CapacityResult(**json.loads(lines[-1]))
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidMeasurement(
            f"writer-capacity worker returned malformed output: {completed.stdout[-2000:]!r}"
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
    """Evaluate one paired cell; split invalid measurement from regression."""
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
    qos_values = [int(value) for value in args.qos_values.split(",")]
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
            "mode": "closed_loop_publish_nowait",
            "protocol": args.protocol,
            "payload_bytes": args.payload_bytes,
            "inflight": args.inflight,
            "outstanding": args.outstanding,
            "max_queued": args.max_queued,
            "yield_every": args.outstanding,
            "backpressure": "yield_then_retry",
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

    for qos in qos_values:
        count = args.count_qos0 if qos == 0 else args.count_qos1
        base_rates: list[float] = []
        candidate_rates: list[float] = []
        pairs: list[dict[str, object]] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured: dict[str, CapacityResult] = {}
            for variant in order:
                try:
                    measured[variant] = run_worker(
                        script,
                        roots[variant],
                        args,
                        qos=qos,
                        count=count,
                    )
                except InvalidMeasurement as exc:
                    failure = f"qos={qos} pair={index + 1} variant={variant}: {exc}"
                    output["status"] = "invalid"
                    output["failures"] = [failure]
                    output["invalidations"] = [failure]
                    _write_evaluation(args, output)
                    print(f"writer capacity measurement invalid: {failure}", file=sys.stderr)
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
            f"protocol={args.protocol} qos={qos} payload={args.payload_bytes} "
            f"inflight={args.inflight} outstanding={args.outstanding}"
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
                "qos": qos,
                "payload_bytes": args.payload_bytes,
                "inflight": args.inflight,
                "outstanding": args.outstanding,
                "count": count,
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
    parser.add_argument("--qos", type=int, choices=(0, 1), default=0)
    parser.add_argument("--qos-values", default="0,1")
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--inflight", type=int, default=20)
    parser.add_argument("--outstanding", type=int, default=64)
    parser.add_argument("--max-queued", type=int, default=200)
    parser.add_argument("--warmup-count", type=int, default=5_000)
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--count-qos0", type=int, default=100_000)
    parser.add_argument("--count-qos1", type=int, default=40_000)
    parser.add_argument("--repeat", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--max-baseline-cv", type=float, default=0.05)
    parser.add_argument("--min-completed-ratio", type=float, default=0.95)
    parser.add_argument("--max-aa-ratio-deviation", type=float, default=0.02)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-writer-capacity.json"))
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()

    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    if args.repeat <= 0 or args.repeat % 2:
        parser.error("--repeat must be a positive even count (complete ABBA cycles)")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be non-negative")
    if args.inflight <= 0 or args.outstanding <= 0 or args.max_queued < 0:
        parser.error("--inflight/--outstanding must be positive and --max-queued non-negative")
    if args.warmup_count < 0 or args.count <= 0 or args.count_qos0 <= 0 or args.count_qos1 <= 0:
        parser.error("counts must be positive (warmup may be zero)")
    try:
        qos_values = [int(value) for value in args.qos_values.split(",")]
    except ValueError as exc:
        parser.error("--qos-values accepts comma-separated 0,1")
        raise AssertionError from exc
    if not qos_values or any(value not in (0, 1) for value in qos_values):
        parser.error("--qos-values accepts comma-separated 0,1")
    if args.max_aa_ratio_deviation < 0:
        parser.error("--max-aa-ratio-deviation must be non-negative")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        raise SystemExit(parent(arguments))
