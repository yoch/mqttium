"""Paired MQTTium network benchmark with latency/window diagnostics.

Each sample starts a fresh publisher process and a fresh ``mosquitto_sub``
subscriber. The parent alternates the base and candidate source trees on the
same runner and broker, which makes small source-level regressions distinguishable
from cross-runner noise.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import math
import os
import resource
import statistics
import subprocess
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from itertools import product
from pathlib import Path


HEADER_HEX_BYTES = 32


@dataclass
class WorkerResult:
    completion: str
    protocol: str
    payload_bytes: int
    count: int
    window: int
    observer_qos: int
    publisher_ack_seconds: float
    delivery_seconds: float
    publisher_ack_msg_s: float
    delivered_msg_s: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    ack_latency_p50_ms: float
    ack_latency_p95_ms: float
    ack_latency_p99_ms: float
    cpu_seconds: float
    effect_inline: int
    effect_enqueued: int
    effect_suspensions: int
    self_minor_faults: int = 0
    self_major_faults: int = 0
    self_voluntary_context_switches: int = 0
    self_involuntary_context_switches: int = 0
    gc_collections: tuple[int, int, int] = (0, 0, 0)
    gc_collected: tuple[int, int, int] = (0, 0, 0)


class InvalidMeasurement(RuntimeError):
    """A worker could not produce a trustworthy sample."""


@dataclass
class CallbackGenerationTracker:
    """Match delayed ``on_publish`` callbacks to reused MQTT packet IDs.

    Packet IDs may be reused once the protocol ACK releases them, while the
    application callback is still queued. A scalar ``mid -> timestamp`` map
    therefore loses generations under concurrent publishing. Keep FIFO
    generations per MID so delayed callbacks cannot consume a newer publish.
    """

    starts: dict[int, deque[int]] = field(default_factory=dict)
    early: dict[int, deque[int]] = field(default_factory=dict)

    def register(self, mid: int, sent_ns: int) -> tuple[int, int] | None:
        starts = self.starts.setdefault(mid, deque())
        starts.append(sent_ns)
        early = self.early.get(mid)
        if not early:
            return None
        finished_ns = early.popleft()
        matched_sent_ns = starts.popleft()
        if not starts:
            self.starts.pop(mid, None)
        if not early:
            self.early.pop(mid, None)
        return matched_sent_ns, finished_ns

    def complete(self, mid: int, finished_ns: int) -> tuple[int, int] | None:
        starts = self.starts.get(mid)
        if not starts:
            self.early.setdefault(mid, deque()).append(finished_ns)
            return None
        sent_ns = starts.popleft()
        if not starts:
            self.starts.pop(mid, None)
        return sent_ns, finished_ns

    def pending_counts(self) -> tuple[int, int]:
        return (
            sum(len(starts) for starts in self.starts.values()),
            sum(len(completions) for completions in self.early.values()),
        )


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def make_payload(sequence: int, size: int) -> bytes:
    header = f"{sequence:016x}{time.monotonic_ns():016x}".encode("ascii")
    return header + b"x" * max(0, size - len(header))


@dataclass
class SubscriberProbe:
    client: subprocess.Popen[bytes]
    observer: subprocess.Popen[bytes]

    def abort(self) -> None:
        for process in (self.observer, self.client):
            if process.poll() is None:
                process.kill()
            process.wait()

    def finish(self, timeout: float) -> tuple[list[float], list[int]]:
        try:
            output, observer_error = self.observer.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.observer.kill()
            self.client.kill()
            self.observer.wait()
            self.client.wait()
            raise TimeoutError("subscriber observer timed out") from exc
        try:
            self.client.wait(timeout=2.0)
        except subprocess.TimeoutExpired as exc:
            self.client.kill()
            self.client.wait()
            raise TimeoutError("mosquitto_sub did not stop after observation") from exc
        client_error = b""
        if self.client.stderr is not None:
            client_error = self.client.stderr.read()
        if self.client.returncode or self.observer.returncode:
            diagnostic = (observer_error or client_error or b"no diagnostic").decode(
                errors="replace"
            )
            raise RuntimeError(f"subscriber probe failed: {diagnostic.strip()}")
        try:
            raw = json.loads(output)
            latencies = [float(value) for value in raw["latencies_ms"]]
            sequences = [int(value) for value in raw["sequences"]]
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("subscriber observer returned malformed output") from exc
        return latencies, sequences


def observe_subscriber() -> None:
    """Timestamp subscriber output outside the measured publisher process."""
    latencies: list[float] = []
    sequences: list[int] = []
    for line in sys.stdin.buffer:
        arrived_ns = time.monotonic_ns()
        try:
            sequences.append(int(line[:16], 16))
            sent_ns = int(line[16:HEADER_HEX_BYTES], 16)
        except ValueError as exc:
            raise RuntimeError("subscriber emitted a malformed payload header") from exc
        latencies.append((arrived_ns - sent_ns) / 1_000_000)
    print(json.dumps({"latencies_ms": latencies, "sequences": sequences}))


def start_subscriber(host: str, port: int, topic: str, count: int) -> SubscriberProbe:
    command = [
        "mosquitto_sub",
        "-h",
        host,
        "-p",
        str(port),
        "-t",
        topic,
        "-q",
        # The observer verifies delivery; it must not add a second PUBACK path
        # to the publisher-throughput measurement.
        "0",
        "-C",
        str(count),
        "-F",
        "%p",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None
    observer = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--observer-worker"],
        stdin=process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.stdout.close()
    time.sleep(0.2)
    if process.poll() is not None:
        observer.kill()
        observer.wait()
        assert process.stderr is not None
        raise RuntimeError(process.stderr.read().decode(errors="replace"))
    return SubscriberProbe(process, observer)


async def publish(
    host: str,
    port: int,
    topic: str,
    count: int,
    size: int,
    window: int,
    protocol: str,
    completion: str,
) -> tuple[float, dict[str, int], list[float]]:
    from mqttium.api import AsyncClient, PublishReceipt
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"paired-net-{os.getpid()}-{time.time_ns()}",
        protocol=(MQTTProtocolVersion.MQTTv5 if protocol == "5" else MQTTProtocolVersion.MQTTv311),
        max_outbound_inflight=window,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(host, port, timeout=10.0)
    pending: deque[tuple[PublishReceipt, int]] = deque()
    completed: asyncio.Queue[tuple[int, int]] = asyncio.Queue()
    callback_generations = CallbackGenerationTracker()
    ack_latencies: list[float] = []
    outstanding = 0
    if completion == "callback":

        def on_publish(mid: int | None, *_args: object) -> None:
            assert mid is not None
            finished = time.monotonic_ns()
            match = callback_generations.complete(mid, finished)
            if match is not None:
                completed.put_nowait(match)

        client.on_publish = on_publish
    started = time.perf_counter()
    for sequence in range(count):
        published_ns = time.monotonic_ns()
        receipt = await client.publish(topic, make_payload(sequence, size), qos=1)
        if completion == "receipt":
            pending.append((receipt, published_ns))
            if len(pending) >= window:
                oldest, sent_ns = pending.popleft()
                await oldest.wait()
                ack_latencies.append((time.monotonic_ns() - sent_ns) / 1_000_000)
        else:
            assert receipt.mid is not None
            match = callback_generations.register(receipt.mid, published_ns)
            if match is not None:
                completed.put_nowait(match)
            outstanding += 1
            if outstanding >= window:
                sent_ns, finished = await completed.get()
                ack_latencies.append((finished - sent_ns) / 1_000_000)
                outstanding -= 1
    if completion == "receipt":
        for receipt, sent_ns in pending:
            await receipt.wait()
            ack_latencies.append((time.monotonic_ns() - sent_ns) / 1_000_000)
    else:
        for _ in range(outstanding):
            sent_ns, finished = await completed.get()
            ack_latencies.append((finished - sent_ns) / 1_000_000)
        pending_starts, early_callbacks = callback_generations.pending_counts()
        if pending_starts or early_callbacks:
            raise RuntimeError(
                "callback generation accounting leaked "
                f"starts={pending_starts} early={early_callbacks}"
            )
    elapsed = time.perf_counter() - started
    effects = client.stats().effects
    await client.disconnect()
    return (
        elapsed,
        {
            "inline": effects.inline_effects,
            "enqueued": effects.enqueued,
            "suspensions": effects.apply_suspensions,
        },
        ack_latencies,
    )


def worker(args: argparse.Namespace) -> None:
    topic = f"bench/paired/{os.getpid()}/{time.time_ns()}"
    subscriber = start_subscriber(args.host, args.port, topic, args.count)
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            subscriber.abort()
            raise RuntimeError(f"cannot pin publisher worker to CPU {args.cpu}") from exc
    delivery_started = time.perf_counter()
    cpu_started = time.process_time()
    usage_started = resource.getrusage(resource.RUSAGE_SELF)
    gc_started = gc.get_stats()
    try:
        ack_seconds, effects, ack_latencies = asyncio.run(
            publish(
                args.host,
                args.port,
                topic,
                args.count,
                args.payload_bytes,
                args.window,
                args.protocol,
                args.completion,
            )
        )
    except BaseException:
        subscriber.abort()
        raise
    latencies, sequences = subscriber.finish(args.timeout)
    if len(latencies) != args.count:
        raise TimeoutError(f"subscriber incomplete {len(latencies)}/{args.count}")

    if sorted(sequences) != list(range(args.count)):
        raise RuntimeError("subscriber payload sequence mismatch")

    delivery_seconds = time.perf_counter() - delivery_started
    usage_finished = resource.getrusage(resource.RUSAGE_SELF)
    gc_finished = gc.get_stats()
    result = WorkerResult(
        completion=args.completion,
        protocol=args.protocol,
        payload_bytes=args.payload_bytes,
        count=args.count,
        window=args.window,
        observer_qos=0,
        publisher_ack_seconds=ack_seconds,
        delivery_seconds=delivery_seconds,
        publisher_ack_msg_s=args.count / ack_seconds,
        delivered_msg_s=args.count / delivery_seconds,
        latency_p50_ms=statistics.median(latencies),
        latency_p95_ms=percentile(latencies, 0.95),
        latency_p99_ms=percentile(latencies, 0.99),
        ack_latency_p50_ms=statistics.median(ack_latencies),
        ack_latency_p95_ms=percentile(ack_latencies, 0.95),
        ack_latency_p99_ms=percentile(ack_latencies, 0.99),
        cpu_seconds=time.process_time() - cpu_started,
        effect_inline=effects["inline"],
        effect_enqueued=effects["enqueued"],
        effect_suspensions=effects["suspensions"],
        self_minor_faults=usage_finished.ru_minflt - usage_started.ru_minflt,
        self_major_faults=usage_finished.ru_majflt - usage_started.ru_majflt,
        self_voluntary_context_switches=usage_finished.ru_nvcsw - usage_started.ru_nvcsw,
        self_involuntary_context_switches=usage_finished.ru_nivcsw - usage_started.ru_nivcsw,
        gc_collections=tuple(
            after["collections"] - before["collections"]
            for before, after in zip(gc_started, gc_finished, strict=True)
        ),
        gc_collected=tuple(
            after["collected"] - before["collected"]
            for before, after in zip(gc_started, gc_finished, strict=True)
        ),
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
    completion: str,
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
        "--completion",
        completion,
        "--timeout",
        str(args.timeout),
    ]
    cpu = getattr(args, "cpu", None)
    if cpu is not None:
        command.extend(("--cpu", str(cpu)))
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=env,
            timeout=args.timeout + 30,
        )
    except subprocess.TimeoutExpired as exc:
        raise InvalidMeasurement(
            f"worker timed out after {args.timeout + 30:.1f}s: {' '.join(command)}"
        ) from exc
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no worker output").strip()
        raise InvalidMeasurement(f"worker exited {completed.returncode}: {diagnostic[-2000:]}")
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        return WorkerResult(**json.loads(lines[-1]))
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise InvalidMeasurement(
            f"worker returned malformed output: {completed.stdout[-2000:]!r}"
        ) from exc


def median(values: list[float]) -> float:
    return statistics.median(values)


def coefficient_of_variation(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def arm_variability(
    label: str,
    base_values: list[float],
    candidate_values: list[float],
    maximum_cv: float,
) -> tuple[float, float, list[str]]:
    """Validate measurement stability symmetrically across both paired arms."""
    base_cv = coefficient_of_variation(base_values)
    candidate_cv = coefficient_of_variation(candidate_values)
    invalidations: list[str] = []
    if base_cv > maximum_cv:
        invalidations.append(f"{label}: baseline CV {base_cv:.2%}")
    if candidate_cv > maximum_cv:
        invalidations.append(f"{label}: candidate CV {candidate_cv:.2%}")
    return base_cv, candidate_cv, invalidations


def calibrated_sample_count(
    requested: int, rates: list[float], minimum_seconds: float, maximum: int
) -> int:
    """Choose one bounded count that gives both variants a meaningful sample."""
    return min(maximum, max(requested, math.ceil(max(rates) * minimum_seconds)))


def _eligibility(path: Path | None) -> dict[str, object]:
    if path is None:
        return {
            "checked": False,
            "eligible": None,
            "failures": ["runner preflight was not provided"],
        }
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "checked": True,
            "eligible": False,
            "failures": [f"cannot read runner preflight: {exc}"],
        }
    eligible = report.get("eligible") is True
    raw_failures = report.get("failures", [])
    failures = (
        [str(failure) for failure in raw_failures]
        if isinstance(raw_failures, list)
        else ["runner preflight has an invalid failures field"]
    )
    return {
        "checked": True,
        "eligible": eligible,
        "failures": failures,
        "report": report,
    }


def _evaluation(
    *,
    policy: str,
    eligibility: dict[str, object],
    failures: list[str],
    invalid: bool = False,
) -> tuple[str, int]:
    if eligibility["eligible"] is not True or invalid:
        return "invalid", 2 if policy == "strict" else 0
    if failures:
        return "failed", 1 if policy == "strict" else 0
    return "passed", 0


def _markdown_summary(payload: dict[str, object]) -> str:
    status = str(payload["status"])
    eligibility = payload["eligibility"]
    assert isinstance(eligibility, dict)
    failures = payload["failures"]
    assert isinstance(failures, list)
    lines = [
        "# Paired network evaluation",
        "",
        f"- Policy: `{payload['policy']}`",
        f"- Status: **{status}**",
        f"- Runner eligible: `{eligibility.get('eligible')}`",
        f"- Scenarios: `{len(payload['scenarios'])}`",  # type: ignore[arg-type]
        "",
    ]
    if failures:
        lines.extend(("## Failures", ""))
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
    else:
        lines.extend(("No threshold failure was detected.", ""))
    if status in ("failed", "invalid") and payload["policy"] == "advisory":
        lines.extend(
            (
                "> [!WARNING]",
                f"> The advisory measurement is {status}; it is not release evidence.",
                "",
            )
        )
    return "\n".join(lines)


def _write_evaluation(args: argparse.Namespace, payload: dict[str, object]) -> None:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    summary_output = args.summary_output or args.output.with_suffix(".md")
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.write_text(_markdown_summary(payload), encoding="utf-8")


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    base = args.base_root.resolve()
    candidate = args.candidate_root.resolve()
    windows = [int(value) for value in args.windows.split(",")]
    payloads = [int(value) for value in args.payloads.split(",")]
    protocols = args.protocols.split(",")
    completions = args.completions.split(",")
    payload: dict[str, object] = {
        "base_root": str(base),
        "candidate_root": str(candidate),
        "repeat": args.repeat,
        "windows": windows,
        "payloads": payloads,
        "protocols": protocols,
        "completions": completions,
        "policy": args.policy,
        "thresholds": {
            "max_baseline_cv": args.max_baseline_cv,
            "min_ack_ratio": args.min_ack_ratio,
        },
        "harness": {
            "subscriber_reader": "separate_process",
            "observer_qos": 0,
            "target_sample_seconds": args.target_sample_seconds,
            "maximum_count": args.max_count,
            "publisher_cpu": args.cpu,
        },
        "eligibility": _eligibility(args.preflight_report),
        "status": "running",
        "failures": [],
        "invalidations": [],
        "regressions": [],
        "scenarios": [],
    }

    eligibility = payload["eligibility"]
    assert isinstance(eligibility, dict)
    if args.policy == "strict" and eligibility["eligible"] is not True:
        payload["status"] = "invalid"
        eligibility_failures = list(eligibility["failures"])  # type: ignore[arg-type]
        payload["failures"] = eligibility_failures
        payload["invalidations"] = eligibility_failures
        _write_evaluation(args, payload)
        return 2

    failures: list[str] = []
    invalidations: list[str] = []
    regressions: list[str] = []
    for protocol, payload_bytes, completion in product(protocols, payloads, completions):
        requested_count = args.count_small if payload_bytes <= 256 else args.count_large
        for window in windows:
            calibration: dict[str, WorkerResult] = {}
            for variant, root in (("base", base), ("candidate", candidate)):
                try:
                    calibration[variant] = run_worker(
                        script,
                        root,
                        args,
                        payload_bytes=payload_bytes,
                        count=requested_count,
                        window=window,
                        protocol=protocol,
                        completion=completion,
                    )
                except InvalidMeasurement as exc:
                    label = (
                        f"protocol={protocol} completion={completion} "
                        f"payload={payload_bytes} window={window} calibration={variant}"
                    )
                    failures.append(f"{label}: {exc}")
                    payload["status"] = "invalid"
                    payload["failures"] = failures
                    payload["invalidations"] = failures
                    _write_evaluation(args, payload)
                    print(f"paired network measurement invalid: {failures[-1]}", file=sys.stderr)
                    return 2 if args.policy == "strict" else 0
            count = calibrated_sample_count(
                requested_count,
                [result.publisher_ack_msg_s for result in calibration.values()],
                args.target_sample_seconds,
                args.max_count,
            )
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
                    try:
                        measured[variant] = run_worker(
                            script,
                            root,
                            args,
                            payload_bytes=payload_bytes,
                            count=count,
                            window=window,
                            protocol=protocol,
                            completion=completion,
                        )
                    except InvalidMeasurement as exc:
                        label = (
                            f"protocol={protocol} completion={completion} "
                            f"payload={payload_bytes} window={window} pair={index + 1} "
                            f"variant={variant}"
                        )
                        failures.append(f"{label}: {exc}")
                        payload["status"] = "invalid"
                        payload["failures"] = failures
                        payload["invalidations"] = failures
                        _write_evaluation(args, payload)
                        print(
                            f"paired network measurement invalid: {failures[-1]}", file=sys.stderr
                        )
                        return 2 if args.policy == "strict" else 0
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
            label = (
                f"protocol={protocol} completion={completion} "
                f"payload={payload_bytes} window={window}"
            )
            baseline_cv, candidate_cv, variability_invalidations = arm_variability(
                label, base_ack_rates, candidate_ack_rates, args.max_baseline_cv
            )
            scenario = {
                "protocol": protocol,
                "completion": completion,
                "payload_bytes": payload_bytes,
                "requested_count": requested_count,
                "count": count,
                "window": window,
                "target_sample_seconds": args.target_sample_seconds,
                "calibration": {variant: asdict(result) for variant, result in calibration.items()},
                "median_candidate_over_base_ack": ratio,
                "base_ack_cv": baseline_cv,
                "candidate_ack_cv": candidate_cv,
                "median_candidate_minus_base_p50_ms": median(p50_deltas),
                "median_candidate_minus_base_p95_ms": median(p95_deltas),
                "base_median_p50_ms": median(base_p50),
                "candidate_median_p50_ms": median(candidate_p50),
                "base_median_p95_ms": median(base_p95),
                "candidate_median_p95_ms": median(candidate_p95),
                "pairs": pairs,
            }
            scenarios_output = payload["scenarios"]
            assert isinstance(scenarios_output, list)
            scenarios_output.append(scenario)
            invalidations.extend(variability_invalidations)
            if ratio < args.min_ack_ratio:
                regressions.append(f"{label}: candidate/base {ratio:.4f}")
            print(
                f"protocol={protocol:3s} completion={completion:8s} "
                f"payload={payload_bytes:4d} window={window:3d} "
                f"ack candidate/base={ratio:.4f} base_cv={baseline_cv:.2%} "
                f"candidate_cv={candidate_cv:.2%} "
                f"p50 base={median(base_p50):7.2f}ms "
                f"candidate={median(candidate_p50):7.2f}ms "
                f"delta={median(p50_deltas):+7.2f}ms"
            )

    failures = invalidations + regressions
    status, exit_code = _evaluation(
        policy=args.policy,
        eligibility=eligibility,
        failures=failures,
        invalid=bool(invalidations),
    )
    payload["status"] = status
    payload["failures"] = failures
    payload["invalidations"] = invalidations
    payload["regressions"] = regressions
    _write_evaluation(args, payload)
    if failures:
        message = "paired network thresholds failed:\n" + "\n".join(failures)
        stream = sys.stderr if args.policy == "strict" else sys.stdout
        print(message, file=stream)
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--observer-worker", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--window", type=int, default=20)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--completion", choices=("receipt", "callback"), default="receipt")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--repeat", type=int, default=8, help="even count forming ABBA cycles")
    parser.add_argument("--windows", default="1,8,32,64,128")
    parser.add_argument("--protocols", default="311,5")
    parser.add_argument("--completions", default="receipt")
    parser.add_argument("--payloads", default="64,4096")
    parser.add_argument("--count-small", type=int, default=1_500)
    parser.add_argument("--count-large", type=int, default=750)
    parser.add_argument("--target-sample-seconds", type=float, default=1.5)
    parser.add_argument("--max-count", type=int, default=50_000)
    parser.add_argument(
        "--max-baseline-cv",
        type=float,
        default=0.05,
        help="maximum ACK-rate CV allowed for either paired arm (legacy option name)",
    )
    parser.add_argument("--min-ack-ratio", type=float, default=0.95)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-network.json"))
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args()
    if not (args.worker or args.observer_worker) and (
        args.base_root is None or args.candidate_root is None
    ):
        parser.error("--base-root and --candidate-root are required")
    if not (args.worker or args.observer_worker) and (args.repeat <= 0 or args.repeat % 2):
        parser.error("--repeat must be a positive even count (complete ABBA cycles)")
    if any(value not in ("receipt", "callback") for value in args.completions.split(",")):
        parser.error("--completions accepts receipt,callback")
    if args.target_sample_seconds <= 0:
        parser.error("--target-sample-seconds must be positive")
    if args.max_count <= 0:
        parser.error("--max-count must be positive")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.observer_worker:
        observe_subscriber()
    elif arguments.worker:
        worker(arguments)
    else:
        raise SystemExit(parent(arguments))
