"""Release-grade open-loop gate with baseline-anchored load and targeted confirmation.

``paired_open_loop.py`` remains the low-level worker. This module owns release
policy: baseline-only capacity calibration, identical absolute A/B load,
balanced ABBA acquisition, and bounded confirmation of suspicious cells.

Raw per-arm latency variability is retained as diagnostic evidence. Loop-lag
regressions are only release-blocking when a relative signal survives extra
A/B samples and its additive increase exceeds same-code A/A noise measured at
the same target rate.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path
from typing import Any

from network_release_gate import PairEstimate, _t_critical_95, paired_ratio_estimate
from paired_network import calibrated_sample_count


DEFAULT_FRACTIONS = (0.50, 0.75, 0.90, 1.00)


@dataclass(frozen=True, slots=True)
class LoadPoint:
    mode: str
    value: float


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    protocol: str
    payload_bytes: int
    completion: str
    window: int

    @property
    def key(self) -> str:
        return (
            f"protocol={self.protocol} payload={self.payload_bytes} "
            f"completion={self.completion} window={self.window}"
        )


@dataclass(frozen=True, slots=True)
class Calibration:
    baseline_capacity: float
    candidate_capacity: float
    count: int
    baseline_samples: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class DifferenceEstimate:
    pairs: int
    cycles: int
    mean_ms: float
    median_cycle_ms: float
    lower_95_ms: float
    upper_95_ms: float
    cycle_standard_deviation_ms: float


@dataclass(frozen=True, slots=True)
class Metrics:
    pairs: int
    throughput_median: float
    throughput: PairEstimate
    loop_lag_median_ratio: float
    loop_lag_ratio: PairEstimate
    loop_lag_delta: DifferenceEstimate
    base_ack_p50_cv: float
    candidate_ack_p50_cv: float


def coefficient_of_variation(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _csv_strings(raw: str) -> list[str]:
    values = [field.strip() for field in raw.split(",") if field.strip()]
    if not values:
        raise ValueError("comma-separated option cannot be empty")
    return values


def _csv_ints(raw: str, *, option: str) -> list[int]:
    values: list[int] = []
    for field in _csv_strings(raw):
        try:
            value = int(field)
        except ValueError as exc:
            raise ValueError(f"{option} accepts comma-separated integers") from exc
        if value <= 0:
            raise ValueError(f"{option} values must be positive")
        values.append(value)
    return values


def _csv_floats(raw: str | None, *, option: str) -> list[float]:
    if raw is None:
        return []
    values: list[float] = []
    for field in _csv_strings(raw):
        try:
            value = float(field)
        except ValueError as exc:
            raise ValueError(f"{option} accepts comma-separated numbers") from exc
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{option} values must be positive and finite")
        values.append(value)
    return values


def _load_points(fractions: str | None, target_rates: str | None) -> list[LoadPoint]:
    rates = _csv_floats(target_rates, option="--target-rates")
    if fractions is None:
        fraction_values = [] if rates else list(DEFAULT_FRACTIONS)
    else:
        fraction_values = _csv_floats(fractions, option="--fractions")
    points = [LoadPoint("baseline_capacity_fraction", value) for value in fraction_values]
    points.extend(LoadPoint("absolute_rate", value) for value in rates)
    if not points:
        raise ValueError("at least one fraction or absolute target rate is required")
    return points


def _parse_seed_schedule(value: str, *, minimum: int = 1) -> list[int]:
    seeds: list[int] = []
    for field in _csv_strings(value):
        try:
            seed = int(field)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid deterministic hash seed: {field!r}") from exc
        if not 0 <= seed <= 4_294_967_295:
            raise argparse.ArgumentTypeError("hash seeds must fit PYTHONHASHSEED range")
        seeds.append(seed)
    if len(seeds) < minimum:
        raise argparse.ArgumentTypeError(f"at least {minimum} hash seed(s) are required")
    if len(set(seeds)) != len(seeds):
        raise argparse.ArgumentTypeError("hash seed schedule must not contain duplicates")
    return seeds


def abba_cycle_differences(differences: list[float], orders: list[list[str]]) -> list[float]:
    """Collapse opposite-order pairs into additive ABBA cycle estimates."""
    if len(differences) != len(orders) or len(differences) < 4 or len(differences) % 2:
        raise ValueError("ABBA difference evaluation requires an even number of at least four pairs")
    cycles: list[float] = []
    for index in range(0, len(differences), 2):
        if orders[index] != ["base", "candidate"] or orders[index + 1] != [
            "candidate",
            "base",
        ]:
            raise ValueError("paired measurements do not form complete ABBA cycles")
        first = differences[index]
        second = differences[index + 1]
        if not math.isfinite(first) or not math.isfinite(second):
            raise ValueError("paired differences must be finite")
        cycles.append((first + second) / 2.0)
    return cycles


def paired_difference_estimate(
    differences: list[float], orders: list[list[str]]
) -> DifferenceEstimate:
    cycles = abba_cycle_differences(differences, orders)
    mean = statistics.fmean(cycles)
    sd = statistics.stdev(cycles) if len(cycles) > 1 else 0.0
    half_width = (
        _t_critical_95(len(cycles) - 1) * sd / math.sqrt(len(cycles))
        if len(cycles) > 1
        else 0.0
    )
    return DifferenceEstimate(
        pairs=len(differences),
        cycles=len(cycles),
        mean_ms=mean,
        median_cycle_ms=statistics.median(cycles),
        lower_95_ms=mean - half_width,
        upper_95_ms=mean + half_width,
        cycle_standard_deviation_ms=sd,
    )


def calibration_count(
    pilot_capacity: float,
    pilot_count: int,
    target_seconds: float,
    max_count: int,
) -> int:
    if not math.isfinite(pilot_capacity) or pilot_capacity <= 0:
        raise ValueError("pilot capacity must be positive and finite")
    requested = math.ceil(pilot_capacity * target_seconds)
    return max(pilot_count, min(max_count, requested))


def target_rate(calibration: Calibration, point: LoadPoint) -> float:
    """Return a fixed rate that candidate capacity cannot lower."""
    if point.mode == "baseline_capacity_fraction":
        return calibration.baseline_capacity * point.value
    if point.mode == "absolute_rate":
        return point.value
    raise ValueError(f"unsupported load mode: {point.mode}")


def _git_sha(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _worker_command(
    args: argparse.Namespace,
    *,
    root: Path,
    spec: ScenarioSpec,
    mode: str,
    count: int,
    target: float,
) -> tuple[list[str], dict[str, str]]:
    command = [
        sys.executable,
        str(args.engine),
        "--worker",
        "--mode",
        mode,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocol",
        spec.protocol,
        "--payload-bytes",
        str(spec.payload_bytes),
        "--completion",
        spec.completion,
        "--window",
        str(spec.window),
        "--count",
        str(count),
        "--target-rate",
        str(target),
        "--timeout",
        str(args.timeout),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    return command, environment


def _run_worker(
    args: argparse.Namespace,
    *,
    root: Path,
    spec: ScenarioSpec,
    mode: str,
    count: int,
    target: float,
    hash_seed: int,
) -> dict[str, Any]:
    command, environment = _worker_command(
        args,
        root=root,
        spec=spec,
        mode=mode,
        count=count,
        target=target,
    )
    environment["PYTHONHASHSEED"] = str(hash_seed)
    timeout = args.timeout + max(30.0, count / max(target, 1.0) * 2.0)
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{spec.key}: worker timed out after {timeout:.1f}s") from exc
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no worker output").strip()
        raise RuntimeError(
            f"{spec.key}: worker exited {completed.returncode}: {diagnostic[-2000:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        payload = json.loads(lines[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"{spec.key}: worker returned malformed output: {completed.stdout[-2000:]!r}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{spec.key}: worker result is not an object")
    return payload


def _number(mapping: dict[str, Any], key: str) -> float:
    value = mapping.get(key)
    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise RuntimeError(f"worker field {key!r} is not finite numeric data")
    return float(value)


def _fresh_preflight(args: argparse.Namespace, *, label: str, raw_dir: Path) -> Path:
    output = raw_dir / f"{label}-preflight.json"
    command = [
        sys.executable,
        str(args.runner_probe),
        "--output",
        str(output),
        "--wait-seconds",
        str(args.preflight_wait_seconds),
        "--poll-seconds",
        str(args.preflight_poll_seconds),
        "--consecutive-eligible",
        str(args.preflight_consecutive_eligible),
        "--enforce",
    ]
    if args.require_temperature:
        command.append("--require-temperature")
    completed = subprocess.run(command, check=False)
    if completed.returncode:
        raise RuntimeError(f"{label}: runner did not requalify")
    try:
        report = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label}: runner preflight report is unreadable") from exc
    if not isinstance(report, dict) or report.get("eligible") is not True:
        raise RuntimeError(f"{label}: runner preflight is not eligible")
    return output


def _scenario_specs(args: argparse.Namespace) -> list[ScenarioSpec]:
    return [
        ScenarioSpec(protocol, payload, completion, window)
        for protocol, payload, completion, window in product(
            _csv_strings(args.protocols),
            _csv_ints(args.payloads, option="--payloads"),
            _csv_strings(args.completions),
            _csv_ints(args.windows, option="--windows"),
        )
    ]


def _calibrate_one(
    args: argparse.Namespace,
    *,
    spec: ScenarioSpec,
    base_root: Path,
    candidate_root: Path,
) -> Calibration:
    pilot_count = args.count_small if spec.payload_bytes <= 256 else args.count_large
    pilot = _run_worker(
        args,
        root=base_root,
        spec=spec,
        mode="calibrate",
        count=pilot_count,
        target=0.0,
        hash_seed=args.calibration_seeds[0],
    )
    final_count = calibration_count(
        _number(pilot, "capacity"),
        pilot_count,
        args.calibration_seconds,
        args.max_count,
    )
    baseline_samples: list[float] = []
    for index in range(args.calibration_repeats):
        sample = _run_worker(
            args,
            root=base_root,
            spec=spec,
            mode="calibrate",
            count=final_count,
            target=0.0,
            hash_seed=args.calibration_seeds[index % len(args.calibration_seeds)],
        )
        baseline_samples.append(_number(sample, "capacity"))
    candidate_sample = _run_worker(
        args,
        root=candidate_root,
        spec=spec,
        mode="calibrate",
        count=final_count,
        target=0.0,
        hash_seed=args.calibration_seeds[-1],
    )
    return Calibration(
        baseline_capacity=statistics.median(baseline_samples),
        candidate_capacity=_number(candidate_sample, "capacity"),
        count=final_count,
        baseline_samples=tuple(baseline_samples),
    )


def _acquire_cycle(
    args: argparse.Namespace,
    *,
    spec: ScenarioSpec,
    base_root: Path,
    candidate_root: Path,
    count: int,
    target: float,
    hash_seed: int,
) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for order in (("base", "candidate"), ("candidate", "base")):
        roots = {"base": base_root, "candidate": candidate_root}
        results: dict[str, dict[str, Any]] = {}
        for variant in order:
            results[variant] = _run_worker(
                args,
                root=roots[variant],
                spec=spec,
                mode="sample",
                count=count,
                target=target,
                hash_seed=hash_seed,
            )
        pairs.append({"order": list(order), **results, "hash_seed": hash_seed})
    return pairs


def _pair_ratios(pairs: list[dict[str, Any]], field: str) -> list[float]:
    ratios: list[float] = []
    for pair in pairs:
        base = _number(pair["base"], field)
        candidate = _number(pair["candidate"], field)
        ratios.append(candidate / max(base, 1e-12))
    return ratios


def _pair_differences(pairs: list[dict[str, Any]], field: str) -> list[float]:
    return [
        _number(pair["candidate"], field) - _number(pair["base"], field) for pair in pairs
    ]


def _orders(pairs: list[dict[str, Any]]) -> list[list[str]]:
    return [list(pair["order"]) for pair in pairs]


def metrics(pairs: list[dict[str, Any]]) -> Metrics:
    if len(pairs) < 4 or len(pairs) % 2:
        raise ValueError("open-loop release metrics require complete ABBA cycles")
    orders = _orders(pairs)
    throughput_ratios = _pair_ratios(pairs, "completed_rate")
    loop_ratios = _pair_ratios(pairs, "loop_lag_p95_ms")
    loop_deltas = _pair_differences(pairs, "loop_lag_p95_ms")
    base_ack = [_number(pair["base"], "ack_latency_p50_ms") for pair in pairs]
    candidate_ack = [_number(pair["candidate"], "ack_latency_p50_ms") for pair in pairs]
    return Metrics(
        pairs=len(pairs),
        throughput_median=statistics.median(throughput_ratios),
        throughput=paired_ratio_estimate(throughput_ratios, orders),
        loop_lag_median_ratio=statistics.median(loop_ratios),
        loop_lag_ratio=paired_ratio_estimate(loop_ratios, orders),
        loop_lag_delta=paired_difference_estimate(loop_deltas, orders),
        base_ack_p50_cv=coefficient_of_variation(base_ack),
        candidate_ack_p50_cv=coefficient_of_variation(candidate_ack),
    )


def control_noise_floor_ms(*control_pairs: list[dict[str, Any]]) -> float:
    cycle_deltas: list[float] = []
    for pairs in control_pairs:
        cycle_deltas.extend(
            abba_cycle_differences(
                _pair_differences(pairs, "loop_lag_p95_ms"),
                _orders(pairs),
            )
        )
    return max((abs(value) for value in cycle_deltas), default=0.0)


def control_is_valid(
    pairs: list[dict[str, Any]],
    *,
    max_throughput_deviation: float,
) -> bool:
    estimate = metrics(pairs).throughput
    return abs(estimate.geometric_mean - 1.0) <= max_throughput_deviation


def confirmed_loop_regression(
    ab_pairs: list[dict[str, Any]],
    *,
    base_control_pairs: list[dict[str, Any]],
    candidate_control_pairs: list[dict[str, Any]],
    max_loop_lag_ratio: float,
) -> tuple[bool, float]:
    """Require ratio evidence and additive effect beyond same-code noise."""
    estimate = metrics(ab_pairs)
    noise_floor = control_noise_floor_ms(base_control_pairs, candidate_control_pairs)
    confirmed = (
        estimate.loop_lag_ratio.geometric_mean > max_loop_lag_ratio
        and estimate.loop_lag_ratio.lower_95 > 1.0
        and estimate.loop_lag_delta.lower_95_ms > noise_floor
    )
    return confirmed, noise_floor


def _load_label(point: LoadPoint) -> str:
    if point.mode == "baseline_capacity_fraction":
        return f"load={point.value:.2f}"
    return f"rate={point.value:g}msg/s"


def _scenario_label(spec: ScenarioSpec, point: LoadPoint) -> str:
    return f"{spec.key} {_load_label(point)}"


def _metrics_dict(value: Metrics) -> dict[str, Any]:
    return asdict(value)


def _write_result(output: Path, payload: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Open-loop release gate",
        "",
        f"- Status: **{payload['status']}**",
        f"- Base: `{payload['base_sha']}`",
        f"- Candidate: `{payload['candidate_sha']}`",
        "- Fractional loads: anchored to baseline capacity only",
        "- Raw ACK p50 CV: diagnostic only",
        "",
    ]
    failures = payload.get("failures", [])
    if failures:
        lines.extend(("## Failures", ""))
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
    invalidations = payload.get("invalidations", [])
    if invalidations:
        lines.extend(("## Invalidations", ""))
        lines.extend(f"- {item}" for item in invalidations)
        lines.append("")
    for scenario in payload.get("scenarios", []):
        final = scenario["final_metrics"]
        lines.append(
            f"- `{scenario['label']}`: throughput={final['throughput_median']:.4f}; "
            f"loop={final['loop_lag_median_ratio']:.4f}; "
            f"loop delta CI=[{final['loop_lag_delta']['lower_95_ms']:.6f}, "
            f"{final['loop_lag_delta']['upper_95_ms']:.6f}] ms; "
            f"confirmation={scenario['confirmation']['status'] if scenario['confirmation'] else 'none'}"
        )
    output.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def parent(args: argparse.Namespace) -> int:
    base_root = args.base_root.resolve()
    candidate_root = args.candidate_root.resolve()
    raw_dir = args.output.parent / f"{args.output.stem}-raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    specs = _scenario_specs(args)
    load_points = _load_points(args.fractions, args.target_rates)
    result: dict[str, Any] = {
        "status": "running",
        "policy": args.policy,
        "base_sha": _git_sha(base_root),
        "candidate_sha": _git_sha(candidate_root),
        "harness": {
            "engine": str(args.engine),
            "publisher_cpu": args.cpu,
            "calibration_source": "baseline_only",
            "calibration_seconds": args.calibration_seconds,
            "calibration_repeats": args.calibration_repeats,
            "initial_pairs_per_scenario": 2 * len(args.initial_cycle_seeds),
            "confirmation_pairs_per_scenario": 2 * len(args.confirmation_cycle_seeds),
            "max_confirmation_scenarios": args.max_confirmation_scenarios,
            "raw_ack_p50_cv": "diagnostic_only",
        },
        "thresholds": {
            "min_completed_ratio": args.min_completed_ratio,
            "max_loop_lag_ratio": args.max_loop_lag_ratio,
            "control_max_throughput_deviation": args.control_max_throughput_deviation,
            "loop_absolute_materiality": "same-code A/A additive noise envelope",
            "loop_confidence": 0.95,
        },
        "calibration": {},
        "scenarios": [],
        "failures": [],
        "invalidations": [],
    }
    if args.policy == "strict" and args.cpu is None:
        result["status"] = "invalid"
        result["invalidations"] = ["strict open-loop release evidence requires --cpu pinning"]
        _write_result(args.output, result)
        return 2
    try:
        _fresh_preflight(args, label="calibration", raw_dir=raw_dir)
        calibrations: dict[ScenarioSpec, Calibration] = {}
        calibration_output = result["calibration"]
        assert isinstance(calibration_output, dict)
        for spec in specs:
            calibration = _calibrate_one(
                args,
                spec=spec,
                base_root=base_root,
                candidate_root=candidate_root,
            )
            calibrations[spec] = calibration
            calibration_output[spec.key] = asdict(calibration)
            print(
                f"{spec.key}: baseline capacity {calibration.baseline_capacity:.1f} msg/s; "
                f"candidate diagnostic {calibration.candidate_capacity:.1f} msg/s",
                flush=True,
            )

        records: list[dict[str, Any]] = []
        scenarios_output = result["scenarios"]
        assert isinstance(scenarios_output, list)
        for protocol in _csv_strings(args.protocols):
            _fresh_preflight(args, label=f"ab-{protocol}", raw_dir=raw_dir)
            for spec in (item for item in specs if item.protocol == protocol):
                calibration = calibrations[spec]
                for point in load_points:
                    target = target_rate(calibration, point)
                    requested_count = (
                        args.count_small if spec.payload_bytes <= 256 else args.count_large
                    )
                    count = calibrated_sample_count(
                        requested_count,
                        [target],
                        args.target_sample_seconds,
                        args.max_count,
                    )
                    pairs: list[dict[str, Any]] = []
                    for seed in args.initial_cycle_seeds:
                        pairs.extend(
                            _acquire_cycle(
                                args,
                                spec=spec,
                                base_root=base_root,
                                candidate_root=candidate_root,
                                count=count,
                                target=target,
                                hash_seed=seed,
                            )
                        )
                    initial = metrics(pairs)
                    record: dict[str, Any] = {
                        "label": _scenario_label(spec, point),
                        "protocol": spec.protocol,
                        "payload_bytes": spec.payload_bytes,
                        "completion": spec.completion,
                        "window": spec.window,
                        "load_mode": point.mode,
                        "load_fraction": (
                            point.value if point.mode == "baseline_capacity_fraction" else None
                        ),
                        "requested_target_rate": (
                            point.value if point.mode == "absolute_rate" else None
                        ),
                        "baseline_capacity": calibration.baseline_capacity,
                        "candidate_capacity_diagnostic": calibration.candidate_capacity,
                        "target_rate": target,
                        "count": count,
                        "initial_metrics": _metrics_dict(initial),
                        "initial_pairs": pairs,
                        "confirmation": None,
                        "final_metrics": _metrics_dict(initial),
                        "throughput_suspect": initial.throughput_median < args.min_completed_ratio,
                        "loop_suspect": initial.loop_lag_median_ratio > args.max_loop_lag_ratio,
                    }
                    records.append(record)
                    scenarios_output.append(record)
                    print(
                        f"{record['label']}: throughput={initial.throughput_median:.4f} "
                        f"loop={initial.loop_lag_median_ratio:.4f}",
                        flush=True,
                    )

        suspects = [
            record
            for record in records
            if record["throughput_suspect"] or record["loop_suspect"]
        ]
        if len(suspects) > args.max_confirmation_scenarios:
            result["status"] = "invalid"
            result["invalidations"] = [
                f"{len(suspects)} cells require confirmation; bounded maximum is "
                f"{args.max_confirmation_scenarios}"
            ]
            _write_result(args.output, result)
            return 2 if args.policy == "strict" else 0

        failures: list[str] = []
        invalidations: list[str] = []
        for index, record in enumerate(suspects, start=1):
            spec = ScenarioSpec(
                protocol=record["protocol"],
                payload_bytes=record["payload_bytes"],
                completion=record["completion"],
                window=record["window"],
            )
            _fresh_preflight(args, label=f"confirm-{index}", raw_dir=raw_dir)
            ab_pairs = list(record["initial_pairs"])
            for seed in args.confirmation_cycle_seeds:
                ab_pairs.extend(
                    _acquire_cycle(
                        args,
                        spec=spec,
                        base_root=base_root,
                        candidate_root=candidate_root,
                        count=record["count"],
                        target=record["target_rate"],
                        hash_seed=seed,
                    )
                )
            final = metrics(ab_pairs)
            confirmation: dict[str, Any] = {
                "status": "confirmed_samples",
                "ab_pairs": ab_pairs,
                "metrics": _metrics_dict(final),
                "base_control": None,
                "candidate_control": None,
                "same_code_noise_floor_ms": None,
            }

            if record["throughput_suspect"] and final.throughput_median < args.min_completed_ratio:
                failures.append(
                    f"{record['label']}: completed ratio {final.throughput_median:.4f} "
                    f"< {args.min_completed_ratio:.4f} after confirmation"
                )

            if record["loop_suspect"] and final.loop_lag_median_ratio > args.max_loop_lag_ratio:
                base_control: list[dict[str, Any]] = []
                candidate_control: list[dict[str, Any]] = []
                for seed in args.control_cycle_seeds:
                    base_control.extend(
                        _acquire_cycle(
                            args,
                            spec=spec,
                            base_root=base_root,
                            candidate_root=base_root,
                            count=record["count"],
                            target=record["target_rate"],
                            hash_seed=seed,
                        )
                    )
                    candidate_control.extend(
                        _acquire_cycle(
                            args,
                            spec=spec,
                            base_root=candidate_root,
                            candidate_root=candidate_root,
                            count=record["count"],
                            target=record["target_rate"],
                            hash_seed=seed,
                        )
                    )
                confirmation["base_control"] = {
                    "pairs": base_control,
                    "metrics": _metrics_dict(metrics(base_control)),
                }
                confirmation["candidate_control"] = {
                    "pairs": candidate_control,
                    "metrics": _metrics_dict(metrics(candidate_control)),
                }
                if not control_is_valid(
                    base_control,
                    max_throughput_deviation=args.control_max_throughput_deviation,
                ):
                    invalidations.append(
                        f"{record['label']}: baseline A/A throughput control exceeds "
                        f"{args.control_max_throughput_deviation:.2%} bias budget"
                    )
                if not control_is_valid(
                    candidate_control,
                    max_throughput_deviation=args.control_max_throughput_deviation,
                ):
                    invalidations.append(
                        f"{record['label']}: candidate A/A throughput control exceeds "
                        f"{args.control_max_throughput_deviation:.2%} bias budget"
                    )
                confirmed, noise_floor = confirmed_loop_regression(
                    ab_pairs,
                    base_control_pairs=base_control,
                    candidate_control_pairs=candidate_control,
                    max_loop_lag_ratio=args.max_loop_lag_ratio,
                )
                confirmation["same_code_noise_floor_ms"] = noise_floor
                confirmation["loop_confirmed"] = confirmed
                if confirmed:
                    failures.append(
                        f"{record['label']}: loop-lag ratio "
                        f"{final.loop_lag_ratio.geometric_mean:.4f} with additive lower 95% "
                        f"bound {final.loop_lag_delta.lower_95_ms:.6f}ms above same-code "
                        f"noise floor {noise_floor:.6f}ms"
                    )

            confirmation["status"] = (
                "invalid_control"
                if any(record["label"] in item for item in invalidations)
                else "completed"
            )
            record["confirmation"] = confirmation
            record["final_metrics"] = _metrics_dict(final)

        result["failures"] = failures
        result["invalidations"] = invalidations
        if invalidations:
            result["status"] = "invalid"
            exit_code = 2
        elif failures:
            result["status"] = "failed"
            exit_code = 1
        else:
            result["status"] = "passed"
            exit_code = 0
        _write_result(args.output, result)
        if args.policy == "strict":
            return exit_code
        return 0
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        result["status"] = "invalid"
        result["invalidations"] = [str(exc)]
        _write_result(args.output, result)
        print(f"open-loop release gate invalid: {exc}", file=sys.stderr)
        return 2 if args.policy == "strict" else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-root", required=True, type=Path)
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument("--engine", type=Path, default=Path("benchmarks/paired_open_loop.py"))
    parser.add_argument("--runner-probe", type=Path, default=Path("benchmarks/runner_probe.py"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--protocols", default="311,5")
    parser.add_argument("--payloads", default="64,4096")
    parser.add_argument("--completions", default="receipt,callback")
    parser.add_argument("--windows", default="100")
    parser.add_argument("--fractions")
    parser.add_argument("--target-rates")
    parser.add_argument("--count-small", type=int, default=2_000)
    parser.add_argument("--count-large", type=int, default=1_000)
    parser.add_argument("--target-sample-seconds", type=float, default=1.5)
    parser.add_argument("--calibration-seconds", type=float, default=0.25)
    parser.add_argument("--calibration-repeats", type=int, default=3)
    parser.add_argument(
        "--calibration-seeds",
        type=lambda value: _parse_seed_schedule(value, minimum=1),
        default=_parse_seed_schedule("0,1,2"),
    )
    parser.add_argument(
        "--initial-cycle-seeds",
        type=lambda value: _parse_seed_schedule(value, minimum=2),
        default=_parse_seed_schedule("10,11", minimum=2),
    )
    parser.add_argument(
        "--confirmation-cycle-seeds",
        type=lambda value: _parse_seed_schedule(value, minimum=2),
        default=_parse_seed_schedule("12,13", minimum=2),
    )
    parser.add_argument(
        "--control-cycle-seeds",
        type=lambda value: _parse_seed_schedule(value, minimum=2),
        default=_parse_seed_schedule("20,21", minimum=2),
    )
    parser.add_argument("--max-count", type=int, default=50_000)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--min-completed-ratio", type=float, default=0.97)
    parser.add_argument("--max-loop-lag-ratio", type=float, default=1.05)
    parser.add_argument("--control-max-throughput-deviation", type=float, default=0.02)
    parser.add_argument("--max-confirmation-scenarios", type=int, default=4)
    parser.add_argument("--preflight-wait-seconds", type=float, default=60.0)
    parser.add_argument("--preflight-poll-seconds", type=float, default=5.0)
    parser.add_argument("--preflight-consecutive-eligible", type=int, default=2)
    parser.add_argument("--require-temperature", action="store_true")
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--output", type=Path, default=Path("/tmp/open-loop-release-gate.json"))
    args = parser.parse_args()

    if args.count_small <= 0 or args.count_large <= 0 or args.max_count <= 0:
        parser.error("sample counts must be positive")
    if args.target_sample_seconds <= 0 or args.calibration_seconds <= 0:
        parser.error("sample and calibration durations must be positive")
    if args.calibration_repeats < 3:
        parser.error("--calibration-repeats must be at least 3")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if not 0 < args.min_completed_ratio <= 1:
        parser.error("--min-completed-ratio must be in (0, 1]")
    if args.max_loop_lag_ratio <= 1:
        parser.error("--max-loop-lag-ratio must be greater than 1")
    if not 0 <= args.control_max_throughput_deviation < 1:
        parser.error("--control-max-throughput-deviation must be in [0, 1)")
    if args.max_confirmation_scenarios < 0:
        parser.error("--max-confirmation-scenarios must be non-negative")
    if args.preflight_wait_seconds < 0 or args.preflight_poll_seconds < 0:
        parser.error("preflight wait/poll durations must be non-negative")
    if args.preflight_consecutive_eligible < 1:
        parser.error("--preflight-consecutive-eligible must be at least 1")
    try:
        _scenario_specs(args)
        _load_points(args.fractions, args.target_rates)
    except ValueError as exc:
        parser.error(str(exc))
    return args


if __name__ == "__main__":
    raise SystemExit(parent(parse_args()))
