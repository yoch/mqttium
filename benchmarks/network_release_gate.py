"""Release-grade statistical gate for paired MQTT network measurements.

``paired_network.py`` remains the low-level acquisition engine. This module
adds the release decision layer: deterministic same-code controls, hash-seed
blocked ABBA cycles, and confidence-bound A/B evaluation.

Raw per-arm variability is retained as diagnostic evidence but is not, by
itself, a reason to discard an otherwise precise paired estimator.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


_T_975 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    21: 2.080,
    22: 2.074,
    23: 2.069,
    24: 2.064,
    25: 2.060,
    26: 2.056,
    27: 2.052,
    28: 2.048,
    29: 2.045,
    30: 2.042,
}


@dataclass(frozen=True)
class PairEstimate:
    pairs: int
    cycles: int
    geometric_mean: float
    median_cycle: float
    lower_95: float
    upper_95: float
    cycle_log_standard_deviation: float
    pair_ratio_cv: float


@dataclass(frozen=True)
class ScenarioEvaluation:
    key: str
    throughput: PairEstimate
    ack_p50: PairEstimate
    delivery_p50: PairEstimate
    base_ack_cv: float
    candidate_ack_cv: float
    failures: tuple[str, ...]


def coefficient_of_variation(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _t_critical_95(degrees_of_freedom: int) -> float:
    if degrees_of_freedom <= 0:
        raise ValueError("degrees_of_freedom must be positive")
    if degrees_of_freedom <= 30:
        return _T_975[degrees_of_freedom]
    if degrees_of_freedom <= 40:
        return 2.021
    if degrees_of_freedom <= 60:
        return 2.000
    if degrees_of_freedom <= 120:
        return 1.980
    return 1.960


def abba_cycle_ratios(ratios: list[float], orders: list[list[str]]) -> list[float]:
    """Collapse opposite-order adjacent pairs into complete ABBA cycle ratios."""
    if len(ratios) != len(orders) or len(ratios) < 4 or len(ratios) % 2:
        raise ValueError("ABBA evaluation requires an even number of at least four pairs")
    cycles: list[float] = []
    for index in range(0, len(ratios), 2):
        first_order = orders[index]
        second_order = orders[index + 1]
        if first_order != ["base", "candidate"] or second_order != ["candidate", "base"]:
            raise ValueError("paired measurements do not form complete ABBA cycles")
        first = ratios[index]
        second = ratios[index + 1]
        if any(not math.isfinite(value) or value <= 0 for value in (first, second)):
            raise ValueError("paired ratios must be finite and positive")
        cycles.append(math.sqrt(first * second))
    return cycles


def paired_ratio_estimate(ratios: list[float], orders: list[list[str]]) -> PairEstimate:
    """Estimate multiplicative effect from complete ABBA cycles on log scale."""
    cycles = abba_cycle_ratios(ratios, orders)
    logs = [math.log(value) for value in cycles]
    mean_log = statistics.fmean(logs)
    log_sd = statistics.stdev(logs) if len(logs) > 1 else 0.0
    half_width = (
        _t_critical_95(len(logs) - 1) * log_sd / math.sqrt(len(logs)) if len(logs) > 1 else 0.0
    )
    return PairEstimate(
        pairs=len(ratios),
        cycles=len(cycles),
        geometric_mean=math.exp(mean_log),
        median_cycle=statistics.median(cycles),
        lower_95=math.exp(mean_log - half_width),
        upper_95=math.exp(mean_log + half_width),
        cycle_log_standard_deviation=log_sd,
        pair_ratio_cv=coefficient_of_variation(ratios),
    )


def control_failures(
    label: str,
    estimate: PairEstimate,
    *,
    bias_floor: float,
    bias_ceiling: float,
    equivalence_floor: float,
    equivalence_ceiling: float,
) -> list[str]:
    """Require same-code bias and uncertainty to fit predeclared budgets."""
    failures: list[str] = []
    if not bias_floor <= estimate.geometric_mean <= bias_ceiling:
        failures.append(
            f"{label}: same-code estimate {estimate.geometric_mean:.4f} "
            f"outside bias budget [{bias_floor:.4f}, {bias_ceiling:.4f}]"
        )
    if estimate.lower_95 < equivalence_floor or estimate.upper_95 > equivalence_ceiling:
        failures.append(
            f"{label}: same-code 95% CI [{estimate.lower_95:.4f}, "
            f"{estimate.upper_95:.4f}] exceeds equivalence "
            f"[{equivalence_floor:.4f}, {equivalence_ceiling:.4f}]"
        )
    return failures


def regression_failures(
    label: str,
    throughput: PairEstimate,
    ack_p50: PairEstimate,
    *,
    min_throughput: float,
    max_ack_p50: float,
) -> list[str]:
    """Apply conservative no-regression bounds to paired A/B confidence intervals."""
    failures: list[str] = []
    if throughput.lower_95 < min_throughput:
        failures.append(
            f"{label}: throughput lower 95% bound {throughput.lower_95:.4f} < {min_throughput:.4f}"
        )
    if ack_p50.upper_95 > max_ack_p50:
        failures.append(
            f"{label}: ACK p50 upper 95% bound {ack_p50.upper_95:.4f} > {max_ack_p50:.4f}"
        )
    return failures


def _scenario_key(scenario: dict[str, Any]) -> str:
    return (
        f"protocol={scenario['protocol']} completion={scenario['completion']} "
        f"payload={scenario['payload_bytes']} window={scenario['window']}"
    )


def _pair_estimates(scenario: dict[str, Any]) -> tuple[PairEstimate, PairEstimate, PairEstimate]:
    throughput: list[float] = []
    ack_p50: list[float] = []
    delivery_p50: list[float] = []
    orders: list[list[str]] = []
    for pair in scenario["pairs"]:
        base = pair["base"]
        candidate = pair["candidate"]
        orders.append(list(pair["order"]))
        throughput.append(candidate["publisher_ack_msg_s"] / base["publisher_ack_msg_s"])
        ack_p50.append(candidate["ack_latency_p50_ms"] / base["ack_latency_p50_ms"])
        delivery_p50.append(candidate["latency_p50_ms"] / base["latency_p50_ms"])
    return (
        paired_ratio_estimate(throughput, orders),
        paired_ratio_estimate(ack_p50, orders),
        paired_ratio_estimate(delivery_p50, orders),
    )


def _raw_ack_cvs(scenario: dict[str, Any]) -> tuple[float, float]:
    base_values = [float(pair["base"]["publisher_ack_msg_s"]) for pair in scenario["pairs"]]
    candidate_values = [
        float(pair["candidate"]["publisher_ack_msg_s"]) for pair in scenario["pairs"]
    ]
    return coefficient_of_variation(base_values), coefficient_of_variation(candidate_values)


def evaluate_control_payload(
    payload: dict[str, Any],
    *,
    throughput_bias_floor: float,
    throughput_bias_ceiling: float,
    throughput_equivalence_floor: float,
    throughput_equivalence_ceiling: float,
    ack_p50_bias_floor: float,
    ack_p50_bias_ceiling: float,
    ack_p50_equivalence_floor: float,
    ack_p50_equivalence_ceiling: float,
) -> list[ScenarioEvaluation]:
    evaluations: list[ScenarioEvaluation] = []
    for scenario in payload["scenarios"]:
        throughput, ack_p50, delivery_p50 = _pair_estimates(scenario)
        base_ack_cv, candidate_ack_cv = _raw_ack_cvs(scenario)
        key = _scenario_key(scenario)
        failures = control_failures(
            f"{key} throughput",
            throughput,
            bias_floor=throughput_bias_floor,
            bias_ceiling=throughput_bias_ceiling,
            equivalence_floor=throughput_equivalence_floor,
            equivalence_ceiling=throughput_equivalence_ceiling,
        )
        failures.extend(
            control_failures(
                f"{key} ACK p50",
                ack_p50,
                bias_floor=ack_p50_bias_floor,
                bias_ceiling=ack_p50_bias_ceiling,
                equivalence_floor=ack_p50_equivalence_floor,
                equivalence_ceiling=ack_p50_equivalence_ceiling,
            )
        )
        evaluations.append(
            ScenarioEvaluation(
                key=key,
                throughput=throughput,
                ack_p50=ack_p50,
                delivery_p50=delivery_p50,
                base_ack_cv=base_ack_cv,
                candidate_ack_cv=candidate_ack_cv,
                failures=tuple(failures),
            )
        )
    return evaluations


def evaluate_ab_payload(
    payload: dict[str, Any],
    *,
    min_throughput: float,
    max_ack_p50: float,
) -> list[ScenarioEvaluation]:
    evaluations: list[ScenarioEvaluation] = []
    for scenario in payload["scenarios"]:
        throughput, ack_p50, delivery_p50 = _pair_estimates(scenario)
        base_ack_cv, candidate_ack_cv = _raw_ack_cvs(scenario)
        key = _scenario_key(scenario)
        failures = regression_failures(
            key,
            throughput,
            ack_p50,
            min_throughput=min_throughput,
            max_ack_p50=max_ack_p50,
        )
        evaluations.append(
            ScenarioEvaluation(
                key=key,
                throughput=throughput,
                ack_p50=ack_p50,
                delivery_p50=delivery_p50,
                base_ack_cv=base_ack_cv,
                candidate_ack_cv=candidate_ack_cv,
                failures=tuple(failures),
            )
        )
    return evaluations


def _git_sha(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _engine_command(
    args: argparse.Namespace,
    *,
    base_root: Path,
    candidate_root: Path,
    output: Path,
    preflight_report: Path,
) -> list[str]:
    # One acquisition invocation is exactly one complete ABBA cycle: two
    # opposite-order pairs under one deterministic Python hash seed.
    command = [
        sys.executable,
        str(args.engine),
        "--base-root",
        str(base_root),
        "--candidate-root",
        str(candidate_root),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocols",
        args.protocols,
        "--payloads",
        args.payloads,
        "--windows",
        args.windows,
        "--completions",
        args.completions,
        "--repeat",
        "2",
        "--count-small",
        str(args.count_small),
        "--count-large",
        str(args.count_large),
        "--target-sample-seconds",
        str(args.target_sample_seconds),
        "--max-count",
        str(args.max_count),
        "--timeout",
        str(args.timeout),
        "--policy",
        "advisory",
        "--max-baseline-cv",
        "10.0",
        "--min-ack-ratio",
        "0.0",
        "--output",
        str(output),
        "--summary-output",
        str(output.with_suffix(".md")),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    command.extend(("--preflight-report", str(preflight_report)))
    return command


def _fresh_preflight(
    args: argparse.Namespace,
    *,
    label: str,
    raw_dir: Path,
    quiet_seconds: float,
    ignore_historical_load: bool,
) -> Path:
    if quiet_seconds > 0:
        print(f"{label}: fixed inter-block quiet period {quiet_seconds:.0f}s")
        time.sleep(quiet_seconds)
    output = raw_dir / f"{label}-preflight.json"
    command = [
        sys.executable,
        str(args.runner_probe),
        "--output",
        str(output),
        "--require-temperature",
        "--enforce",
    ]
    if ignore_historical_load:
        command.append("--ignore-historical-load")
    completed = subprocess.run(command, check=False)
    if completed.returncode:
        raise RuntimeError(f"{label}: fresh runner preflight failed")
    return output


def _run_engine_cycle(
    args: argparse.Namespace,
    *,
    label: str,
    base_root: Path,
    candidate_root: Path,
    raw_dir: Path,
    preflight_report: Path,
    hash_seed: int,
) -> dict[str, Any]:
    output = raw_dir / f"{label}.json"
    command = _engine_command(
        args,
        base_root=base_root,
        candidate_root=candidate_root,
        output=output,
        preflight_report=preflight_report,
    )
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = str(hash_seed)
    completed = subprocess.run(command, check=False, env=env)
    if completed.returncode:
        raise RuntimeError(f"measurement engine exited {completed.returncode}: {' '.join(command)}")
    try:
        payload = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"measurement engine did not write valid {output}") from exc
    eligibility = payload.get("eligibility", {})
    if not isinstance(eligibility, dict) or eligibility.get("eligible") is not True:
        raise RuntimeError(f"{label}: runner preflight is not eligible")
    if payload.get("status") == "invalid":
        raise RuntimeError(f"{label}: acquisition invalid: {payload.get('failures', [])}")
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise RuntimeError(f"{label}: acquisition returned no scenarios")
    for scenario in scenarios:
        if not isinstance(scenario, dict) or len(scenario.get("pairs", [])) != 2:
            raise RuntimeError(f"{label}: each seeded acquisition must contain one ABBA cycle")
    return payload


def _combine_phase_payloads(
    cycle_payloads: list[tuple[int, int, dict[str, Any]]],
) -> dict[str, Any]:
    if not cycle_payloads:
        raise RuntimeError("phase contains no cycle payloads")

    first_scenarios = cycle_payloads[0][2].get("scenarios")
    if not isinstance(first_scenarios, list) or not first_scenarios:
        raise RuntimeError("phase first cycle contains no scenarios")

    scenario_order = [_scenario_key(scenario) for scenario in first_scenarios]
    combined: dict[str, dict[str, Any]] = {}
    for scenario in first_scenarios:
        key = _scenario_key(scenario)
        combined[key] = {
            "protocol": scenario["protocol"],
            "completion": scenario["completion"],
            "payload_bytes": scenario["payload_bytes"],
            "window": scenario["window"],
            "pairs": [],
        }

    for block, hash_seed, payload in cycle_payloads:
        scenarios = payload.get("scenarios")
        if not isinstance(scenarios, list):
            raise RuntimeError("cycle payload has invalid scenarios")
        if [_scenario_key(scenario) for scenario in scenarios] != scenario_order:
            raise RuntimeError("cycle payload scenario set/order changed within phase")
        for scenario in scenarios:
            key = _scenario_key(scenario)
            pairs = scenario.get("pairs")
            if not isinstance(pairs, list) or len(pairs) != 2:
                raise RuntimeError(f"{key}: seeded cycle does not contain exactly two pairs")
            for pair in pairs:
                annotated = dict(pair)
                annotated["block"] = block
                annotated["hash_seed"] = hash_seed
                combined[key]["pairs"].append(annotated)

    # Validate ABBA completeness/order before any statistical interpretation.
    for scenario in combined.values():
        orders = [list(pair["order"]) for pair in scenario["pairs"]]
        abba_cycle_ratios([1.0] * len(orders), orders)

    return {
        "status": "passed",
        "scenarios": [combined[key] for key in scenario_order],
    }


def _run_phase(
    args: argparse.Namespace,
    *,
    label: str,
    base_root: Path,
    candidate_root: Path,
    raw_dir: Path,
    initial_quiet_seconds: float,
    blocks: int,
    cycle_seeds: list[int],
    check_historical_load: bool,
) -> dict[str, Any]:
    if blocks < 1:
        raise ValueError("phase must acquire at least one block")
    if not cycle_seeds:
        raise ValueError("phase must acquire at least one cycle seed")
    cycle_payloads: list[tuple[int, int, dict[str, Any]]] = []
    for block in range(blocks):
        quiet_seconds = initial_quiet_seconds if block == 0 else args.inter_phase_quiet_seconds
        block_label = f"{label}-block-{block + 1}"
        preflight_report = _fresh_preflight(
            args,
            label=block_label,
            raw_dir=raw_dir,
            quiet_seconds=quiet_seconds,
            ignore_historical_load=not (check_historical_load and block == 0),
        )
        for hash_seed in cycle_seeds:
            cycle_label = f"{block_label}-seed-{hash_seed}"
            print(f"{cycle_label}: acquiring one complete ABBA cycle")
            payload = _run_engine_cycle(
                args,
                label=cycle_label,
                base_root=base_root,
                candidate_root=candidate_root,
                raw_dir=raw_dir,
                preflight_report=preflight_report,
                hash_seed=hash_seed,
            )
            cycle_payloads.append((block + 1, hash_seed, payload))
    return _combine_phase_payloads(cycle_payloads)


def _evaluation_dict(evaluation: ScenarioEvaluation) -> dict[str, Any]:
    return asdict(evaluation)


def _write_result(output: Path, payload: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    sampling = payload["sampling"]
    lines = [
        "# Network release gate",
        "",
        f"- Status: **{payload['status']}**",
        f"- Base: `{payload['base_sha']}`",
        f"- Candidate: `{payload['candidate_sha']}`",
        f"- Control: `{sampling['control_blocks']}` block(s), seeds "
        f"`{','.join(map(str, sampling['control_cycle_seeds']))}` "
        f"({sampling['control_cycles_per_scenario']} ABBA cycles)",
        f"- A/B: `{sampling['ab_blocks']}` block(s), seeds "
        f"`{','.join(map(str, sampling['cycle_seeds']))}` "
        f"({sampling['cycles_per_scenario']} ABBA cycles / "
        f"{sampling['pairs_per_scenario']} paired samples)",
        "",
    ]
    failures = payload.get("failures", [])
    if failures:
        lines.extend(("## Failures", ""))
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
    for section in ("base_control", "candidate_control", "ab"):
        values = payload.get(section)
        if not values:
            continue
        lines.extend((f"## {section.replace('_', ' ').title()}", ""))
        for item in values:
            throughput = item["throughput"]
            ack = item["ack_p50"]
            lines.append(
                f"- `{item['key']}`: throughput {throughput['geometric_mean']:.4f} "
                f"CI [{throughput['lower_95']:.4f}, {throughput['upper_95']:.4f}]; "
                f"ACK p50 {ack['geometric_mean']:.4f} "
                f"CI [{ack['lower_95']:.4f}, {ack['upper_95']:.4f}]"
            )
        lines.append("")
    output.with_suffix(".md").write_text("\n".join(lines), encoding="utf-8")


def _control_evaluations(
    args: argparse.Namespace, payload: dict[str, Any]
) -> list[ScenarioEvaluation]:
    return evaluate_control_payload(
        payload,
        throughput_bias_floor=args.control_throughput_bias_floor,
        throughput_bias_ceiling=args.control_throughput_bias_ceiling,
        throughput_equivalence_floor=args.control_throughput_floor,
        throughput_equivalence_ceiling=args.control_throughput_ceiling,
        ack_p50_bias_floor=args.control_ack_p50_bias_floor,
        ack_p50_bias_ceiling=args.control_ack_p50_bias_ceiling,
        ack_p50_equivalence_floor=args.control_ack_p50_floor,
        ack_p50_equivalence_ceiling=args.control_ack_p50_ceiling,
    )


def parent(args: argparse.Namespace) -> int:
    base_root = args.base_root.resolve()
    candidate_root = args.candidate_root.resolve()
    raw_dir = args.output.parent / f"{args.output.stem}-raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    control_cycles = args.control_blocks * len(args.control_cycle_seeds)
    ab_cycles = args.ab_blocks * len(args.cycle_seeds)
    result: dict[str, Any] = {
        "status": "running",
        "policy": args.policy,
        "base_sha": _git_sha(base_root),
        "candidate_sha": _git_sha(candidate_root),
        "sampling": {
            "control_blocks": args.control_blocks,
            "control_cycle_seeds": args.control_cycle_seeds,
            "control_cycles_per_scenario": control_cycles,
            "ab_blocks": args.ab_blocks,
            "cycle_seeds": args.cycle_seeds,
            "cycles_per_scenario": ab_cycles,
            "pairs_per_scenario": 2 * ab_cycles,
            "engine_pairs_per_seed": 2,
        },
        "thresholds": {
            "control_throughput_bias": [
                args.control_throughput_bias_floor,
                args.control_throughput_bias_ceiling,
            ],
            "control_throughput_equivalence": [
                args.control_throughput_floor,
                args.control_throughput_ceiling,
            ],
            "control_ack_p50_bias": [
                args.control_ack_p50_bias_floor,
                args.control_ack_p50_bias_ceiling,
            ],
            "control_ack_p50_equivalence": [
                args.control_ack_p50_floor,
                args.control_ack_p50_ceiling,
            ],
            "min_throughput": args.min_throughput,
            "max_ack_p50": args.max_ack_p50,
            "confidence": 0.95,
            "inter_block_quiet_seconds": args.inter_phase_quiet_seconds,
        },
        "raw_arm_cv": "diagnostic_only",
        "base_control": [],
        "candidate_control": [],
        "ab": [],
        "failures": [],
    }
    try:
        base_control_payload = _run_phase(
            args,
            label="base-aa",
            base_root=base_root,
            candidate_root=base_root,
            raw_dir=raw_dir,
            initial_quiet_seconds=0.0,
            blocks=args.control_blocks,
            cycle_seeds=args.control_cycle_seeds,
            check_historical_load=True,
        )
        base_control = _control_evaluations(args, base_control_payload)
        result["base_control"] = [_evaluation_dict(item) for item in base_control]
        failures = [failure for item in base_control for failure in item.failures]
        if failures:
            result["status"] = "invalid"
            result["failures"] = failures
            _write_result(args.output, result)
            return 2 if args.policy == "strict" else 0

        candidate_control_payload = _run_phase(
            args,
            label="candidate-aa",
            base_root=candidate_root,
            candidate_root=candidate_root,
            raw_dir=raw_dir,
            initial_quiet_seconds=args.inter_phase_quiet_seconds,
            blocks=args.control_blocks,
            cycle_seeds=args.control_cycle_seeds,
            check_historical_load=False,
        )
        candidate_control = _control_evaluations(args, candidate_control_payload)
        result["candidate_control"] = [_evaluation_dict(item) for item in candidate_control]
        failures = [failure for item in candidate_control for failure in item.failures]
        if failures:
            result["status"] = "invalid"
            result["failures"] = failures
            _write_result(args.output, result)
            return 2 if args.policy == "strict" else 0

        ab_payload = _run_phase(
            args,
            label="ab",
            base_root=base_root,
            candidate_root=candidate_root,
            raw_dir=raw_dir,
            initial_quiet_seconds=args.inter_phase_quiet_seconds,
            blocks=args.ab_blocks,
            cycle_seeds=args.cycle_seeds,
            check_historical_load=False,
        )
        ab = evaluate_ab_payload(
            ab_payload,
            min_throughput=args.min_throughput,
            max_ack_p50=args.max_ack_p50,
        )
        result["ab"] = [_evaluation_dict(item) for item in ab]
        failures = [failure for item in ab for failure in item.failures]
        result["failures"] = failures
        result["status"] = "failed" if failures else "passed"
        _write_result(args.output, result)
        if failures and args.policy == "strict":
            return 1
        return 0
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        result["status"] = "invalid"
        result["failures"] = [str(exc)]
        _write_result(args.output, result)
        print(f"network release gate invalid: {exc}", file=sys.stderr)
        return 2 if args.policy == "strict" else 0


def _parse_seed_schedule(value: str, *, minimum: int, kind: str) -> list[int]:
    raw = [item.strip() for item in value.split(",") if item.strip()]
    if not raw:
        raise argparse.ArgumentTypeError(f"{kind} seed schedule cannot be empty")
    seeds: list[int] = []
    for item in raw:
        try:
            seed = int(item)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid deterministic hash seed: {item!r}") from exc
        if not 0 <= seed <= 4_294_967_295:
            raise argparse.ArgumentTypeError(
                "hash seeds must fit PYTHONHASHSEED range 0..4294967295"
            )
        seeds.append(seed)
    if len(seeds) < minimum:
        raise argparse.ArgumentTypeError(
            f"{kind} requires at least {minimum} cycle seed(s) per block"
        )
    if len(set(seeds)) != len(seeds):
        raise argparse.ArgumentTypeError("cycle seed schedule must not contain duplicates")
    return seeds


def _parse_cycle_seeds(value: str) -> list[int]:
    return _parse_seed_schedule(value, minimum=6, kind="A/B phase")


def _parse_control_cycle_seeds(value: str) -> list[int]:
    return _parse_seed_schedule(value, minimum=2, kind="same-code control")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-root", required=True, type=Path)
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument("--engine", type=Path, default=Path("benchmarks/paired_network.py"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--protocols", default="311")
    parser.add_argument("--completions", default="callback")
    parser.add_argument("--payloads", default="64")
    parser.add_argument("--windows", default="1,20,64")
    parser.add_argument("--control-blocks", type=int, default=1)
    parser.add_argument(
        "--control-cycle-seeds",
        type=_parse_control_cycle_seeds,
        default=_parse_control_cycle_seeds("0,1,2"),
    )
    parser.add_argument("--ab-blocks", type=int, default=2)
    parser.add_argument(
        "--cycle-seeds",
        type=_parse_cycle_seeds,
        default=_parse_cycle_seeds("0,1,2,3,4,5"),
    )
    parser.add_argument("--count-small", type=int, default=6_000)
    parser.add_argument("--count-large", type=int, default=3_000)
    parser.add_argument("--target-sample-seconds", type=float, default=2.0)
    parser.add_argument("--max-count", type=int, default=60_000)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--runner-probe", type=Path, default=Path("benchmarks/runner_probe.py"))
    parser.add_argument("--inter-phase-quiet-seconds", type=float, default=30.0)
    parser.add_argument("--control-throughput-bias-floor", type=float, default=0.98)
    parser.add_argument("--control-throughput-bias-ceiling", type=float, default=1.02)
    parser.add_argument("--control-throughput-floor", type=float, default=0.95)
    parser.add_argument("--control-throughput-ceiling", type=float, default=1.05)
    parser.add_argument("--control-ack-p50-bias-floor", type=float, default=0.95)
    parser.add_argument("--control-ack-p50-bias-ceiling", type=float, default=1.05)
    parser.add_argument("--control-ack-p50-floor", type=float, default=0.90)
    parser.add_argument("--control-ack-p50-ceiling", type=float, default=1.10)
    parser.add_argument("--min-throughput", type=float, default=0.95)
    parser.add_argument("--max-ack-p50", type=float, default=1.10)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--output", type=Path, default=Path("/tmp/network-release-gate.json"))
    args = parser.parse_args()
    if args.target_sample_seconds <= 0:
        parser.error("--target-sample-seconds must be positive")
    if args.control_blocks < 1:
        parser.error("--control-blocks must be at least 1")
    if args.ab_blocks < 1:
        parser.error("--ab-blocks must be at least 1")
    if args.inter_phase_quiet_seconds < 0:
        parser.error("--inter-phase-quiet-seconds must be non-negative")
    if not 0 < args.control_throughput_floor < 1 < args.control_throughput_ceiling:
        parser.error("throughput control equivalence bounds must straddle 1.0")
    if not 0 < args.control_throughput_bias_floor < 1 < args.control_throughput_bias_ceiling:
        parser.error("throughput control bias bounds must straddle 1.0")
    if not (
        args.control_throughput_floor
        <= args.control_throughput_bias_floor
        < args.control_throughput_bias_ceiling
        <= args.control_throughput_ceiling
    ):
        parser.error("throughput bias budget must fit inside equivalence bounds")
    if not 0 < args.control_ack_p50_floor < 1 < args.control_ack_p50_ceiling:
        parser.error("ACK p50 control equivalence bounds must straddle 1.0")
    if not 0 < args.control_ack_p50_bias_floor < 1 < args.control_ack_p50_bias_ceiling:
        parser.error("ACK p50 control bias bounds must straddle 1.0")
    if not (
        args.control_ack_p50_floor
        <= args.control_ack_p50_bias_floor
        < args.control_ack_p50_bias_ceiling
        <= args.control_ack_p50_ceiling
    ):
        parser.error("ACK p50 bias budget must fit inside equivalence bounds")
    return args


if __name__ == "__main__":
    raise SystemExit(parent(parse_args()))
