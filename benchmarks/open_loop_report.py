"""Explain paired open-loop loop-lag failures using absolute measurements."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def _number(mapping: dict[str, Any], key: str) -> float:
    value = mapping[key]
    if not isinstance(value, (int, float)):
        raise TypeError(f"field {key!r} is not numeric")
    return float(value)


def _load_label(scenario: dict[str, Any]) -> str:
    if scenario.get("load_mode") == "absolute_rate":
        return f"rate={_number(scenario, 'requested_target_rate'):g}msg/s"
    return f"load={_number(scenario, 'load_fraction'):.2f}"


def _diagnose_scenario(scenario: dict[str, Any], threshold: float) -> dict[str, Any]:
    pairs = scenario.get("pairs")
    if not isinstance(pairs, list) or not pairs:
        raise ValueError("scenario has no measurement pairs")

    base_lag: list[float] = []
    candidate_lag: list[float] = []
    pair_deltas: list[float] = []
    pair_ratios: list[float] = []
    for pair in pairs:
        if not isinstance(pair, dict):
            raise TypeError("measurement pair is not an object")
        base = pair.get("base")
        candidate = pair.get("candidate")
        if not isinstance(base, dict) or not isinstance(candidate, dict):
            raise TypeError("measurement pair is missing base/candidate objects")
        base_ms = _number(base, "loop_lag_p95_ms")
        candidate_ms = _number(candidate, "loop_lag_p95_ms")
        base_lag.append(base_ms)
        candidate_lag.append(candidate_ms)
        pair_deltas.append(candidate_ms - base_ms)
        pair_ratios.append(candidate_ms / max(base_ms, 1e-9))

    ratio = _number(scenario, "median_candidate_over_base_loop_lag_p95")
    base_median = statistics.median(base_lag)
    candidate_median = statistics.median(candidate_lag)
    return {
        "protocol": scenario.get("protocol"),
        "payload_bytes": scenario.get("payload_bytes"),
        "window": scenario.get("window"),
        "completion": scenario.get("completion"),
        "load": _load_label(scenario),
        "loop_lag_ratio": ratio,
        "relative_failure": ratio > threshold,
        "base_loop_lag_p95_ms_median": base_median,
        "candidate_loop_lag_p95_ms_median": candidate_median,
        "median_absolute_delta_ms": candidate_median - base_median,
        "median_pair_delta_ms": statistics.median(pair_deltas),
        "max_abs_pair_delta_ms": max(abs(value) for value in pair_deltas),
        "pair_ratios": pair_ratios,
        "base_loop_lag_p95_ms": base_lag,
        "candidate_loop_lag_p95_ms": candidate_lag,
    }


def diagnose(report: dict[str, Any]) -> dict[str, Any]:
    thresholds = report.get("thresholds")
    scenarios = report.get("scenarios")
    if not isinstance(thresholds, dict) or not isinstance(scenarios, list):
        raise ValueError("report is missing thresholds or scenarios")
    threshold = _number(thresholds, "max_loop_lag_ratio")
    diagnostics = [
        _diagnose_scenario(scenario, threshold)
        for scenario in scenarios
        if isinstance(scenario, dict)
    ]
    return {
        "source_status": report.get("status"),
        "max_loop_lag_ratio": threshold,
        "scenario_count": len(diagnostics),
        "relative_loop_lag_failures": sum(bool(item["relative_failure"]) for item in diagnostics),
        "invalidations": report.get("invalidations", []),
        "regressions": report.get("regressions", []),
        "scenarios": diagnostics,
    }


def _print_summary(diagnostic: dict[str, Any], *, show_all: bool) -> None:
    threshold = float(diagnostic["max_loop_lag_ratio"])
    print(
        f"source_status={diagnostic['source_status']} scenarios={diagnostic['scenario_count']} "
        f"loop_lag_threshold={threshold:.4f} "
        f"relative_failures={diagnostic['relative_loop_lag_failures']}"
    )
    scenarios = diagnostic["scenarios"]
    assert isinstance(scenarios, list)
    for item in scenarios:
        assert isinstance(item, dict)
        if not show_all and item["relative_failure"] is not True:
            continue
        print(
            "protocol={protocol} payload={payload_bytes} window={window} "
            "completion={completion} {load} ratio={loop_lag_ratio:.4f} "
            "lag_ms={base_loop_lag_p95_ms_median:.6f}->"
            "{candidate_loop_lag_p95_ms_median:.6f} "
            "delta_ms={median_absolute_delta_ms:+.6f} "
            "max_pair_delta_ms={max_abs_pair_delta_ms:.6f}".format(**item)
        )
    for key in ("invalidations", "regressions"):
        values = diagnostic.get(key)
        if isinstance(values, list) and values:
            print(f"{key}:")
            for value in values:
                print(f"- {value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="paired-open-loop JSON report")
    parser.add_argument("--all", action="store_true", help="print every scenario")
    parser.add_argument("--output", type=Path, help="write structured diagnostics as JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = json.loads(args.report.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise TypeError("report root must be an object")
    diagnostic = diagnose(report)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(diagnostic, indent=2) + "\n", encoding="utf-8")
    _print_summary(diagnostic, show_all=args.all)


if __name__ == "__main__":
    main()
