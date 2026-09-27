#!/usr/bin/env python3
"""Tell backlog from loop congestion in retained open-loop evidence (#493).

`paired_open_loop.py` records `loop_lag_p95_ms` as how late each publication
starts against its own schedule, `start + k / target_rate`. While the publisher
keeps up, that is timer granularity or loop congestion. Once it cannot keep up,
every late publication delays all later ones: publication `k` is late by about
`k * (1 / offered_rate - 1 / target_rate)`, so the 95th percentile approaches

    0.95 * count * (1 / offered_rate - 1 / target_rate)

That backlog grows with the sample length and with any capacity gap between the
arms, however small. A lag ratio taken in that regime measures throughput, not
loop latency.

This tool re-reads retained gate or engine JSON, classifies every sample as
`paced` or `backlog`, compares backlog samples with the prediction above, and
reports the per-cell lag ratio next to the per-message CPU ratio and the
sample's regime. It also summarises runner preflight samples (CPU frequency,
firmware throttling) and calibration capacities when present.

Diagnostic only: it changes no gate verdict.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

# A sample whose offered rate falls this far below its target cannot have kept
# up with its own schedule for the whole run.
DEFAULT_BACKLOG_PACE = 0.99
# Share of the backlog prediction a measured value may deviate by and still be
# called explained by backlog.
DEFAULT_FIT_TOLERANCE = 0.35
# A publisher busy for this share of its sample never slept: its CPU time is
# its wall time, so CPU per message no longer measures per-message cost.
BUSY_SHARE = 0.95
SAMPLE_FIELDS = ("loop_lag_p95_ms", "offered_rate", "target_rate", "count")


@dataclass(frozen=True)
class SampleView:
    regime: str
    pace: float
    loop_lag_p95_ms: float
    predicted_backlog_ms: float
    cpu_us_per_msg: float | None
    busy_share: float | None
    completed_rate: float | None


@dataclass(frozen=True)
class CellReport:
    source: str
    label: str
    phase: str
    pairs: int
    target_rate: float
    base_regimes: dict[str, int]
    candidate_regimes: dict[str, int]
    base_lag_p95_ms: float
    candidate_lag_p95_ms: float
    lag_ratio: float
    base_pace: float
    candidate_pace: float
    cpu_ratio: float | None
    busy_samples: int
    completed_ratio: float | None
    backlog_samples: int
    backlog_explained: int
    verdict: str


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def is_sample(value: Any) -> bool:
    return isinstance(value, dict) and all(
        _finite(value.get(key)) is not None for key in SAMPLE_FIELDS
    )


def predicted_backlog_ms(sample: dict[str, Any]) -> float:
    """95th-percentile schedule lag of a publisher that ran at its offered rate."""
    count = float(sample["count"])
    offered = float(sample["offered_rate"])
    target = float(sample["target_rate"])
    if offered <= 0 or target <= 0:
        return math.inf
    return max(0.0, 0.95 * count * (1.0 / offered - 1.0 / target)) * 1000.0


def view(sample: dict[str, Any], *, backlog_pace: float) -> SampleView:
    target = float(sample["target_rate"])
    pace = float(sample["offered_rate"]) / target if target > 0 else math.inf
    cpu = _finite(sample.get("cpu_seconds"))
    count = float(sample["count"])
    offered = float(sample["offered_rate"])
    elapsed = count / offered if offered > 0 else None
    return SampleView(
        regime="backlog" if pace < backlog_pace else "paced",
        pace=pace,
        loop_lag_p95_ms=float(sample["loop_lag_p95_ms"]),
        predicted_backlog_ms=predicted_backlog_ms(sample),
        cpu_us_per_msg=(cpu * 1_000_000 / count) if cpu is not None and count > 0 else None,
        busy_share=(cpu / elapsed) if cpu is not None and elapsed else None,
        completed_rate=_finite(sample.get("completed_rate")),
    )


def explained_by_backlog(sample: SampleView, *, tolerance: float) -> bool:
    if sample.regime != "backlog" or not math.isfinite(sample.predicted_backlog_ms):
        return False
    if sample.predicted_backlog_ms <= 0:
        return False
    error = abs(sample.loop_lag_p95_ms - sample.predicted_backlog_ms)
    return error <= tolerance * sample.predicted_backlog_ms


def _busy(sample: SampleView) -> bool:
    return sample.busy_share is not None and sample.busy_share >= BUSY_SHARE


def _ratio(values: list[float | None]) -> float | None:
    usable = [value for value in values if value is not None and math.isfinite(value)]
    return statistics.median(usable) if usable else None


def cell_report(
    pairs: list[dict[str, Any]],
    *,
    source: str,
    label: str,
    phase: str,
    backlog_pace: float,
    tolerance: float,
) -> CellReport:
    base = [view(pair["base"], backlog_pace=backlog_pace) for pair in pairs]
    candidate = [view(pair["candidate"], backlog_pace=backlog_pace) for pair in pairs]
    lag_ratios = [
        c.loop_lag_p95_ms / max(b.loop_lag_p95_ms, 1e-9)
        for b, c in zip(base, candidate, strict=True)
    ]
    cpu_ratios: list[float | None] = []
    completed_ratios: list[float | None] = []
    for b, c in zip(base, candidate, strict=True):
        # A busy arm's CPU time is its wall time: no per-message cost signal.
        cpu_ratios.append(
            c.cpu_us_per_msg / b.cpu_us_per_msg
            if b.cpu_us_per_msg and c.cpu_us_per_msg is not None and not (_busy(b) or _busy(c))
            else None
        )
        completed_ratios.append(
            c.completed_rate / b.completed_rate
            if b.completed_rate and c.completed_rate is not None
            else None
        )
    samples = base + candidate
    backlog = [sample for sample in samples if sample.regime == "backlog"]
    explained = [sample for sample in backlog if explained_by_backlog(sample, tolerance=tolerance)]
    regimes_b = {name: sum(s.regime == name for s in base) for name in ("paced", "backlog")}
    regimes_c = {name: sum(s.regime == name for s in candidate) for name in ("paced", "backlog")}
    # A lag ratio compares loop latency only when every sample kept pace. It
    # compares throughput when both arms fell behind; anything else mixes both.
    if not backlog:
        verdict = "paced"
    elif len(backlog) == len(samples) and len(explained) == len(backlog):
        verdict = "backlog"
    else:
        verdict = "mixed"
    target = statistics.median(float(pair["base"]["target_rate"]) for pair in pairs)
    return CellReport(
        source=source,
        label=label,
        phase=phase,
        pairs=len(pairs),
        target_rate=target,
        base_regimes=regimes_b,
        candidate_regimes=regimes_c,
        base_lag_p95_ms=statistics.median(s.loop_lag_p95_ms for s in base),
        candidate_lag_p95_ms=statistics.median(s.loop_lag_p95_ms for s in candidate),
        lag_ratio=statistics.median(lag_ratios),
        base_pace=statistics.median(s.pace for s in base),
        candidate_pace=statistics.median(s.pace for s in candidate),
        cpu_ratio=_ratio(cpu_ratios),
        busy_samples=sum(_busy(sample) for sample in samples),
        completed_ratio=_ratio(completed_ratios),
        backlog_samples=len(backlog),
        backlog_explained=len(explained),
        verdict=verdict,
    )


def _is_pair(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and is_sample(value.get("base"))
        and is_sample(value.get("candidate"))
    )


def collect_cells(
    payload: Any,
    *,
    source: str,
    backlog_pace: float = DEFAULT_BACKLOG_PACE,
    tolerance: float = DEFAULT_FIT_TOLERANCE,
) -> list[CellReport]:
    """Find every list of paired samples, whatever the gate version nested it in."""
    cells: list[CellReport] = []

    def walk(node: Any, label: str, phase: str) -> None:
        if isinstance(node, dict):
            label = str(node.get("label", label))
            for key, child in node.items():
                walk(child, label, key if isinstance(child, (list, dict)) else phase)
        elif isinstance(node, list):
            pairs = [item for item in node if _is_pair(item)]
            if pairs and len(pairs) == len(node):
                cells.append(
                    cell_report(
                        pairs,
                        source=source,
                        label=label,
                        phase=phase,
                        backlog_pace=backlog_pace,
                        tolerance=tolerance,
                    )
                )
                return
            for child in node:
                walk(child, label, phase)

    walk(payload, "", "")
    return cells


def collect_preflight(payload: Any) -> list[dict[str, Any]]:
    """Return every runner sample carrying frequency or throttling data."""
    found: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "cpu_frequencies" in node or "firmware_throttled" in node:
                found.append(
                    {
                        "cpu_frequencies": node.get("cpu_frequencies"),
                        "firmware_throttled": node.get("firmware_throttled"),
                        "max_temperature_c": node.get("max_temperature_c"),
                    }
                )
                return
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(payload)
    return found


def collect_calibrations(payload: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "baseline_capacity" in node and "label" in node:
                found.append(
                    {
                        "label": node["label"],
                        "baseline_capacity": node.get("baseline_capacity"),
                        "candidate_capacity_diagnostic": node.get("candidate_capacity_diagnostic"),
                        "target_rate": node.get("target_rate"),
                    }
                )
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(payload)
    return found


def _json_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.rglob("*.json")))
        elif path.suffix == ".json":
            files.append(path)
    return files


def analyse(paths: list[Path], *, backlog_pace: float, tolerance: float) -> dict[str, Any]:
    cells: list[CellReport] = []
    preflight: list[dict[str, Any]] = []
    calibrations: list[dict[str, Any]] = []
    for file in _json_files(paths):
        try:
            payload = json.loads(file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        source = str(file)
        cells.extend(
            collect_cells(payload, source=source, backlog_pace=backlog_pace, tolerance=tolerance)
        )
        preflight.extend({"source": source, **sample} for sample in collect_preflight(payload))
        calibrations.extend({"source": source, **item} for item in collect_calibrations(payload))
    return {
        "backlog_pace": backlog_pace,
        "fit_tolerance": tolerance,
        "cells": [asdict(cell) for cell in cells],
        "preflight": preflight,
        "calibrations": calibrations,
        "summary": summarise(cells),
    }


def summarise(cells: list[CellReport]) -> dict[str, Any]:
    raised = [cell for cell in cells if cell.lag_ratio > 1.05]
    by_verdict = {
        name: sum(cell.verdict == name for cell in raised) for name in ("paced", "backlog", "mixed")
    }
    paced_cpu = [cell.cpu_ratio for cell in cells if cell.verdict == "paced" and cell.cpu_ratio]
    return {
        "cells": len(cells),
        "lag_ratio_above_1_05": len(raised),
        "lag_ratio_above_1_05_by_regime": by_verdict,
        "backlog_samples": sum(cell.backlog_samples for cell in cells),
        "backlog_samples_explained": sum(cell.backlog_explained for cell in cells),
        "paced_cpu_ratio_median": statistics.median(paced_cpu) if paced_cpu else None,
        "busy_samples": sum(cell.busy_samples for cell in cells),
    }


def _fmt(value: float | None, digits: int = 3) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def markdown(result: dict[str, Any]) -> str:
    summary = result["summary"]
    lines = [
        "# Open-loop lag analysis (#493)",
        "",
        f"- Backlog when offered/target < {result['backlog_pace']}; "
        f"backlog explained when within ±{result['fit_tolerance']:.0%} of "
        "0.95·N·(1/offered − 1/target).",
        f"- Cells: {summary['cells']}; lag ratio > 1.05: {summary['lag_ratio_above_1_05']} "
        f"(by regime: {summary['lag_ratio_above_1_05_by_regime']}).",
        f"- Backlog samples: {summary['backlog_samples']}, explained by the backlog "
        f"prediction: {summary['backlog_samples_explained']}.",
        f"- Median CPU/message ratio in paced cells: {_fmt(summary['paced_cpu_ratio_median'])} "
        f"(samples busy ≥ {BUSY_SHARE:.0%} of their wall time excluded: {summary['busy_samples']}).",
        "",
        "| Source | Cell | Phase | Pairs | Target msg/s | Regime B / C (backlog) | "
        "Lag p95 B / C ms | Lag ratio | Pace B / C | CPU/msg ratio | Busy samples | Completed ratio | "
        "Backlog explained | Verdict |",
        "| --- | --- | --- | ---: | ---: | --- | --- | ---: | --- | ---: | ---: | ---: | --- | --- |",
    ]
    for cell in result["cells"]:
        lines.append(
            f"| {Path(cell['source']).parent.name}/{Path(cell['source']).name} | {cell['label']} "
            f"| {cell['phase']} | {cell['pairs']} | {cell['target_rate']:.0f} "
            f"| {cell['base_regimes']['backlog']} / {cell['candidate_regimes']['backlog']} "
            f"| {cell['base_lag_p95_ms']:.3f} / {cell['candidate_lag_p95_ms']:.3f} "
            f"| {cell['lag_ratio']:.3f} "
            f"| {cell['base_pace']:.3f} / {cell['candidate_pace']:.3f} "
            f"| {_fmt(cell['cpu_ratio'])} | {cell['busy_samples']}/{2 * cell['pairs']} "
            f"| {_fmt(cell['completed_ratio'])} "
            f"| {cell['backlog_explained']}/{cell['backlog_samples']} | {cell['verdict']} |"
        )
    if result["calibrations"]:
        lines += [
            "",
            "## Calibration",
            "",
            "| Source | Cell | Baseline capacity | Candidate diagnostic | Target |",
            "| --- | --- | ---: | ---: | ---: |",
        ]
        for item in result["calibrations"]:
            lines.append(
                f"| {Path(item['source']).parent.name}/{Path(item['source']).name} "
                f"| {item['label']} | {_fmt(item['baseline_capacity'], 0)} "
                f"| {_fmt(item['candidate_capacity_diagnostic'], 0)} | {_fmt(item['target_rate'], 0)} |"
            )
    if result["preflight"]:
        lines += [
            "",
            "## Runner samples",
            "",
            "| Source | CPU frequencies | Throttled | Max °C |",
            "| --- | --- | --- | ---: |",
        ]
        for item in result["preflight"]:
            lines.append(
                f"| {Path(item['source']).name} | {json.dumps(item['cpu_frequencies'])} "
                f"| {json.dumps(item['firmware_throttled'])} | {item['max_temperature_c']} |"
            )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="+", type=Path, help="JSON files or directories")
    parser.add_argument("--backlog-pace", type=float, default=DEFAULT_BACKLOG_PACE)
    parser.add_argument("--fit-tolerance", type=float, default=DEFAULT_FIT_TOLERANCE)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--summary-output", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    result = analyse(args.paths, backlog_pace=args.backlog_pace, tolerance=args.fit_tolerance)
    text = markdown(result)
    if args.output is not None:
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if args.summary_output is not None:
        args.summary_output.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
