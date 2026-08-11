"""Paired same-runner microbenchmarks in fresh, source-isolated workers."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from _paired_scenarios import REGISTRY, run_scenario


SCENARIOS = tuple(REGISTRY)


@dataclass
class WorkerResult:
    scenario: str
    elapsed_s: float
    operations: int
    ops_per_s: float


def _pin(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def _worker(args: argparse.Namespace) -> None:
    _pin(args.cpu)
    measurement = run_scenario(args.scenario)
    result = WorkerResult(
        scenario=args.scenario,
        elapsed_s=measurement.elapsed_s,
        operations=measurement.operations,
        ops_per_s=measurement.ops_per_s,
    )
    print(json.dumps(asdict(result)))


def _run_worker(
    script: Path,
    root: Path,
    scenario: str,
    *,
    cpu: int | None,
) -> WorkerResult:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    command = [sys.executable, str(script), "--worker", "--scenario", scenario]
    if cpu is not None:
        command.extend(("--cpu", str(cpu)))
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return WorkerResult(**json.loads(lines[-1]))


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    payload: dict[str, object] = {
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "repeat": args.repeat,
        "cpu": args.cpu,
        "scenarios": [],
    }
    output_scenarios = payload["scenarios"]
    assert isinstance(output_scenarios, list)
    scenarios = (args.scenario,) if args.scenario is not None else SCENARIOS
    for scenario in scenarios:
        pairs: list[dict[str, object]] = []
        ratios: list[float] = []
        base_rates: list[float] = []
        candidate_rates: list[float] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured = {
                variant: _run_worker(script, roots[variant], scenario, cpu=args.cpu)
                for variant in order
            }
            base_rate = measured["base"].ops_per_s
            candidate_rate = measured["candidate"].ops_per_s
            ratio = candidate_rate / base_rate
            base_rates.append(base_rate)
            candidate_rates.append(candidate_rate)
            ratios.append(ratio)
            pairs.append(
                {
                    "order": list(order),
                    "base_ops_per_s": base_rate,
                    "candidate_ops_per_s": candidate_rate,
                    "candidate_over_base": ratio,
                }
            )
        median_ratio = statistics.median(ratios)
        output_scenarios.append(
            {
                "name": scenario,
                "median_candidate_over_base": median_ratio,
                "min_candidate_over_base": min(ratios),
                "max_candidate_over_base": max(ratios),
                "base_cv": _cv(base_rates),
                "candidate_cv": _cv(candidate_rates),
                "pairs_favouring_candidate": sum(ratio > 1 for ratio in ratios),
                "pairs": pairs,
            }
        )
        print(
            f"{scenario:28s} candidate/base={median_ratio:.4f} "
            f"base_cv={_cv(base_rates):.2%} "
            f"range=[{min(ratios):.4f}, {max(ratios):.4f}]"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--repeat", type=int, default=9)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-regression.json"))
    args = parser.parse_args()
    if args.worker and args.scenario is None:
        parser.error("--scenario is required with --worker")
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    if args.repeat <= 0:
        parser.error("--repeat must be positive")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        _worker(arguments)
    else:
        _parent(arguments)
