from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any


def _sample(
    *,
    root: Path,
    harness: Path,
    scenario: str,
    variant: str,
    messages: int,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    proc = subprocess.run(
        [
            sys.executable,
            str(harness),
            "--worker",
            "--scenario",
            scenario,
            "--variant",
            variant,
            "--messages",
            str(messages),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    result_line = next(
        line.removeprefix("RESULT ")
        for line in reversed(proc.stdout.splitlines())
        if line.startswith("RESULT ")
    )
    return json.loads(result_line)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=7)
    parser.add_argument("--messages", type=int, default=180_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    roots = {
        "base": args.base_root.resolve(),
        "candidate": args.candidate_root.resolve(),
    }
    scenarios = ("ingress_direct", "ingress_broker")
    variants = ("mqttium-baseline", "mqttium-sync-inline")
    cycles: list[dict[str, Any]] = []
    for cycle in range(args.cycles):
        order = ("base", "candidate") if cycle % 2 == 0 else ("candidate", "base")
        row: dict[str, Any] = {"cycle": cycle, "order": order, "samples": {}}
        for scenario in scenarios:
            row["samples"][scenario] = {}
            for variant in variants:
                row["samples"][scenario][variant] = {}
                for root_name in order:
                    row["samples"][scenario][variant][root_name] = _sample(
                        root=roots[root_name],
                        harness=args.harness,
                        scenario=scenario,
                        variant=variant,
                        messages=args.messages,
                    )
        cycles.append(row)

    summary: dict[str, Any] = {}
    for scenario in scenarios:
        summary[scenario] = {}
        for variant in variants:
            rate_ratios = [
                row["samples"][scenario][variant]["candidate"]["rate"]
                / row["samples"][scenario][variant]["base"]["rate"]
                for row in cycles
            ]
            lag_ratios = [
                row["samples"][scenario][variant]["candidate"]["loop_lag_p95_ms"]
                / row["samples"][scenario][variant]["base"]["loop_lag_p95_ms"]
                for row in cycles
                if row["samples"][scenario][variant]["base"]["loop_lag_p95_ms"] > 0
            ]
            summary[scenario][variant] = {
                "rate_ratio_median": statistics.median(rate_ratios),
                "rate_ratios": rate_ratios,
                "loop_lag_p95_ratio_median": statistics.median(lag_ratios),
                "base_rate_median": statistics.median(
                    row["samples"][scenario][variant]["base"]["rate"] for row in cycles
                ),
                "candidate_rate_median": statistics.median(
                    row["samples"][scenario][variant]["candidate"]["rate"] for row in cycles
                ),
            }

    result = {"cycles": cycles, "summary": summary}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
