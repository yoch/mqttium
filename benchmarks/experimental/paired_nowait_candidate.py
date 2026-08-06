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
    variant: str,
    pairs: int,
    window: int,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    proc = subprocess.run(
        [
            sys.executable,
            str(harness),
            "--worker",
            "--scenario",
            "rtt",
            "--variant",
            variant,
            "--pairs",
            str(pairs),
            "--window",
            str(window),
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
    parser.add_argument("--pairs", type=int, default=12_000)
    parser.add_argument("--window", type=int, default=32)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    roots = {
        "base": args.base_root.resolve(),
        "candidate": args.candidate_root.resolve(),
    }
    variants = (
        "mqttium-await",
        "mqttium-nowait",
        "mqttium-sync-inline-nowait",
    )
    cycles: list[dict[str, Any]] = []
    for cycle in range(args.cycles):
        order = ("base", "candidate") if cycle % 2 == 0 else ("candidate", "base")
        row: dict[str, Any] = {"cycle": cycle, "order": order, "samples": {}}
        for variant in variants:
            row["samples"][variant] = {}
            for root_name in order:
                row["samples"][variant][root_name] = _sample(
                    root=roots[root_name],
                    harness=args.harness,
                    variant=variant,
                    pairs=args.pairs,
                    window=args.window,
                )
        cycles.append(row)

    summary: dict[str, Any] = {}
    for variant in variants:
        rate_ratios = [
            row["samples"][variant]["candidate"]["rate"] / row["samples"][variant]["base"]["rate"]
            for row in cycles
        ]
        p50_ratios = [
            row["samples"][variant]["candidate"]["p50_ms"]
            / row["samples"][variant]["base"]["p50_ms"]
            for row in cycles
        ]
        p95_ratios = [
            row["samples"][variant]["candidate"]["p95_ms"]
            / row["samples"][variant]["base"]["p95_ms"]
            for row in cycles
        ]
        summary[variant] = {
            "rate_ratio_median": statistics.median(rate_ratios),
            "rate_ratios": rate_ratios,
            "p50_ratio_median": statistics.median(p50_ratios),
            "p95_ratio_median": statistics.median(p95_ratios),
            "base_rate_median": statistics.median(
                row["samples"][variant]["base"]["rate"] for row in cycles
            ),
            "candidate_rate_median": statistics.median(
                row["samples"][variant]["candidate"]["rate"] for row in cycles
            ),
        }

    result = {"cycles": cycles, "summary": summary}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
