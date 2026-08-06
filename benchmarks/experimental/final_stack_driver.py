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
    roots: dict[str, Path],
    harness: Path,
    scenario: str,
    root_name: str,
    variant: str,
    messages: int,
    pairs: int,
    window: int,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(roots[root_name] / "src")
    command = [
        sys.executable,
        str(harness),
        "--worker",
        "--scenario",
        scenario,
        "--variant",
        variant,
    ]
    if scenario == "rtt":
        command += ["--pairs", str(pairs), "--window", str(window)]
    else:
        command += ["--messages", str(messages)]
    proc = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    line = next(
        item.removeprefix("RESULT ")
        for item in reversed(proc.stdout.splitlines())
        if item.startswith("RESULT ")
    )
    return json.loads(line)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-root", type=Path, required=True)
    parser.add_argument("--optimized-root", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--messages", type=int, default=160_000)
    parser.add_argument("--pairs", type=int, default=10_000)
    parser.add_argument("--window", type=int, default=32)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    roots = {
        "base": args.base_root.resolve(),
        "optimized": args.optimized_root.resolve(),
    }
    cases = {
        "ingress_direct": {
            "main-default": ("base", "mqttium-baseline"),
            "optimized-default": ("optimized", "mqttium-baseline"),
            "optimized-inline": ("optimized", "mqttium-sync-inline"),
            "gmqtt": ("base", "gmqtt"),
        },
        "ingress_broker": {
            "main-default": ("base", "mqttium-baseline"),
            "optimized-default": ("optimized", "mqttium-baseline"),
            "optimized-inline": ("optimized", "mqttium-sync-inline"),
            "gmqtt": ("base", "gmqtt"),
        },
        "rtt": {
            "main-default": ("base", "mqttium-nowait"),
            "optimized-default": ("optimized", "mqttium-nowait"),
            "optimized-inline": ("optimized", "mqttium-sync-inline-nowait"),
            "gmqtt": ("base", "gmqtt"),
        },
    }
    labels = ("main-default", "optimized-default", "optimized-inline", "gmqtt")
    rows: list[dict[str, Any]] = []
    for cycle in range(args.cycles):
        order = labels if cycle % 2 == 0 else tuple(reversed(labels))
        row: dict[str, Any] = {"cycle": cycle, "order": order, "scenarios": {}}
        for scenario, mapping in cases.items():
            row["scenarios"][scenario] = {}
            for label in order:
                root_name, variant = mapping[label]
                row["scenarios"][scenario][label] = _sample(
                    roots=roots,
                    harness=args.harness,
                    scenario=scenario,
                    root_name=root_name,
                    variant=variant,
                    messages=args.messages,
                    pairs=args.pairs,
                    window=args.window,
                )
        rows.append(row)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"rows": rows}, indent=2) + "\n")

    summary: dict[str, Any] = {}
    for scenario in cases:
        medians = {
            label: statistics.median(
                row["scenarios"][scenario][label]["rate"] for row in rows
            )
            for label in labels
        }
        summary[scenario] = {
            "median_rates": medians,
            "optimized_default_vs_main": medians["optimized-default"]
            / medians["main-default"],
            "optimized_default_vs_gmqtt": medians["optimized-default"]
            / medians["gmqtt"],
            "optimized_inline_vs_gmqtt": medians["optimized-inline"]
            / medians["gmqtt"],
        }
        if scenario == "rtt":
            summary[scenario]["median_p50_ms"] = {
                label: statistics.median(
                    row["scenarios"][scenario][label]["p50_ms"] for row in rows
                )
                for label in labels
            }
            summary[scenario]["median_p95_ms"] = {
                label: statistics.median(
                    row["scenarios"][scenario][label]["p95_ms"] for row in rows
                )
                for label in labels
            }
    result = {"rows": rows, "summary": summary}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
