"""Record runner identity and reject unsuitable dedicated-runner conditions."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from benchmark_support import PreflightLimits, evaluate_preflight, runner_metadata, sample_runner


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--max-load-per-cpu", type=float, default=0.25)
    parser.add_argument("--max-cpu-percent", type=float, default=20.0)
    parser.add_argument("--max-temperature-c", type=float, default=80.0)
    parser.add_argument("--required-governor", default="performance")
    parser.add_argument("--require-temperature", action="store_true")
    parser.add_argument(
        "--enforce",
        action="store_true",
        help="exit non-zero when the runner is unsuitable; otherwise only record",
    )
    args = parser.parse_args()
    if args.interval < 0:
        parser.error("--interval must be non-negative")
    limits = PreflightLimits(
        max_load_per_cpu=args.max_load_per_cpu,
        max_cpu_percent=args.max_cpu_percent,
        max_temperature_c=args.max_temperature_c,
        required_governor=args.required_governor or None,
        require_temperature=args.require_temperature,
    )
    sample = sample_runner(interval_s=args.interval)
    failures = evaluate_preflight(sample, limits)
    report = {
        "metadata": runner_metadata(broker_version_command=["mosquitto", "-h"]),
        "sample": sample,
        "limits": asdict(limits),
        "eligible": not failures,
        "failures": failures,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    status = "eligible" if report["eligible"] else "unsuitable"
    print(f"runner preflight: {status}")
    for failure in report["failures"]:
        print(f"- {failure}")
    if args.enforce and not report["eligible"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
