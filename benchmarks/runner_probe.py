"""Record runner identity and reject unsuitable dedicated-runner conditions."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from benchmark_support import PreflightLimits, evaluate_preflight, runner_metadata, sample_runner


def acquire_preflight(
    limits: PreflightLimits,
    *,
    interval_s: float,
    wait_seconds: float = 0.0,
    poll_seconds: float = 5.0,
    consecutive_eligible: int = 1,
) -> dict[str, Any]:
    """Sample until eligibility is stable or the bounded wait budget is exhausted."""
    deadline = time.monotonic() + wait_seconds
    streak = 0
    attempts: list[dict[str, Any]] = []
    final_sample: dict[str, Any] = {}
    final_failures: list[str] = []

    while True:
        final_sample = sample_runner(interval_s=interval_s)
        final_failures = evaluate_preflight(final_sample, limits)
        eligible = not final_failures
        streak = streak + 1 if eligible else 0
        attempts.append(
            {
                "sample": final_sample,
                "eligible": eligible,
                "failures": final_failures,
            }
        )
        if streak >= consecutive_eligible:
            break

        remaining = deadline - time.monotonic()
        if wait_seconds <= 0 or remaining <= 0:
            break
        time.sleep(min(poll_seconds, remaining))

    stable = streak >= consecutive_eligible
    failures = list(final_failures)
    if not stable and wait_seconds > 0:
        failures.append(
            "runner did not produce "
            f"{consecutive_eligible} consecutive eligible sample(s) "
            f"within {wait_seconds:g}s"
        )
    return {
        "metadata": runner_metadata(broker_version_command=["mosquitto", "-h"]),
        "sample": final_sample,
        "limits": asdict(limits),
        "eligible": stable,
        "failures": failures,
        "attempts": attempts,
        "requalification": {
            "wait_seconds": wait_seconds,
            "poll_seconds": poll_seconds,
            "consecutive_eligible": consecutive_eligible,
        },
    }


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
        "--wait-seconds",
        type=float,
        default=0.0,
        help="bounded time to wait for the runner to become stably eligible",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=5.0,
        help="delay between eligibility samples while waiting",
    )
    parser.add_argument(
        "--consecutive-eligible",
        type=int,
        default=1,
        help="number of consecutive eligible samples required",
    )
    parser.add_argument(
        "--enforce",
        action="store_true",
        help="exit non-zero when the runner is unsuitable; otherwise only record",
    )
    args = parser.parse_args()
    if args.interval < 0:
        parser.error("--interval must be non-negative")
    if args.wait_seconds < 0:
        parser.error("--wait-seconds must be non-negative")
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be positive")
    if args.consecutive_eligible <= 0:
        parser.error("--consecutive-eligible must be positive")
    if args.wait_seconds == 0 and args.consecutive_eligible != 1:
        parser.error("--consecutive-eligible > 1 requires --wait-seconds > 0")

    limits = PreflightLimits(
        max_load_per_cpu=args.max_load_per_cpu,
        max_cpu_percent=args.max_cpu_percent,
        max_temperature_c=args.max_temperature_c,
        required_governor=args.required_governor or None,
        require_temperature=args.require_temperature,
    )
    report = acquire_preflight(
        limits,
        interval_s=args.interval,
        wait_seconds=args.wait_seconds,
        poll_seconds=args.poll_seconds,
        consecutive_eligible=args.consecutive_eligible,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    status = "eligible" if report["eligible"] else "unsuitable"
    print(f"runner preflight: {status}")
    if len(report["attempts"]) > 1:
        print(f"- attempts: {len(report['attempts'])}")
    for failure in report["failures"]:
        print(f"- {failure}")
    if args.enforce and not report["eligible"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
