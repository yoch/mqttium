"""Fail on a memory regression in a memory_profile.py run.

The benchmarks workflow only uploaded JSON, so nothing turned red when retained
memory grew. This compares a run against benchmarks/memory_thresholds.json and
exits non-zero on a breach.

Checks per scenario:

* ``max_traced_peak_mib`` bounds the tracemalloc peak. That measures Python
  allocations and is comparable across runners.
* Selected aggregate scenarios may also define ``max_loaded_rss_delta_mib``.
  Those gates are deliberately limited to stable GitHub Actions runners.
* ``logical`` asserts the scenario's ``loaded`` counters exactly. Without it a
  benchmark that silently stopped doing the same work would look like an
  improvement.

Usage:
    python benchmarks/check_memory_thresholds.py --profile memory-profile.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_THRESHOLDS = Path(__file__).with_name("memory_thresholds.json")


def loaded_counters(scenario: dict[str, Any]) -> dict[str, Any]:
    for snapshot in scenario.get("snapshots", []):
        if snapshot.get("phase") == "loaded":
            counters: dict[str, Any] = snapshot.get("logical") or {}
            return counters
    return {}


def traced_peak_mib(scenario: dict[str, Any]) -> float:
    peaks = [s.get("traced_peak_mib", 0.0) for s in scenario.get("snapshots", [])]
    return max(peaks, default=0.0)


def loaded_rss_delta_mib(scenario: dict[str, Any]) -> float | None:
    for snapshot in scenario.get("snapshots", []):
        if snapshot.get("phase") == "loaded":
            value = snapshot.get("rss_delta_mib")
            return None if value is None else float(value)
    return None


def check(profile: dict[str, Any], thresholds: dict[str, Any]) -> list[str]:
    scenarios = {s["name"]: s for s in profile.get("scenarios", [])}
    failures: list[str] = []

    for name, limits in thresholds["scenarios"].items():
        scenario = scenarios.get(name)
        if scenario is None:
            failures.append(f"{name}: missing from the profile")
            continue

        peak = traced_peak_mib(scenario)
        limit = limits["max_traced_peak_mib"]
        if peak > limit:
            failures.append(f"{name}: traced peak {peak:.2f} MiB exceeds {limit:.2f} MiB")

        rss_limit = limits.get("max_loaded_rss_delta_mib")
        if rss_limit is not None:
            rss_delta = loaded_rss_delta_mib(scenario)
            if rss_delta is None:
                failures.append(f"{name}: loaded RSS delta is unavailable")
            elif rss_delta > rss_limit:
                failures.append(
                    f"{name}: loaded RSS delta {rss_delta:.2f} MiB exceeds {rss_limit:.2f} MiB"
                )

        counters = loaded_counters(scenario)
        for key, expected in limits.get("logical", {}).items():
            actual = counters.get(key)
            if actual != expected:
                failures.append(f"{name}: logical {key} is {actual!r}, expected {expected!r}")

    unknown = set(scenarios) - set(thresholds["scenarios"])
    if unknown:
        failures.append(
            "no threshold for " + ", ".join(sorted(unknown)) + "; add one to memory_thresholds.json"
        )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path, help="memory_profile.py JSON output")
    parser.add_argument("--thresholds", type=Path, default=DEFAULT_THRESHOLDS)
    args = parser.parse_args()

    profile = json.loads(args.profile.read_text())
    thresholds = json.loads(args.thresholds.read_text())

    failures = check(profile, thresholds)
    for name, limits in thresholds["scenarios"].items():
        scenario = next((s for s in profile.get("scenarios", []) if s["name"] == name), None)
        if scenario is not None:
            peak = traced_peak_mib(scenario)
            headroom = limits["max_traced_peak_mib"] - peak
            print(
                f"{name:32s} peak={peak:7.2f} MiB  limit={limits['max_traced_peak_mib']:7.2f} MiB  headroom={headroom:+7.2f}"
            )

    if failures:
        print("\nMemory regression:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print("\nAll scenarios within their memory thresholds.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
