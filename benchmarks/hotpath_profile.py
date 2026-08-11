"""Record exact Python call counts and allocation peaks for micro scenarios."""

from __future__ import annotations

import argparse
import cProfile
import json
import pstats
import tracemalloc
from pathlib import Path
from typing import Any

from _paired_scenarios import REGISTRY, run_scenario


def _profile(name: str, top: int) -> dict[str, object]:
    profiler = cProfile.Profile()
    measurement = profiler.runcall(run_scenario, name)
    stats = pstats.Stats(profiler)
    raw_stats: Any = stats
    hottest = sorted(
        (
            {
                "file": key[0],
                "line": key[1],
                "function": key[2],
                "primitive_calls": value[0],
                "calls": value[1],
                "total_s": value[2],
                "cumulative_s": value[3],
            }
            for key, value in raw_stats.stats.items()
        ),
        key=lambda item: float(item["total_s"]),
        reverse=True,
    )[:top]

    tracemalloc.start()
    allocation_measurement = run_scenario(name)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "scenario": name,
        "operations": measurement.operations,
        "profiled_elapsed_s": measurement.elapsed_s,
        "calls": raw_stats.total_calls,
        "primitive_calls": raw_stats.prim_calls,
        "calls_per_operation": raw_stats.total_calls / measurement.operations,
        "primitive_calls_per_operation": raw_stats.prim_calls / measurement.operations,
        "allocation_run_elapsed_s": allocation_measurement.elapsed_s,
        "tracemalloc_current_bytes": current,
        "tracemalloc_peak_bytes": peak,
        "top_functions": hottest,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=tuple(REGISTRY), action="append")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.top <= 0:
        parser.error("--top must be positive")
    scenarios = args.scenario or list(REGISTRY)
    payload = {"scenarios": [_profile(name, args.top) for name in scenarios]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
