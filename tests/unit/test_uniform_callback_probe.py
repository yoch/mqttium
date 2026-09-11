from __future__ import annotations

import importlib.util
from pathlib import Path


def probe():
    path = Path(__file__).parents[2] / "benchmarks/uniform_callback_probe.py"
    spec = importlib.util.spec_from_file_location("uniform_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_window_metrics_include_zero_windows_and_exclude_drain():
    p = probe()
    report = p.windows([10.01, 10.09, 10.12, 11.95, 12.01], 10, 2, 0.1)
    assert len(report["counts"]) == 20
    assert sum(report["counts"]) == 4
    assert report["counts"][:3] == [2, 1, 0]
    assert report["zero_windows"] == 17
    assert report["mean_rate"] == 2
    assert report["minimum_rate"] == report["p05_rate"] == 0
    assert report["cv"] > 0
    coarse = p.windows([10.01, 10.09, 10.12, 11.95, 12.01], 10, 2, 1)
    assert coarse["counts"] == [3, 1]


def test_quantiles_are_nearest_rank_and_empty_latency_is_absent():
    p = probe()
    assert p.quantile([], 0.95) is None
    assert p.quantile([7, 1, 2, 5], 0.5) == 2
    assert p.quantile([7, 1, 2, 5], 0.95) == 7
