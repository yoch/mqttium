from __future__ import annotations

import sys
from pathlib import Path

# paired_writer_capacity.py is a standalone benchmark script and imports its
# sibling paired_network.py by script-directory name. Import it the same way the
# executable is loaded rather than giving pytest a different module graph.
_BENCHMARKS = Path(__file__).resolve().parents[2] / "benchmarks"
if str(_BENCHMARKS) not in sys.path:
    sys.path.insert(0, str(_BENCHMARKS))

from paired_writer_capacity import assess_rates  # noqa: E402


def _assess(base: list[float], candidate: list[float], *, aa: bool = False):
    return assess_rates(
        base,
        candidate,
        max_baseline_cv=0.05,
        min_completed_ratio=0.95,
        aa_control=aa,
        max_aa_ratio_deviation=0.02,
        label="qos=0",
    )


def test_rc6_class_capacity_collapse_is_a_regression() -> None:
    ratio, baseline_cv, invalidations, regressions = _assess(
        [61_000.0, 61_300.0, 60_900.0, 61_200.0],
        [30_000.0, 30_500.0, 30_100.0, 30_400.0],
    )

    assert baseline_cv < 0.05
    assert ratio < 0.51
    assert invalidations == []
    assert regressions == [f"qos=0: candidate/base completed ratio {ratio:.4f}"]


def test_restored_capacity_above_95_percent_passes() -> None:
    ratio, _baseline_cv, invalidations, regressions = _assess(
        [61_000.0, 61_300.0, 60_900.0, 61_200.0],
        [60_000.0, 60_200.0, 59_900.0, 60_100.0],
    )

    assert ratio >= 0.95
    assert invalidations == []
    assert regressions == []


def test_aa_ratio_drift_over_two_percent_invalidates_measurement() -> None:
    ratio, _baseline_cv, invalidations, regressions = _assess(
        [20_000.0, 20_100.0, 19_900.0, 20_050.0],
        [21_000.0, 21_100.0, 20_900.0, 21_050.0],
        aa=True,
    )

    assert ratio > 1.02
    assert invalidations == [
        f"qos=0: A/A completed ratio {ratio:.4f} outside 1+/-2.00%"
    ]
    assert regressions == []


def test_noisy_baseline_invalidates_measurement() -> None:
    _ratio, baseline_cv, invalidations, regressions = _assess(
        [18_000.0, 22_000.0, 19_000.0, 23_000.0],
        [19_000.0, 21_000.0, 20_000.0, 22_000.0],
    )

    assert baseline_cv > 0.05
    assert invalidations == [f"qos=0: baseline CV {baseline_cv:.2%}"]
    assert regressions == []
