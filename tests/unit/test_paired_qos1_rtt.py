from __future__ import annotations

import sys
from pathlib import Path

import pytest

_BENCHMARKS = Path(__file__).resolve().parents[2] / "benchmarks"
if str(_BENCHMARKS) not in sys.path:
    sys.path.insert(0, str(_BENCHMARKS))

from paired_qos1_rtt import (  # noqa: E402
    _writer_policy_counters,
    assess_callback_pairs,
    percentile,
)


def _pair(
    *,
    base_p50: float = 100.0,
    candidate_p50: float = 100.0,
    base_p95: float = 200.0,
    candidate_p95: float = 200.0,
    base_rate: float = 1_000.0,
    candidate_rate: float = 1_000.0,
) -> dict[str, object]:
    return {
        "base": {
            "latency_p50_ns": base_p50,
            "latency_p95_ns": base_p95,
            "latency_p99_ns": base_p95 * 2,
            "operations_per_second": base_rate,
        },
        "candidate": {
            "latency_p50_ns": candidate_p50,
            "latency_p95_ns": candidate_p95,
            "latency_p99_ns": candidate_p95 * 2,
            "operations_per_second": candidate_rate,
        },
    }


def test_percentile_interpolates_without_mutating_input() -> None:
    values = [4.0, 1.0, 3.0, 2.0]
    assert percentile(values, 50) == 2.5
    assert percentile(values, 95) == pytest.approx(3.85)
    assert values == [4.0, 1.0, 3.0, 2.0]


def test_stable_aa_callback_control_passes() -> None:
    pairs = [_pair(base_p50=100 + index, candidate_p50=100.5 + index) for index in range(8)]
    summary, invalidations = assess_callback_pairs(
        pairs, aa_control=True, max_cv=0.05, max_aa_drift=0.05
    )

    assert invalidations == []
    assert summary["latency_p50_ns"]["median_candidate_over_base"] == pytest.approx(
        1.0048, rel=0.01
    )


def test_tail_jitter_is_reported_but_does_not_invalidate_repeatability() -> None:
    pairs = [
        _pair(base_p95=100.0 if index % 2 else 500.0, candidate_p95=300.0) for index in range(8)
    ]
    summary, invalidations = assess_callback_pairs(
        pairs, aa_control=False, max_cv=0.05, max_aa_drift=0.05
    )

    assert summary["latency_p95_ns"]["base_cv"] > 0.05
    assert invalidations == []


def test_noisy_p50_and_rate_invalidate_the_sample() -> None:
    pairs = [
        _pair(
            base_p50=value,
            candidate_p50=100.0,
            base_rate=1_000.0,
            candidate_rate=rate,
        )
        for value, rate in zip(
            (80.0, 120.0, 85.0, 115.0),
            (800.0, 1_200.0, 850.0, 1_150.0),
            strict=True,
        )
    ]
    _summary, invalidations = assess_callback_pairs(
        pairs, aa_control=False, max_cv=0.05, max_aa_drift=0.05
    )

    assert any(reason.startswith("latency_p50_ns: baseline CV") for reason in invalidations)
    assert any(reason.startswith("operations_per_second: candidate CV") for reason in invalidations)


async def test_policy_counters_preserve_one_eager_qos0_write() -> None:
    counters = await _writer_policy_counters()

    assert counters["qos0_burst"] == {"burst": 16, "eager": 1, "queued": 15}
    assert len(counters["ack_bursts"]) == 6
