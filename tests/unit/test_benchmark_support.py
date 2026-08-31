from __future__ import annotations

from benchmarks.benchmark_support import (
    PreflightLimits,
    evaluate_preflight,
)


def test_preflight_reports_every_independent_failure() -> None:
    failures = evaluate_preflight(
        {
            "load_1m_per_cpu": 0.5,
            "cpu_percent": 30.0,
            "max_temperature_c": 90.0,
            "cpu_governors": ["powersave"],
        },
        PreflightLimits(),
    )

    assert len(failures) == 4
    assert any("load" in failure for failure in failures)
    assert any("CPU use" in failure for failure in failures)
    assert any("temperature" in failure for failure in failures)
    assert any("governors" in failure for failure in failures)


def test_temperature_can_be_required_on_a_dedicated_runner() -> None:
    failures = evaluate_preflight(
        {
            "load_1m_per_cpu": 0.0,
            "cpu_percent": 0.0,
            "max_temperature_c": None,
            "cpu_governors": ["performance"],
        },
        PreflightLimits(require_temperature=True),
    )

    assert failures == ["temperature sensors are unavailable"]


def test_historical_load_can_be_ignored_after_initial_preflight() -> None:
    failures = evaluate_preflight(
        {
            "load_1m_per_cpu": 4.0,
            "cpu_percent": 0.0,
            "max_temperature_c": 50.0,
            "cpu_governors": ["performance"],
        },
        PreflightLimits(max_load_per_cpu=None),
    )

    assert failures == []
