from __future__ import annotations

import sys

import pytest

from benchmarks.benchmark_support import (
    PreflightLimits,
    _cpu_frequencies,
    _firmware_throttled,
    evaluate_preflight,
)

_IDLE = {"load_1m_per_cpu": 0.0, "cpu_percent": 0.0, "cpu_governors": ["performance"]}


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


def test_cpu_frequencies_record_current_max_and_residency(tmp_path) -> None:
    policy = tmp_path / "cpu2" / "cpufreq"
    (policy / "stats").mkdir(parents=True)
    (policy / "scaling_cur_freq").write_text("1500000\n")
    (policy / "scaling_max_freq").write_text("2400000\n")
    (policy / "cpuinfo_max_freq").write_text("2400000\n")
    (policy / "stats" / "time_in_state").write_text("1500000 120\n2400000 98000\n")
    (tmp_path / "cpu3" / "cpufreq").mkdir(parents=True)

    frequencies = _cpu_frequencies(tmp_path)

    assert frequencies["cpu2"] == {
        "scaling_cur_freq": 1_500_000,
        "scaling_max_freq": 2_400_000,
        "cpuinfo_max_freq": 2_400_000,
        "time_in_state": {"1500000": 120, "2400000": 98000},
    }
    assert frequencies["cpu3"]["time_in_state"] is None


@pytest.mark.parametrize(
    ("output", "expected"),
    [("throttled=0x50005", 0x50005), ("throttled=0x0", 0), ("unexpected", None)],
)
def test_firmware_throttled_parses_the_vcgencmd_register(output, expected) -> None:
    assert _firmware_throttled([sys.executable, "-c", f"print({output!r})"]) == expected


def test_firmware_throttled_is_none_without_vcgencmd() -> None:
    assert _firmware_throttled(["mqttium-no-such-vcgencmd"]) is None


@pytest.mark.parametrize(
    ("register", "rejected"),
    [(None, False), (0x0, False), (0x50000, False), (0x50005, True), (0x8, True)],
)
def test_only_current_firmware_limits_make_a_sample_unsuitable(register, rejected) -> None:
    # Bits 16-19 record limits since boot; only bits 0-3 describe the present.
    failures = evaluate_preflight({**_IDLE, "firmware_throttled": register}, PreflightLimits())
    assert bool(failures) is rejected
    if rejected:
        assert "firmware reports" in failures[0]
