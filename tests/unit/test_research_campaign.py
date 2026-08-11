from __future__ import annotations

from argparse import Namespace

import pytest

from benchmarks.research_campaign import (
    UsageSpec,
    _network_profile,
    _usage_command,
    comparison_summary,
)


def _args(**overrides: object) -> Namespace:
    values = {
        "host": "127.0.0.1",
        "port": 11883,
        "timeout": 30.0,
        "cpu": 3,
        "cafile": None,
        "ws_url": None,
        "network_profile": "local",
        "allow_netem": False,
    }
    values.update(overrides)
    return Namespace(**values)


def test_usage_command_is_a_clean_worker_without_profiler() -> None:
    command = _usage_command(
        UsageSpec("reliable", "reliable", "common", count=50, window=8),
        _args(),
    )

    assert command[0].endswith("python")
    assert "research_scenarios.py" in command[1]
    assert command[command.index("--count") + 1] == "50"
    assert command[command.index("--window") + 1] == "8"
    assert "py-spy" not in command
    assert "perf" not in command
    assert "strace" not in command


def test_quick_comparison_requires_large_unanimous_signal() -> None:
    pairs = [
        {"base_rate": 100.0, "candidate_rate": 108.0},
        {"base_rate": 102.0, "candidate_rate": 110.0},
        {"base_rate": 98.0, "candidate_rate": 106.0},
    ]

    summary = comparison_summary(pairs, confirm=False)

    assert summary["quick_signal"] is True
    assert summary["pairs_favouring_candidate"] == 3
    assert summary["formal_network_gain"] is False


def test_formal_summary_rejects_noisy_baseline() -> None:
    pairs = [
        {"base_rate": 80.0, "candidate_rate": 100.0},
        {"base_rate": 120.0, "candidate_rate": 140.0},
        {"base_rate": 100.0, "candidate_rate": 120.0},
    ]

    summary = comparison_summary(pairs, confirm=True)

    assert summary["base_cv"] > 0.05
    assert summary["formal_network_gain"] is False


def test_aa_control_must_stay_within_two_percent() -> None:
    pairs = [
        {"base_rate": 100.0, "candidate_rate": 107.0},
        {"base_rate": 101.0, "candidate_rate": 108.0},
        {"base_rate": 99.0, "candidate_rate": 106.0},
        {"base_rate": 100.0, "candidate_rate": 107.0},
    ]

    summary = comparison_summary(pairs, confirm=False, aa_control=True)

    assert summary["neutral_control"] is False
    assert summary["valid"] is False
    assert summary["quick_signal"] is False


def test_netem_profile_requires_explicit_authorisation() -> None:
    with pytest.raises(RuntimeError, match="--allow-netem"):
        with _network_profile(_args(network_profile="wan")):
            pass
