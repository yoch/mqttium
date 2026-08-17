from __future__ import annotations

import json
import subprocess
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest

from benchmarks.paired_network import (
    CallbackGenerationTracker,
    InvalidMeasurement,
    _eligibility,
    _evaluation,
    _write_evaluation,
    arm_variability,
    calibrated_sample_count,
    run_worker,
)


def test_advisory_failure_is_visible_but_non_blocking(tmp_path) -> None:
    eligibility = {"checked": True, "eligible": True, "failures": []}

    assert _evaluation(policy="advisory", eligibility=eligibility, failures=["ratio 0.94"]) == (
        "failed",
        0,
    )


def test_strict_distinguishes_invalid_runner_and_regression() -> None:
    ineligible = {"checked": True, "eligible": False, "failures": ["busy runner"]}
    eligible = {"checked": True, "eligible": True, "failures": []}

    assert _evaluation(policy="strict", eligibility=ineligible, failures=[]) == ("invalid", 2)
    assert _evaluation(policy="strict", eligibility=eligible, failures=["ratio 0.94"]) == (
        "failed",
        1,
    )
    assert _evaluation(
        policy="strict",
        eligibility=eligible,
        failures=["baseline CV 6%"],
        invalid=True,
    ) == ("invalid", 2)
    assert _evaluation(policy="strict", eligibility=eligible, failures=[]) == ("passed", 0)


def test_evaluation_is_written_before_exit_decision(tmp_path) -> None:
    output = tmp_path / "paired.json"
    summary = tmp_path / "paired.md"
    payload = {
        "policy": "strict",
        "status": "failed",
        "eligibility": {"checked": True, "eligible": True, "failures": []},
        "failures": ["candidate/base 0.9400"],
        "scenarios": [{}],
    }

    _write_evaluation(Namespace(output=output, summary_output=summary), payload)

    persisted = json.loads(output.read_text())
    assert persisted["status"] == "failed"
    assert persisted["failures"] == ["candidate/base 0.9400"]
    assert "candidate/base 0.9400" in summary.read_text()


def test_invalid_advisory_summary_is_explicit(tmp_path) -> None:
    output = tmp_path / "paired.json"
    summary = tmp_path / "paired.md"
    payload = {
        "policy": "advisory",
        "status": "invalid",
        "eligibility": {"checked": True, "eligible": True, "failures": []},
        "failures": ["baseline CV 8.49%"],
        "scenarios": [{}],
    }

    _write_evaluation(Namespace(output=output, summary_output=summary), payload)

    text = summary.read_text()
    assert "Status: **invalid**" in text
    assert "not release evidence" in text


def test_missing_and_malformed_preflight_are_invalid(tmp_path) -> None:
    missing = _eligibility(None)
    malformed_path = tmp_path / "runner.json"
    malformed_path.write_text("not-json")
    malformed = _eligibility(malformed_path)

    assert missing["eligible"] is None
    assert malformed["eligible"] is False
    assert malformed["failures"]


def test_worker_failure_is_classified_as_invalid_measurement(monkeypatch) -> None:
    def fail(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args, kwargs
        return subprocess.CompletedProcess([], 1, stdout="", stderr="broker disconnected")

    monkeypatch.setattr(subprocess, "run", fail)
    args = Namespace(host="127.0.0.1", port=1, timeout=0.1)

    with pytest.raises(InvalidMeasurement, match="broker disconnected"):
        run_worker(
            Path("benchmarks/paired_network.py"),
            Path("."),
            args,
            payload_bytes=64,
            count=1,
            window=1,
            protocol="311",
            completion="receipt",
        )


def test_sample_count_enforces_duration_and_cap() -> None:
    assert calibrated_sample_count(750, [10_000.0, 12_000.0], 1.5, 50_000) == 18_000
    assert calibrated_sample_count(20_000, [1_000.0, 1_200.0], 1.5, 50_000) == 20_000
    assert calibrated_sample_count(750, [100_000.0, 90_000.0], 1.5, 50_000) == 50_000


def test_callback_generation_tracker_preserves_reused_mid_fifo() -> None:
    tracker = CallbackGenerationTracker()

    assert tracker.register(7, 100) is None
    assert tracker.register(7, 200) is None
    assert tracker.complete(7, 150) == (100, 150)
    assert tracker.complete(7, 250) == (200, 250)
    assert tracker.pending_counts() == (0, 0)


def test_callback_generation_tracker_matches_early_callback() -> None:
    tracker = CallbackGenerationTracker()

    assert tracker.complete(9, 150) is None
    assert tracker.register(9, 100) == (100, 150)
    assert tracker.pending_counts() == (0, 0)


def test_candidate_only_variability_invalidates_measurement() -> None:
    base_cv, candidate_cv, invalidations = arm_variability(
        "window=8",
        [100.0, 100.5, 99.5, 100.0],
        [80.0, 120.0, 85.0, 115.0],
        0.05,
    )

    assert base_cv < 0.05
    assert candidate_cv > 0.05
    assert invalidations == [f"window=8: candidate CV {candidate_cv:.2%}"]


def test_observer_runs_outside_publisher_and_preserves_sequence() -> None:
    sent = time.monotonic_ns()
    payload = b"".join(
        f"{sequence:016x}{sent:016x}".encode("ascii") + b"payload\n" for sequence in range(3)
    )
    completed = subprocess.run(
        [sys.executable, "benchmarks/paired_network.py", "--observer-worker"],
        input=payload,
        capture_output=True,
        check=True,
    )

    result = json.loads(completed.stdout)
    assert result["sequences"] == [0, 1, 2]
    assert len(result["latencies_ms"]) == 3
    assert all(latency >= 0 for latency in result["latencies_ms"])
