from __future__ import annotations

import importlib
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

import pytest


@pytest.fixture
def open_loop(monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "benchmarks"))
    sys.modules.pop("paired_open_loop", None)
    return importlib.import_module("paired_open_loop")


def test_load_points_preserve_fraction_defaults_and_make_fixed_rates_explicit(open_loop) -> None:
    default = open_loop._load_points(None, None)
    fixed = open_loop._load_points(None, "5000,10000")
    combined = open_loop._load_points("0.5,0.9", "5000")

    assert [(point.mode, point.value) for point in default] == [
        ("capacity_fraction", 0.5),
        ("capacity_fraction", 0.75),
        ("capacity_fraction", 0.9),
        ("capacity_fraction", 1.0),
    ]
    assert [(point.mode, point.value) for point in fixed] == [
        ("absolute_rate", 5000.0),
        ("absolute_rate", 10000.0),
    ]
    assert [(point.mode, point.value) for point in combined] == [
        ("capacity_fraction", 0.5),
        ("capacity_fraction", 0.9),
        ("absolute_rate", 5000.0),
    ]


def test_parent_windows_preserve_legacy_single_window(open_loop) -> None:
    assert open_loop._parent_windows(100, None) == [100]
    assert open_loop._parent_windows(100, "8,32,64,128") == [8, 32, 64, 128]

    with pytest.raises(ValueError, match="positive"):
        open_loop._parent_windows(100, "8,0")


@pytest.mark.parametrize("value", ("0", "-1", "nan", "inf"))
def test_load_points_reject_non_positive_or_non_finite_rates(open_loop, value: str) -> None:
    with pytest.raises(ValueError, match="positive"):
        open_loop._load_points(None, value)


async def test_callback_completion_tracker_preserves_reused_mid_fifo(open_loop) -> None:
    tracker = open_loop.CallbackCompletionTracker()

    tracker.record_publish(1, 100, True)
    tracker.record_publish(1, 200, False)
    tracker.record_completion(1, 130)
    tracker.record_completion(1, 250)

    assert await tracker.next_sample(0.1) == (0.00003, True)
    assert await tracker.next_sample(0.1) == (0.00005, False)
    assert tracker.published_ns == {}
    assert tracker.pacer_slept == {}


async def test_callback_completion_tracker_matches_early_callback(open_loop) -> None:
    tracker = open_loop.CallbackCompletionTracker()

    tracker.record_completion(7, 150)
    tracker.record_publish(7, 100, True)

    assert await tracker.next_sample(0.1) == (0.00005, True)
    assert tracker.early == {}


async def test_callback_completion_tracker_keeps_latency_only_api(open_loop) -> None:
    tracker = open_loop.CallbackCompletionTracker()

    tracker.record_publish(3, 100)
    tracker.record_completion(3, 140)

    assert await tracker.next_latency_ms(0.1) == 0.00004
    assert tracker.pacer_slept == {}


def test_run_worker_surfaces_subprocess_failure(
    open_loop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        open_loop.subprocess,
        "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 1, "", "worker exploded"),
    )
    args = Namespace(host="127.0.0.1", port=11883, timeout=1.0, cpu=None)

    with pytest.raises(open_loop.InvalidMeasurement, match="worker exploded"):
        open_loop._run_worker(
            tmp_path / "benchmark.py",
            tmp_path,
            args,
            mode="sample",
            protocol="311",
            payload_bytes=64,
            completion="callback",
            window=64,
            count=10,
            target_rate=5000.0,
        )


def test_fixed_rate_parent_records_windows_and_enforces_same_tree_control(
    open_loop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sample_rates = iter((100.0, 104.0, 104.0, 100.0))
    seen_windows: list[int] = []

    def fake_run_worker(*_args, **kwargs):
        seen_windows.append(kwargs["window"])
        if kwargs["mode"] == "calibrate":
            return {"capacity": 12_000.0}
        return {
            "completed_rate": next(sample_rates),
            "ack_latency_p50_ms": 0.5,
            "loop_lag_p95_ms": 0.1,
        }

    monkeypatch.setattr(open_loop, "_run_worker", fake_run_worker)
    output = tmp_path / "open-loop.json"
    args = Namespace(
        base_root=tmp_path,
        candidate_root=tmp_path,
        protocols="311",
        payloads="64",
        completions="receipt",
        window=100,
        windows="8",
        fractions=None,
        target_rates="5000",
        preflight_report=None,
        policy="advisory",
        max_baseline_cv=0.05,
        min_completed_ratio=0.97,
        max_loop_lag_ratio=1.05,
        max_aa_ratio_deviation=0.02,
        cpu=None,
        target_sample_seconds=0.01,
        max_count=10,
        count_small=1,
        count_large=1,
        repeat=2,
        host="127.0.0.1",
        port=11883,
        timeout=1.0,
        output=output,
        summary_output=None,
    )

    assert open_loop.parent(args) == 0

    result = open_loop.json.loads(output.read_text())
    assert seen_windows == [8, 8, 8, 8, 8, 8]
    assert result["harness"]["aa_control"] is True
    assert result["status"] == "invalid"
    assert result["scenarios"][0]["window"] == 8
    assert result["scenarios"][0]["load_mode"] == "absolute_rate"
    assert result["scenarios"][0]["requested_target_rate"] == 5000.0
    assert any("A/A completed ratio" in item for item in result["invalidations"])


def test_parent_invalidates_candidate_only_variability(
    open_loop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    base_root = tmp_path / "base"
    candidate_root = tmp_path / "candidate"
    base_root.mkdir()
    candidate_root.mkdir()
    samples = {
        base_root.resolve(): iter(((100.0, 0.5),) * 4),
        candidate_root.resolve(): iter(((80.0, 0.2), (120.0, 0.8), (80.0, 0.2), (120.0, 0.8))),
    }

    def fake_run_worker(*call_args, **kwargs):
        if kwargs["mode"] == "calibrate":
            return {"capacity": 12_000.0}
        completed_rate, latency = next(samples[Path(call_args[1]).resolve()])
        return {
            "completed_rate": completed_rate,
            "ack_latency_p50_ms": latency,
            "loop_lag_p95_ms": 0.1,
        }

    monkeypatch.setattr(open_loop, "_run_worker", fake_run_worker)
    output = tmp_path / "candidate-variability.json"
    args = Namespace(
        base_root=base_root,
        candidate_root=candidate_root,
        protocols="311",
        payloads="64",
        completions="callback",
        window=64,
        windows=None,
        fractions=None,
        target_rates="5000",
        preflight_report=None,
        policy="advisory",
        max_baseline_cv=0.05,
        min_completed_ratio=0.97,
        max_loop_lag_ratio=1.05,
        max_aa_ratio_deviation=0.02,
        cpu=None,
        target_sample_seconds=0.01,
        max_count=10,
        count_small=1,
        count_large=1,
        repeat=4,
        host="127.0.0.1",
        port=11883,
        timeout=1.0,
        output=output,
        summary_output=None,
    )

    assert open_loop.parent(args) == 0
    result = open_loop.json.loads(output.read_text())
    scenario = result["scenarios"][0]
    assert scenario["base_completed_cv"] == 0.0
    assert scenario["base_ack_latency_p50_cv"] == 0.0
    assert scenario["candidate_completed_cv"] > 0.05
    assert scenario["candidate_ack_latency_p50_cv"] > 0.05
    assert result["status"] == "invalid"
    assert any("candidate completed-rate CV" in item for item in result["invalidations"])
    assert any("candidate p50-latency CV" in item for item in result["invalidations"])
