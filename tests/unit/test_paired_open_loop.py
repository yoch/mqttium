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
            completion="receipt",
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
        completions="receipt",
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


@pytest.mark.parametrize("join_setup,join_tail", [(0.0, 0.0), (0.07, 0.02)])
async def test_cpu_interval_excludes_initial_pacing_sleep(
    open_loop, monkeypatch, join_setup, join_tail
):
    from collections import defaultdict

    clock = {"wall": 0.0, "cpu": 0.0}

    class Loop:
        def time(self):
            return clock["wall"]

    class Receipt:
        mid = 1

        async def wait(self):
            pass

    class Client:
        async def connect(self, *_a, **_kw):
            pass

        async def publish(self, *_a, **_kw):
            clock["wall"] += 0.001
            clock["cpu"] += 0.0005
            return Receipt()

        async def disconnect(self):
            pass

    async def connected(*_a):
        return Client()

    async def sleep(delay):
        clock["wall"] += delay
        clock["cpu"] += 0.0001

    real_gather = open_loop.asyncio.gather

    def gather(*tasks):
        joined = real_gather(*tasks)
        clock["wall"] += join_setup
        clock["cpu"] += join_setup

        async def finish():
            await joined
            clock["wall"] += join_tail
            clock["cpu"] += join_tail

        return finish()

    monkeypatch.setattr(open_loop, "_connected_client", connected)
    monkeypatch.setattr(open_loop.asyncio, "get_running_loop", lambda: Loop())
    monkeypatch.setattr(open_loop.asyncio, "sleep", sleep)
    monkeypatch.setattr(open_loop.asyncio, "gather", gather)
    monkeypatch.setattr(open_loop.time, "process_time", lambda: clock["cpu"])
    monkeypatch.setattr(open_loop, "runtime_counters", lambda *_a: defaultdict(int))
    args = Namespace(
        protocol="311",
        window=100,
        completion="receipt",
        host="unused",
        port=0,
        timeout=1,
        target_rate=10,
        count=3,
        payload_bytes=64,
    )
    result = await open_loop.sample(args, "test/timing")
    assert result.cpu_seconds == pytest.approx(0.0017 + join_setup + join_tail)
    assert result.measurement_seconds == pytest.approx(0.201 + join_setup + join_tail)
    assert result.pacing_sleeps == 2
    assert result.completion_ratio == 1.0
    assert result.offered_seconds == pytest.approx(0.201)
    assert result.offered_cpu_seconds == pytest.approx(0.0017)
    assert result.receipt_completed_seconds == pytest.approx(0.201 + join_setup)
    assert result.observer_join_setup_seconds == pytest.approx(join_setup)
    assert result.observer_join_tail_seconds == pytest.approx(join_tail)
    assert result.pending_receipts_after_offer == 3
    assert result.pending_receipts_high_water == 3
    assert len(result.gc_collections) == 3


@pytest.mark.parametrize("retention,expected_peak", [("pending", 256), ("all", 1_024)])
async def test_completed_observers_do_not_accumulate_with_sample_length(
    open_loop, monkeypatch, retention, expected_peak
):
    from collections import defaultdict

    class Receipt:
        mid = 1

        async def wait(self):
            pass

    class Client:
        async def connect(self, *_a, **_kw):
            pass

        async def publish(self, *_a, **_kw):
            await open_loop.asyncio.sleep(0)
            return Receipt()

        async def disconnect(self):
            pass

    async def connected(*_a):
        return Client()

    monkeypatch.setattr(open_loop, "_connected_client", connected)
    monkeypatch.setattr(open_loop, "runtime_counters", lambda *_a: defaultdict(int))
    args = Namespace(
        protocol="311",
        window=100,
        completion="receipt",
        host="unused",
        port=0,
        timeout=1,
        target_rate=0,
        count=1_024,
        payload_bytes=64,
        observer_retention=retention,
    )
    result = await open_loop.sample(args, "test/observers")
    assert result.observer_retention == retention
    assert result.retained_observers_high_water <= expected_peak + 1
    assert result.retained_observers_high_water >= expected_peak
    assert result.pending_receipts_high_water <= 2
    assert result.completion_ratio == 1.0


async def test_observer_retirement_preserves_pending_tasks_and_surfaces_failure(open_loop):
    from collections import deque

    loop = open_loop.asyncio.get_running_loop()
    first, pending, failed = (loop.create_future() for _ in range(3))
    first.set_result(None)
    failed.set_exception(RuntimeError("receipt failed"))
    tasks = deque([first, pending, failed])
    open_loop._retire_completed_observers(tasks)
    assert list(tasks) == [pending, failed]
    pending.set_result(None)
    with pytest.raises(RuntimeError, match="receipt failed"):
        open_loop._retire_completed_observers(tasks)
    assert not tasks


def test_run_worker_forwards_backlog_bound_and_collector_diagnostic(
    open_loop, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: list[list[str]] = []

    def fake_run(command, **_kwargs):
        seen.append(command)
        return subprocess.CompletedProcess(command, 0, '{"completed_rate": 1.0}\n', "")

    monkeypatch.setattr(open_loop.subprocess, "run", fake_run)
    common = {"host": "127.0.0.1", "port": 11883, "timeout": 1.0, "cpu": None}
    for args in (
        Namespace(**common),
        Namespace(**common, max_unacknowledged_messages=1024, gc_disable=True),
    ):
        open_loop._run_worker(
            tmp_path / "benchmark.py",
            tmp_path,
            args,
            mode="sample",
            protocol="5",
            payload_bytes=4096,
            completion="receipt",
            window=100,
            count=10,
            target_rate=26_000.0,
        )

    unchanged, variant = seen
    assert "--max-unacknowledged-messages" not in unchanged
    assert "--gc-disable" not in unchanged
    bound = variant.index("--max-unacknowledged-messages")
    assert variant[bound + 1] == "1024"
    assert "--gc-disable" in variant
