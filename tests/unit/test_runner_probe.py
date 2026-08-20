from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]


def _runner_probe(monkeypatch: Any) -> Any:
    monkeypatch.syspath_prepend(str(ROOT / "benchmarks"))
    module = importlib.import_module("runner_probe")
    monkeypatch.setattr(module, "runner_metadata", lambda **_kwargs: {"test": True})
    return module


def _sample(*, load: float = 0.0) -> dict[str, object]:
    return {
        "cpu_percent": 0.0,
        "load_1m_per_cpu": load,
        "max_temperature_c": 50.0,
        "cpu_governors": ["performance"],
    }


def test_acquire_preflight_preserves_one_shot_default(monkeypatch: Any) -> None:
    runner_probe = _runner_probe(monkeypatch)
    monkeypatch.setattr(runner_probe, "sample_runner", lambda **_kwargs: _sample())
    monkeypatch.setattr(
        runner_probe.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("unexpected sleep")),
    )

    report = runner_probe.acquire_preflight(
        runner_probe.PreflightLimits(),
        interval_s=0.0,
    )

    assert report["eligible"] is True
    assert report["failures"] == []
    assert len(report["attempts"]) == 1
    assert report["requalification"] == {
        "wait_seconds": 0.0,
        "poll_seconds": 5.0,
        "consecutive_eligible": 1,
    }


def test_acquire_preflight_requires_consecutive_eligible_samples(monkeypatch: Any) -> None:
    runner_probe = _runner_probe(monkeypatch)
    samples = iter((_sample(), _sample(load=0.5), _sample(), _sample()))
    monkeypatch.setattr(runner_probe, "sample_runner", lambda **_kwargs: next(samples))
    now = [0.0]
    sleeps: list[float] = []
    monkeypatch.setattr(runner_probe.time, "monotonic", lambda: now[0])

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(runner_probe.time, "sleep", sleep)

    report = runner_probe.acquire_preflight(
        runner_probe.PreflightLimits(),
        interval_s=0.0,
        wait_seconds=60.0,
        poll_seconds=5.0,
        consecutive_eligible=2,
    )

    assert report["eligible"] is True
    assert [attempt["eligible"] for attempt in report["attempts"]] == [True, False, True, True]
    assert sleeps == [5.0, 5.0, 5.0]


def test_acquire_preflight_fails_closed_after_wait_budget(monkeypatch: Any) -> None:
    runner_probe = _runner_probe(monkeypatch)
    monkeypatch.setattr(runner_probe, "sample_runner", lambda **_kwargs: _sample(load=0.5))
    now = [0.0]
    monkeypatch.setattr(runner_probe.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(
        runner_probe.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds)
    )

    report = runner_probe.acquire_preflight(
        runner_probe.PreflightLimits(),
        interval_s=0.0,
        wait_seconds=10.0,
        poll_seconds=5.0,
        consecutive_eligible=2,
    )

    assert report["eligible"] is False
    assert len(report["attempts"]) == 3
    assert any("1-minute load per CPU" in failure for failure in report["failures"])
    assert report["failures"][-1] == (
        "runner did not produce 2 consecutive eligible sample(s) within 10s"
    )
