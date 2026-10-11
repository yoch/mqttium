"""Every isolated memory scenario completes against the current runtime.

The scheduled performance-health workflow is the only caller of the full
harness, so a fixture that drifts from a runtime contract would otherwise fail
weekly instead of on the pull request that broke it. Measurements are not
checked here: ``psutil`` is replaced by a constant probe.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import tracemalloc
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

_BENCHMARKS = Path(__file__).resolve().parents[2] / "benchmarks"


class _ConstantProcess:
    def memory_info(self) -> SimpleNamespace:
        return SimpleNamespace(rss=0)

    def memory_full_info(self) -> SimpleNamespace:
        return SimpleNamespace(rss=0, uss=0, pss=0)

    def num_threads(self) -> int:
        return 1

    def num_fds(self) -> int:
        return 0


@pytest.fixture
def memory_profile(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    monkeypatch.syspath_prepend(str(_BENCHMARKS))
    monkeypatch.setitem(sys.modules, "psutil", SimpleNamespace(Process=_ConstantProcess))
    spec = importlib.util.spec_from_file_location(
        "memory_profile_under_test", _BENCHMARKS / "memory_profile.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    was_tracing = tracemalloc.is_tracing()
    yield module
    if not was_tracing:
        tracemalloc.stop()


def test_every_memory_scenario_runs_at_reduced_scale(memory_profile: ModuleType) -> None:
    thresholds = json.loads((_BENCHMARKS / "memory_thresholds.json").read_text(encoding="utf-8"))
    assert {spec.name for spec in memory_profile.SCENARIOS} == set(thresholds["scenarios"])
    assert {spec.runner for spec in memory_profile.SCENARIOS} == set(memory_profile.RUNNERS)
    for base in memory_profile.SCENARIOS:
        spec = memory_profile._scaled_spec(base, 0.01)
        result = memory_profile.RUNNERS[spec.runner](spec)
        assert result["name"] == spec.name
        assert "loaded" in {snapshot["phase"] for snapshot in result["snapshots"]}, spec.name
