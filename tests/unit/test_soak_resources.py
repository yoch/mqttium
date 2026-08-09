from __future__ import annotations

from argparse import Namespace

from benchmarks.soak import _measurement_done, _resource_assessment


def _snapshot(fds: int, rss: float) -> dict[str, object]:
    return {
        "threads": 1,
        "fds": fds,
        "asyncio_tasks": 4,
        "rss_mib": rss,
        "uss_mib": rss - 1,
        "pss_mib": rss - 0.5,
        "traced_current_mib": 2.0,
    }


def test_resource_assessment_gates_counts_but_only_reports_memory() -> None:
    stable = _resource_assessment([_snapshot(8, 20.0), _snapshot(8, 24.0), _snapshot(8, 22.0)])
    leaking = _resource_assessment([_snapshot(8, 20.0), _snapshot(9, 20.0), _snapshot(9, 20.0)])

    assert stable["status"] == "stable"
    assert stable["post_trim_delta_mib"]["rss_mib"] == 2.0
    assert leaking["discrete_violations"] == ["fds"]


def test_cycle_measurement_always_runs_once_and_stops_at_requested_count() -> None:
    args = Namespace(duration_seconds=0.0, cycles=3)

    assert not _measurement_done(args, measured_cycles=0, measurement_started=1.0)
    assert not _measurement_done(args, measured_cycles=2, measurement_started=1.0)
    assert _measurement_done(args, measured_cycles=3, measurement_started=1.0)
