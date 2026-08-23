from __future__ import annotations

import asyncio
from argparse import Namespace
from types import SimpleNamespace

from benchmarks.soak import _force_reconnect, _measurement_done, _resource_assessment


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
    decreased = [_snapshot(8, 20.0), _snapshot(8, 20.0), _snapshot(8, 20.0)]
    decreased[2]["asyncio_tasks"] = 3
    leaking = _resource_assessment([_snapshot(8, 20.0), _snapshot(9, 20.0), _snapshot(9, 20.0)])

    assert stable["status"] == "stable"
    assert stable["post_trim_delta_mib"]["rss_mib"] == 2.0  # type: ignore[index]
    assert stable["memory_is_diagnostic"] is True
    assert _resource_assessment(decreased)["status"] == "stable"
    assert leaking["discrete_violations"] == ["fds"]


def test_cycle_measurement_always_runs_once_and_stops_at_requested_count() -> None:
    args = Namespace(duration_seconds=0.0, cycles=3)

    assert not _measurement_done(args, measured_cycles=0, measurement_started=1.0)
    assert not _measurement_done(args, measured_cycles=2, measurement_started=1.0)
    assert _measurement_done(args, measured_cycles=3, measurement_started=1.0)


async def test_forced_reconnect_waits_for_reconnect_task_to_settle() -> None:
    class Client:
        def __init__(self) -> None:
            self.is_connected = True
            self.epoch = 1
            self.reconnect_running = False
            self.settled = asyncio.Event()
            self.settle_task: asyncio.Task[None] | None = None
            self._transport = SimpleNamespace(close=self.close_transport)

        async def close_transport(self) -> None:
            self.epoch += 1
            self.reconnect_running = True
            self.settle_task = asyncio.create_task(self.finish_reconnect())

        async def finish_reconnect(self) -> None:
            await asyncio.sleep(0)
            self.reconnect_running = False
            self.settled.set()

        def stats(self) -> SimpleNamespace:
            return SimpleNamespace(
                connection_epoch=self.epoch,
                tasks=SimpleNamespace(reconnect=self.reconnect_running),
            )

    client = Client()

    await _force_reconnect(client, cycle=1, timeout=1.0)  # type: ignore[arg-type]

    assert client.settled.is_set()
    assert client.settle_task is not None and client.settle_task.done()
    assert client.stats().tasks.reconnect is False
