"""Harness-level tests for the cooperative checkpoint scheduler."""

from __future__ import annotations

import asyncio

from tests.concurrency.scheduler import (
    CooperativeScheduler,
    DeadlockError,
    Schedule,
    ScheduleMismatch,
)


async def test_explicit_schedule_is_replayable_and_printable() -> None:
    log: list[str] = []

    async def actor_a(scheduler: CooperativeScheduler) -> None:
        log.append("a-start")
        await scheduler.checkpoint("a.mid")
        log.append("a-end")

    async def actor_b(scheduler: CooperativeScheduler) -> None:
        log.append("b-start")
        await scheduler.checkpoint("b.mid")
        log.append("b-end")

    text = "resume a @ a.mid #1\nresume b @ b.mid #1\n"
    schedule = Schedule.parse("# policy=explicit\n" + text)
    assert "resume a @ a.mid #1" in schedule.format()

    async def run_once(replay: Schedule | None = None) -> list[str]:
        log.clear()
        scheduler = CooperativeScheduler(timeout=1.0, idle_timeout=0.3)
        result = await scheduler.run(
            [("a", actor_a(scheduler)), ("b", actor_b(scheduler))],
            schedule=replay,
        )
        assert result.ok, result.error
        return list(log)

    first = await run_once(schedule)
    assert first == ["a-start", "b-start", "a-end", "b-end"]
    second = await run_once(schedule)
    assert second == first


async def test_first_chooser_drains_in_arrival_order() -> None:
    log: list[str] = []
    scheduler = CooperativeScheduler(timeout=1.0, idle_timeout=0.3)

    async def actor(name: str) -> None:
        log.append(f"{name}-start")
        await scheduler.checkpoint("mid")
        log.append(f"{name}-end")

    result = await scheduler.run([("a", actor("a")), ("b", actor("b"))])
    assert result.ok, result.error
    assert log == ["a-start", "b-start", "a-end", "b-end"]
    assert [step.format() for step in result.schedule.steps] == [
        "resume a @ mid #1",
        "resume b @ mid #1",
    ]


async def test_replay_mismatch_is_explicit() -> None:
    scheduler = CooperativeScheduler(timeout=1.0, idle_timeout=0.3)

    async def actor() -> None:
        await scheduler.checkpoint("only")

    result = await scheduler.run(
        [("a", actor())],
        schedule=Schedule.parse("resume a @ missing #1\n"),
    )
    assert not result.ok
    assert isinstance(result.error, ScheduleMismatch)


async def test_deadlock_without_progress_is_a_timeout_or_deadlock() -> None:
    scheduler = CooperativeScheduler(timeout=0.6, idle_timeout=0.15)

    async def stuck() -> None:
        await asyncio.Event().wait()

    result = await scheduler.run([("stuck", stuck())])
    assert result.timed_out or result.deadlock
    assert result.error is not None
    assert "stuck" in str(result.error) or isinstance(result.error, (TimeoutError, DeadlockError))


async def test_disabled_checkpoints_do_not_park() -> None:
    scheduler = CooperativeScheduler(
        enabled=frozenset({"keep"}),
        timeout=1.0,
        idle_timeout=0.3,
    )
    log: list[str] = []

    async def actor() -> None:
        await scheduler.checkpoint("skip")
        log.append("ran")
        await scheduler.checkpoint("keep")
        log.append("done")

    result = await scheduler.run([("a", actor())])
    assert result.ok, result.error
    assert log == ["ran", "done"]
    assert [step.checkpoint for step in result.schedule.steps] == ["keep"]
