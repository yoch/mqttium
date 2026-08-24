"""Bounded state-space evidence for the cooperative explorer."""

from __future__ import annotations

from tests.concurrency.explore import explore_dfs, explore_random
from tests.concurrency.scenarios import WRITE_BOUNDARIES, publish_admit


async def test_dfs_write_boundary_state_space_is_small_and_finite() -> None:
    stats = await explore_dfs(
        max_schedules=24,
        max_depth=3,
        enabled=WRITE_BOUNDARIES,
        actors=publish_admit,
        auto_ack=True,
    )
    assert stats.runs >= 1
    assert stats.unique_schedules >= 1
    assert stats.timeouts == 0, stats.format()
    assert stats.deadlocks == 0, stats.format()
    assert stats.errors == 0, stats.format()
    # One publisher and one write-boundary plus close/fail/ack/inbound actions
    # still collapse to a handful of distinct schedules at depth 3.
    assert stats.unique_schedules <= 24
    assert stats.max_steps <= 12


async def test_random_seed_is_repeatable() -> None:
    first = await explore_random(
        seed=7,
        max_schedules=6,
        enabled=WRITE_BOUNDARIES,
        actors=publish_admit,
        auto_ack=True,
    )
    second = await explore_random(
        seed=7,
        max_schedules=6,
        enabled=WRITE_BOUNDARIES,
        actors=publish_admit,
        auto_ack=True,
    )
    assert first.unique_schedules == second.unique_schedules
    assert first.ok == second.ok
    assert first.timeouts == 0
    assert second.timeouts == 0
