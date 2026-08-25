"""Behavioral proof for the V3 pressure/interleaving runtime schedule fuzzer.

The campaign gate is coverage-first (issue #388): a green run is trusted only
when every intended pressure surface was actually reached, and every surface
has a targeted behavioral mutation proving the oracles can see it break.
Issue #389 part B is qualified here: an application publisher is observed in
the parked state during execution, and every exit from that state settles the
publisher exactly once with a zero terminal waiter count.
"""

from __future__ import annotations

import json

import pytest

from tests.fuzz.runtime_pressure_fuzzer import (
    REQUIRED_PRESSURE_COVERAGE,
    PressureCampaignResult,
    PressureFamily,
    PressureMutation,
    RuntimeFuzzFailure,
    assert_pressure_coverage,
    generate_pressure_schedule,
    run_pressure_campaign,
    run_pressure_schedule,
)

_FAMILY_COUNT = len(PressureFamily)


def _seed_for(family: PressureFamily, ordinal: int = 0) -> int:
    families = tuple(PressureFamily)
    return families.index(family) + ordinal * _FAMILY_COUNT


def test_generator_is_deterministic_and_covers_every_family() -> None:
    schedules = [generate_pressure_schedule(seed, steps=36) for seed in range(64)]
    again = [generate_pressure_schedule(seed, steps=36) for seed in range(64)]

    assert schedules == again
    assert all(len(schedule.operations) == 36 for schedule in schedules)
    assert {schedule.family for schedule in schedules} == set(PressureFamily)
    # The settlement budget is generated, not fixed: the historical
    # unconditional four-turn convergence must not be the only value.
    settle_values = {value for schedule in schedules for value in schedule.settle_plan}
    assert {0, 1, 2, 4} <= settle_values
    # Both capability axes are composed present and absent.
    assert {schedule.profile.write_nowait for schedule in schedules} == {True, False}
    assert {schedule.profile.write_many for schedule in schedules} == {True, False}


async def test_pressure_schedule_is_reproducible() -> None:
    for family in (PressureFamily.BURST_LATENCY_BATCH, PressureFamily.PARKED_RELEASE):
        schedule = generate_pressure_schedule(_seed_for(family), steps=36)

        first = await run_pressure_schedule(schedule)
        second = await run_pressure_schedule(schedule)

        assert first.operations == second.operations
        assert first.final_snapshot == second.final_snapshot


async def test_campaign_reaches_every_required_pressure_surface() -> None:
    result = await run_pressure_campaign(seeds=range(32), steps=36, require_coverage=True)

    assert result.failures == 0
    assert result.completed == 32
    assert set(result.family_coverage) == {family.value for family in PressureFamily}
    for counter in REQUIRED_PRESSURE_COVERAGE:
        assert result.pressure_coverage[counter] > 0, counter


def test_coverage_assertion_rejects_a_cold_surface() -> None:
    counters = dict.fromkeys(REQUIRED_PRESSURE_COVERAGE, 1)
    counters["latency_batches"] = 0
    result = PressureCampaignResult(
        completed=1,
        failures=0,
        failing_seeds=(),
        wall_seconds=0.0,
        unique_operation_traces=1,
        unique_scheduling_traces=1,
        coverage={},
        family_coverage={},
        pressure_coverage=counters,
    )

    with pytest.raises(AssertionError, match="latency_batches"):
        assert_pressure_coverage(result)


async def test_eager_write_and_refusal_fallback_are_reached() -> None:
    run = await run_pressure_schedule(
        generate_pressure_schedule(_seed_for(PressureFamily.EAGER_PACED), steps=36)
    )

    counters = run.final_snapshot["pressure"]["counters"]
    assert counters["eager_accepted"] >= 2
    assert counters["eager_refused"] >= 1


async def test_burst_reaches_the_latency_batch_flush() -> None:
    run = await run_pressure_schedule(
        generate_pressure_schedule(_seed_for(PressureFamily.BURST_LATENCY_BATCH), steps=36)
    )

    assert run.final_snapshot["pressure"]["counters"]["latency_batches"] >= 1


async def test_burst_reaches_write_many_coalescing() -> None:
    run = await run_pressure_schedule(
        generate_pressure_schedule(_seed_for(PressureFamily.BURST_WRITE_MANY), steps=36)
    )

    assert run.final_snapshot["pressure"]["counters"]["write_many_calls"] >= 1


async def test_segmented_payload_takes_the_two_write_path() -> None:
    run = await run_pressure_schedule(
        generate_pressure_schedule(_seed_for(PressureFamily.SEGMENTED), steps=36)
    )

    assert run.final_snapshot["pressure"]["counters"]["segmented_writes"] >= 1


@pytest.mark.parametrize(
    "family",
    [
        PressureFamily.PARKED_RELEASE,
        PressureFamily.PARKED_CANCEL,
        PressureFamily.PARKED_TEARDOWN,
    ],
)
async def test_parked_publisher_is_observed_and_settles_exactly_once(
    family: PressureFamily,
) -> None:
    """Issue #389 part B: active parked state plus every exit from it.

    ``publish_waiters`` is sampled while the schedule executes, each parked
    publisher task settles exactly once (release, cancellation, or terminal
    failure), and the Part A terminal oracle proves the waiter count returned
    to zero.
    """
    run = await run_pressure_schedule(generate_pressure_schedule(_seed_for(family), steps=36))

    pressure = run.final_snapshot["pressure"]
    assert pressure["publish_waiters_high_water"] >= 1
    assert pressure["counters"]["parked_publisher_observed"] == 1
    assert run.final_snapshot["client"]["receipts"]["publish_waiters"] == 0
    tasks = [
        task
        for task in run.final_snapshot["application_tasks"]
        if task["label"].startswith("publish-")
    ]
    assert len(tasks) >= 3
    assert all(task["done"] for task in tasks)
    cancelled = [task for task in tasks if task["cancelled"]]
    assert bool(cancelled) == (family is PressureFamily.PARKED_CANCEL)


async def test_pressure_overlaps_a_lifecycle_ownership_window() -> None:
    run = await run_pressure_schedule(
        generate_pressure_schedule(_seed_for(PressureFamily.WRITER_PRESSURE_TEARDOWN), steps=36)
    )

    counters = run.final_snapshot["pressure"]["counters"]
    assert counters["pressure_lifecycle_overlaps"] == 1
    assert counters["writer_waiters_observed"] == 1
    assert run.final_snapshot["client"]["writer"]["queued_bytes"] == 0


@pytest.mark.parametrize(
    ("mutation", "minimum_detected"),
    [
        (PressureMutation.EAGER_ACCEPT_DROPS_FRAME, 8),
        (PressureMutation.EAGER_REFUSAL_LIES, 4),
        (PressureMutation.SEGMENTED_PAYLOAD_DROPPED, 2),
        (PressureMutation.PARKED_PUBLISHER_NOT_WOKEN, 4),
        (PressureMutation.PUBLISH_WAITER_DECREMENT_LOST, 6),
    ],
)
async def test_short_campaign_detects_known_pressure_bug_classes(
    mutation: PressureMutation,
    minimum_detected: int,
) -> None:
    result = await run_pressure_campaign(seeds=range(16), steps=36, mutation=mutation)

    assert result.failures >= minimum_detected
    assert result.completed == 16


async def test_missed_wakeup_and_leaked_waiter_hit_their_specific_oracles() -> None:
    parked_release = generate_pressure_schedule(_seed_for(PressureFamily.PARKED_RELEASE), steps=36)

    with pytest.raises(RuntimeFuzzFailure, match="PUBLISH was not reached"):
        await run_pressure_schedule(
            parked_release, mutation=PressureMutation.PARKED_PUBLISHER_NOT_WOKEN
        )
    with pytest.raises(RuntimeFuzzFailure, match="publish waiter survived terminal teardown"):
        await run_pressure_schedule(
            parked_release, mutation=PressureMutation.PUBLISH_WAITER_DECREMENT_LOST
        )


async def test_pressure_failure_writes_a_replayable_artifact(tmp_path) -> None:
    schedule = generate_pressure_schedule(_seed_for(PressureFamily.SEGMENTED), steps=36)

    with pytest.raises(RuntimeFuzzFailure) as caught:
        await run_pressure_schedule(
            schedule,
            mutation=PressureMutation.SEGMENTED_PAYLOAD_DROPPED,
            artifacts_dir=tmp_path,
        )

    artifact = caught.value.artifact
    saved = json.loads(
        (tmp_path / f"runtime-pressure-seed{schedule.seed}.json").read_text(encoding="utf-8")
    )
    assert saved["schema"] == "mqttium-runtime-fuzz-v3"
    assert saved["family"] == PressureFamily.SEGMENTED.value
    assert saved["mutation"] == "segmented_payload_dropped"
    assert saved["profile"] == {
        "write_nowait": schedule.profile.write_nowait,
        "write_many": schedule.profile.write_many,
    }
    assert saved["operations"] == artifact.operations
    assert "pressure" in saved["owners"]
    assert "seed=" in artifact.to_text()
