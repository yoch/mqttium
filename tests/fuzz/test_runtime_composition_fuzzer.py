"""Behavioral qualification for two-window runtime schedule composition."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.fuzz.runtime_composition_fuzzer import (
    CompositionMutation,
    CompositionPair,
    generate_composed_schedule,
    run_composed_schedule,
    run_composition_campaign,
)
from tests.fuzz.runtime_fuzzer import RuntimeFuzzFailure, generate_schedule


def test_v2_generation_is_deterministic_without_changing_v1() -> None:
    v1_before = generate_schedule(seed=17, steps=32)
    first = generate_composed_schedule(seed=17, steps=48)
    second = generate_composed_schedule(seed=17, steps=48)

    assert first == second
    assert len(first.operations) == 48
    assert max(first.window_depths) == 2
    assert set(first.window_depths) <= {0, 1, 2}
    assert generate_schedule(seed=17, steps=32) == v1_before


def test_six_seed_cycle_covers_the_initial_composition_pairs() -> None:
    schedules = [generate_composed_schedule(seed, steps=48) for seed in range(6)]

    assert {schedule.pair for schedule in schedules} == set(CompositionPair)
    assert all(schedule.release_order for schedule in schedules)


def test_same_pair_exercises_both_release_permutations() -> None:
    schedules = [generate_composed_schedule(seed, steps=48) for seed in range(0, 6 * 40, 6)]

    first_releases = {schedule.release_order[0] for schedule in schedules}
    assert first_releases == {"callback", "reconnect"}
    assert len({schedule.release_trace for schedule in schedules}) >= 8


def test_callback_reader_pair_covers_eof_and_real_keepalive_teardown() -> None:
    schedules = [
        generate_composed_schedule(seed, steps=48)
        for seed in range(300)
        if generate_composed_schedule(seed, steps=48).pair is CompositionPair.CALLBACK_READER
    ]
    operations = {operation.render() for schedule in schedules for operation in schedule.operations}

    assert "broker.inject_eof" in operations
    assert "keepalive.timeout_due" in operations


@pytest.mark.parametrize("seed", range(6))
async def test_each_composition_pair_runs_healthy(seed: int) -> None:
    schedule = generate_composed_schedule(seed, steps=48)

    result = await run_composed_schedule(schedule)

    assert result.failure is None


@pytest.mark.parametrize(
    ("mutation", "pair"),
    [
        (CompositionMutation.OLD_WRITER_SURVIVES_RECONNECT, CompositionPair.WRITER_RECONNECT),
        (
            CompositionMutation.CALLBACK_TAKEOVER_LOSES,
            CompositionPair.CALLBACK_RECONNECT,
        ),
        (CompositionMutation.EFFECT_CROSSES_GENERATION, CompositionPair.EFFECT_RECONNECT),
    ],
)
async def test_behavioral_mutants_require_their_composed_pair(
    mutation: CompositionMutation,
    pair: CompositionPair,
) -> None:
    matching = next(
        generate_composed_schedule(seed, steps=48)
        for seed in range(300)
        if generate_composed_schedule(seed, steps=48).pair is pair
        and generate_composed_schedule(seed, steps=48).mutation_window
    )

    with pytest.raises(RuntimeFuzzFailure):
        await run_composed_schedule(matching, mutation=mutation)

    nonmatching = next(
        generate_composed_schedule(seed, steps=48)
        for seed in range(300)
        if generate_composed_schedule(seed, steps=48).pair
        not in {
            pair,
            CompositionPair.CALLBACK_READER
            if mutation is CompositionMutation.CALLBACK_TAKEOVER_LOSES
            else pair,
        }
    )
    result = await run_composed_schedule(nonmatching, mutation=mutation)
    assert result.failure is None


async def test_keepalive_takeover_regression_mutant_requires_callback_close_overlap() -> None:
    schedule = next(
        generate_composed_schedule(seed, steps=48)
        for seed in range(600)
        if generate_composed_schedule(seed, steps=48).pair is CompositionPair.CALLBACK_READER
        and generate_composed_schedule(seed, steps=48).release_order[0] == "callback"
        and any(
            operation.render() == "keepalive.timeout_due"
            for operation in generate_composed_schedule(seed, steps=48).operations
        )
    )

    with pytest.raises(RuntimeFuzzFailure):
        await run_composed_schedule(
            schedule,
            mutation=CompositionMutation.CLOSING_TRANSPORT_TAKEOVER_LOSES,
        )


async def test_campaign_reports_pair_depth_and_release_diversity() -> None:
    result = await run_composition_campaign(seeds=range(24), steps=48)

    assert result.completed == 24
    assert result.failures == 0
    assert result.total_operations == 24 * 48
    assert result.wall_seconds > 0
    assert result.schedules_per_second > 0
    assert set(result.pair_coverage) == {pair.value for pair in CompositionPair}
    assert result.window_depth_counts[2] > 0
    assert result.unique_release_traces >= 12


async def test_failure_artifact_keeps_composition_and_v1_owner_evidence(
    tmp_path: Path,
) -> None:
    schedule = next(
        generate_composed_schedule(seed, steps=48)
        for seed in range(300)
        if generate_composed_schedule(seed, steps=48).pair is CompositionPair.WRITER_RECONNECT
        and generate_composed_schedule(seed, steps=48).mutation_window
    )

    with pytest.raises(RuntimeFuzzFailure):
        await run_composed_schedule(
            schedule,
            mutation=CompositionMutation.OLD_WRITER_SURVIVES_RECONNECT,
            artifacts_dir=tmp_path,
        )

    artifact = json.loads(next(tmp_path.glob("*.json")).read_text(encoding="utf-8"))
    assert artifact["schema"] == "mqttium-runtime-fuzz-v2"
    assert artifact["pair"] == CompositionPair.WRITER_RECONNECT.value
    assert artifact["release_trace"] == list(schedule.release_trace)
    assert 2 in artifact["window_depths"]
    assert artifact["owners"]["composition"]["pair"] == schedule.pair.value
    assert artifact["owners"]["transports"][0]["attempted_packets"]
    assert artifact["owners"]["transports"][0]["completed_epochs"]
