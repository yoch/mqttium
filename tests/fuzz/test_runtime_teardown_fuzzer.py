"""Qualification of the V4 teardown profile of the runtime schedule fuzzer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.fuzz.runtime_fuzzer import RuntimeFuzzFailure
from tests.fuzz.runtime_teardown_fuzzer import (
    TeardownCampaignResult,
    TeardownFamily,
    TeardownMutation,
    assert_teardown_coverage,
    generate_teardown_schedule,
    run_teardown_campaign,
    run_teardown_schedule,
)
from tests.fuzz.runtime_teardown_fuzzer import main as teardown_main

_FAMILIES = tuple(TeardownFamily)


def _seeds_for(family: TeardownFamily, count: int) -> list[int]:
    return [_FAMILIES.index(family) + ordinal * len(_FAMILIES) for ordinal in range(count)]


def test_generator_is_deterministic_and_covers_every_family() -> None:
    schedules = [generate_teardown_schedule(seed) for seed in range(64)]
    assert schedules == [generate_teardown_schedule(seed) for seed in range(64)]
    assert all(len(schedule.operations) == 28 for schedule in schedules)
    assert {schedule.family for schedule in schedules} == set(TeardownFamily)
    assert {schedule.pending_publish for schedule in schedules} == {True, False}
    # Only the family without a reconnect policy runs without one.
    assert {s.family for s in schedules if not s.auto_reconnect} == {
        TeardownFamily.DISCONNECT_TERMINAL
    }
    with pytest.raises(ValueError):
        generate_teardown_schedule(0, steps=12)


async def test_healthy_campaign_reaches_every_teardown_surface() -> None:
    result = await run_teardown_campaign(seeds=range(48))
    assert result.failures == 0, result.failing_seeds
    assert_teardown_coverage(result)
    assert set(result.families) == {family.value for family in TeardownFamily}


def test_coverage_assertion_rejects_a_cold_surface() -> None:
    families = {family.value: 1 for family in TeardownFamily}
    assert_teardown_coverage(TeardownCampaignResult(4, 0, (), families, 4, 1))
    with pytest.raises(AssertionError, match="never scheduled"):
        cold = dict(families, external_cancel=0)
        assert_teardown_coverage(TeardownCampaignResult(3, 0, (), cold, 3, 1))
    with pytest.raises(AssertionError, match="release window"):
        assert_teardown_coverage(TeardownCampaignResult(4, 0, (), families, 3, 1))
    with pytest.raises(AssertionError, match="unacknowledged publication"):
        assert_teardown_coverage(TeardownCampaignResult(4, 0, (), families, 4, 0))


@pytest.mark.parametrize(
    ("mutation", "family", "oracle"),
    [
        (
            TeardownMutation.RELEASE_CANCEL_ENDS_READER,
            TeardownFamily.INTERNAL_FAILURE,
            "RECONNECTING with neither a reader nor a reconnect task",
        ),
        (
            TeardownMutation.RELEASE_CANCEL_ENDS_READER,
            TeardownFamily.EXTERNAL_CANCEL,
            "RECONNECTING with neither a reader nor a reconnect task",
        ),
        # Not paired with DISCONNECT_RECONNECTABLE: a terminal disconnect()
        # settles pending work itself when no reader is left to do it, so a
        # reader ended at its release loses nothing in that family.
        (
            TeardownMutation.INTERRUPTED_TEARDOWN_KEEPS_RECEIPTS,
            TeardownFamily.EXTERNAL_CANCEL,
            "publish receipt survived terminal teardown",
        ),
    ],
)
async def test_mutation_hits_its_specific_oracle(mutation, family, oracle) -> None:
    failures: list[str] = []
    for seed in _seeds_for(family, 12):
        schedule = generate_teardown_schedule(seed)
        try:
            await run_teardown_schedule(schedule, mutation=mutation, watchdog_seconds=2.0)
        except RuntimeFuzzFailure as failure:
            failures.append(failure.artifact.failure)
    assert failures, f"{mutation.value} survived every {family.value} schedule"
    assert all(oracle in failure for failure in failures), failures


async def test_mutations_are_inert_without_a_reconnectable_loss() -> None:
    for mutation in TeardownMutation:
        for seed in _seeds_for(TeardownFamily.DISCONNECT_TERMINAL, 6):
            await run_teardown_schedule(generate_teardown_schedule(seed), mutation=mutation)


async def test_failure_writes_a_replayable_artifact(tmp_path: Path) -> None:
    seed = _seeds_for(TeardownFamily.INTERNAL_FAILURE, 1)[0]
    with pytest.raises(RuntimeFuzzFailure):
        await run_teardown_schedule(
            generate_teardown_schedule(seed),
            mutation=TeardownMutation.RELEASE_CANCEL_ENDS_READER,
            artifacts_dir=tmp_path,
            watchdog_seconds=2.0,
        )
    artifact = json.loads((tmp_path / f"runtime-teardown-seed{seed}.json").read_text())
    assert artifact["mutation"] == "release_cancel_ends_reader"
    assert artifact["owners"]["teardown"]["family"] == "internal_failure"
    assert "reader.cancel_internal" in artifact["operations"]


def test_cli_runs_a_coverage_gated_campaign(tmp_path: Path, capsys) -> None:
    status = teardown_main(["--seed", "0", "--seeds", "16", "--artifacts-dir", str(tmp_path)])
    assert status == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["schema"] == "mqttium-runtime-teardown-fuzz-v1"
    assert summary["completed"] == 16 and summary["failures"] == 0
