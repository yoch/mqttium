"""Behavioral proof for the deterministic AsyncClient schedule fuzzer."""

from __future__ import annotations

import json

import pytest

from tests.fuzz.runtime_fuzzer import (
    RuntimeFuzzFailure,
    RuntimeMutation,
    generate_schedule,
    run_campaign,
    run_schedule,
)


async def test_generated_runtime_schedule_is_reproducible_and_healthy() -> None:
    schedule = generate_schedule(seed=4, steps=24)

    first = await run_schedule(schedule)
    second = await run_schedule(schedule)

    assert first.failure is None
    assert second.failure is None
    assert first.operations == second.operations
    assert first.final_snapshot == second.final_snapshot


async def test_runtime_failure_writes_a_replayable_human_readable_artifact(tmp_path) -> None:
    schedule = generate_schedule(seed=3, steps=24)

    with pytest.raises(RuntimeFuzzFailure) as caught:
        await run_schedule(
            schedule,
            mutation=RuntimeMutation.WRITER_FAILURE_NO_WAKE,
            artifacts_dir=tmp_path,
        )

    artifact = caught.value.artifact
    artifact_path = tmp_path / "runtime-seed3.json"
    assert artifact_path.exists()
    saved = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert saved["schema"] == "mqttium-runtime-fuzz-v1"
    assert saved["seed"] == 3
    assert saved["mutation"] == "writer_failure_no_wake"
    assert saved["operations"] == artifact.operations
    assert "writer" in saved["owners"]
    assert "waiter" in saved["failure"]
    assert "seed=3" in artifact.to_text()
    assert "operations:" in artifact.to_text()


@pytest.mark.parametrize(
    ("mutation", "minimum_detected"),
    [
        (RuntimeMutation.WRITER_FAILURE_NO_WAKE, 3),
        (RuntimeMutation.EPOCH_NOT_INVALIDATED, 9),
        (RuntimeMutation.EFFECT_NOT_SETTLED, 3),
        (RuntimeMutation.CALLBACK_CANCEL_STOPS_WORKER, 3),
    ],
)
async def test_short_campaign_detects_known_runtime_bug_classes(
    mutation: RuntimeMutation,
    minimum_detected: int,
) -> None:
    result = await run_campaign(
        seeds=range(12),
        steps=24,
        mutation=mutation,
    )

    assert result.failures >= minimum_detected
    assert result.completed == 12
