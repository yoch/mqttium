"""Behavioral proof for the deterministic AsyncClient schedule fuzzer."""

from __future__ import annotations

import json

import pytest

from tests.fuzz import runtime_fuzzer
from tests.fuzz.runtime_fuzzer import (
    RuntimeFuzzFailure,
    RuntimeMutation,
    RuntimeOperation,
    RuntimeSchedule,
    generate_schedule,
    run_campaign,
    run_schedule,
)


def test_v1_cli_accepts_shared_runner_timeouts(tmp_path) -> None:
    exit_status = runtime_fuzzer.main(
        [
            "--seed",
            "0",
            "--seeds",
            "1",
            "--steps",
            "24",
            "--watchdog-seconds",
            "30",
            "--connect-timeout-seconds",
            "10",
            "--artifacts-dir",
            str(tmp_path),
        ]
    )

    assert exit_status == 0


async def test_unexpected_application_task_exception_fails_the_schedule() -> None:
    schedule = RuntimeSchedule(
        seed=90,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("app", "connect"),
        ),
    )

    with pytest.raises(RuntimeFuzzFailure, match="unexpected application task exception"):
        await run_schedule(schedule)


async def test_unexpected_loop_exception_context_fails_the_schedule() -> None:
    schedule = RuntimeSchedule(
        seed=91,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("callback", "raise_once"),
            RuntimeOperation("broker", "publish", 0),
            RuntimeOperation("checkpoint", "callbacks_drained"),
        ),
    )

    with pytest.raises(RuntimeFuzzFailure, match="unexpected event-loop exception"):
        await run_schedule(schedule)


async def test_failed_transport_write_is_attempted_but_not_completed() -> None:
    result = await run_schedule(generate_schedule(seed=0, steps=24))

    transport = result.final_snapshot["transports"][0]
    attempted = transport["attempted_packets"]
    completed = transport["completed_packets"]
    assert attempted.count("PUBLISH") == completed.count("PUBLISH") + 1


async def test_duplicate_completed_publish_fails_wire_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_write = runtime_fuzzer._ScheduleTransport.write

    async def duplicate_completed_publish(transport: object, data: object) -> None:
        completed_before = len(transport.completed)  # type: ignore[attr-defined]
        await original_write(transport, data)  # type: ignore[arg-type]
        newly_completed = transport.completed[completed_before:]  # type: ignore[attr-defined]
        transport.completed.extend(  # type: ignore[attr-defined]
            item
            for item in newly_completed
            if item[0].packet_type is runtime_fuzzer.PacketType.PUBLISH
        )

    monkeypatch.setattr(
        runtime_fuzzer._ScheduleTransport,
        "write",
        duplicate_completed_publish,
    )
    schedule = RuntimeSchedule(
        seed=95,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("app", "publish", 0),
            RuntimeOperation("checkpoint", "wire", "PUBLISH"),
            RuntimeOperation("app", "disconnect"),
            RuntimeOperation("checkpoint", "terminal"),
        ),
    )

    with pytest.raises(RuntimeFuzzFailure, match="wire multiplicity"):
        await run_schedule(schedule)


async def test_late_duplicate_completed_publish_fails_terminal_wire_obligation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_write = runtime_fuzzer._ScheduleTransport.write

    async def duplicate_publish_on_disconnect(transport: object, data: object) -> None:
        completed_before = len(transport.completed)  # type: ignore[attr-defined]
        await original_write(transport, data)  # type: ignore[arg-type]
        newly_completed = transport.completed[completed_before:]  # type: ignore[attr-defined]
        if any(
            item[0].packet_type is runtime_fuzzer.PacketType.DISCONNECT for item in newly_completed
        ):
            publish = next(
                item
                for item in reversed(transport.completed[:completed_before])  # type: ignore[attr-defined]
                if item[0].packet_type is runtime_fuzzer.PacketType.PUBLISH
            )
            transport.completed.append(publish)  # type: ignore[attr-defined]

    monkeypatch.setattr(
        runtime_fuzzer._ScheduleTransport,
        "write",
        duplicate_publish_on_disconnect,
    )
    schedule = RuntimeSchedule(
        seed=96,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("app", "publish", 0),
            RuntimeOperation("checkpoint", "wire", "PUBLISH"),
            RuntimeOperation("app", "disconnect"),
            RuntimeOperation("checkpoint", "terminal"),
        ),
    )

    with pytest.raises(RuntimeFuzzFailure, match="wire multiplicity"):
        await run_schedule(schedule)


async def test_terminal_oracle_rejects_publish_waiter_accounting_leak() -> None:
    schedule = RuntimeSchedule(
        seed=97,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("app", "disconnect"),
            RuntimeOperation("checkpoint", "terminal"),
        ),
    )

    with pytest.raises(RuntimeFuzzFailure, match="publish waiter survived terminal teardown"):
        await run_schedule(
            schedule,
            mutation=RuntimeMutation.PUBLISH_WAITER_ACCOUNTING_LEAK,
        )


async def test_whole_schedule_watchdog_reports_deadlock_and_allows_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_close = runtime_fuzzer._ScheduleTransport.close
    close_calls = 0

    async def block_first_close(transport: object) -> None:
        nonlocal close_calls
        close_calls += 1
        if close_calls == 1:
            await runtime_fuzzer.asyncio.Event().wait()
        await original_close(transport)  # type: ignore[arg-type]

    monkeypatch.setattr(runtime_fuzzer._ScheduleTransport, "close", block_first_close)
    schedule = RuntimeSchedule(
        seed=92,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("broker", "eof"),
        ),
    )

    with pytest.raises(RuntimeFuzzFailure, match="whole-schedule liveness watchdog"):
        await run_schedule(schedule, watchdog_seconds=0.05)
    assert close_calls >= 2


async def test_effect_failure_window_settles_late_collected_effect() -> None:
    schedule = RuntimeSchedule(
        seed=93,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("schedule", "hold_writes"),
            RuntimeOperation("app", "publish", 0),
            RuntimeOperation("checkpoint", "writer_active"),
            RuntimeOperation("effect", "block_next"),
            RuntimeOperation("schedule", "hold_close"),
            RuntimeOperation("broker", "publish", 1),
            RuntimeOperation("checkpoint", "effect_active"),
            RuntimeOperation("effect", "drain_failure"),
            RuntimeOperation("effect", "drain_failure"),
            RuntimeOperation("effect", "fail_on_release"),
            RuntimeOperation("schedule", "release_effect"),
            RuntimeOperation("checkpoint", "effect_failing_close"),
            RuntimeOperation("effect", "collect_late"),
            RuntimeOperation("schedule", "release_close"),
            RuntimeOperation("checkpoint", "terminal"),
        ),
    )

    result = await run_schedule(schedule)

    assert result.final_snapshot["effects"]["failing_close"] is False
    assert result.final_snapshot["client"]["effects"]["pending"] == 0


async def test_callback_connect_takes_over_automatic_reconnect() -> None:
    schedule = RuntimeSchedule(
        seed=94,
        auto_reconnect=True,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("callback", "on_disconnect_connect_once"),
            RuntimeOperation("broker", "eof"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("checkpoint", "takeover_stable"),
            RuntimeOperation("app", "disconnect"),
            RuntimeOperation("checkpoint", "terminal"),
        ),
    )

    result = await run_schedule(schedule)

    assert len(result.final_snapshot["transports"]) == 2


@pytest.mark.parametrize(
    ("mutation", "seed"),
    [
        (RuntimeMutation.LATE_EFFECT_ABANDONED, 93),
        (RuntimeMutation.USER_TAKEOVER_LOSES, 94),
    ],
)
async def test_interleaving_mutants_require_their_behavioral_windows(
    mutation: RuntimeMutation,
    seed: int,
) -> None:
    schedule = (
        RuntimeSchedule(
            seed=93,
            operations=(
                RuntimeOperation("app", "connect"),
                RuntimeOperation("checkpoint", "wire", "CONNECT"),
                RuntimeOperation("broker", "connack"),
                RuntimeOperation("checkpoint", "connected"),
                RuntimeOperation("schedule", "hold_writes"),
                RuntimeOperation("app", "publish", 0),
                RuntimeOperation("checkpoint", "writer_active"),
                RuntimeOperation("effect", "block_next"),
                RuntimeOperation("schedule", "hold_close"),
                RuntimeOperation("broker", "publish", 1),
                RuntimeOperation("checkpoint", "effect_active"),
                RuntimeOperation("effect", "drain_failure"),
                RuntimeOperation("effect", "fail_on_release"),
                RuntimeOperation("schedule", "release_effect"),
                RuntimeOperation("checkpoint", "effect_failing_close"),
                RuntimeOperation("effect", "collect_late"),
                RuntimeOperation("schedule", "release_close"),
                RuntimeOperation("checkpoint", "terminal"),
            ),
        )
        if seed == 93
        else RuntimeSchedule(
            seed=94,
            auto_reconnect=True,
            operations=(
                RuntimeOperation("app", "connect"),
                RuntimeOperation("checkpoint", "wire", "CONNECT"),
                RuntimeOperation("broker", "connack"),
                RuntimeOperation("checkpoint", "connected"),
                RuntimeOperation("factory", "block_next"),
                RuntimeOperation("broker", "eof"),
                RuntimeOperation("checkpoint", "factory_blocked"),
                RuntimeOperation("app", "connect"),
                RuntimeOperation("schedule", "release_factory"),
                RuntimeOperation("checkpoint", "wire", "CONNECT"),
                RuntimeOperation("broker", "connack"),
                RuntimeOperation("checkpoint", "wire", "CONNECT"),
                RuntimeOperation("broker", "connack"),
                RuntimeOperation("checkpoint", "connected"),
            ),
        )
    )

    with pytest.raises(RuntimeFuzzFailure):
        await run_schedule(schedule, mutation=mutation)


async def test_generated_runtime_schedule_is_reproducible_and_healthy() -> None:
    schedule = generate_schedule(seed=4, steps=24)

    first = await run_schedule(schedule)
    second = await run_schedule(schedule)

    assert first.failure is None
    assert second.failure is None
    assert first.operations == second.operations
    assert first.final_snapshot == second.final_snapshot


def test_generator_uses_step_budget_and_produces_diverse_schedule_traces() -> None:
    schedules = [generate_schedule(seed, steps=32) for seed in range(1_000)]
    operation_traces = {
        tuple(operation.render() for operation in schedule.operations) for schedule in schedules
    }
    scheduling_traces = {
        tuple(
            operation.render()
            for operation in schedule.operations
            if operation.actor in {"checkpoint", "schedule", "factory", "effect"}
        )
        for schedule in schedules
    }

    assert all(len(schedule.operations) == 32 for schedule in schedules)
    assert len(operation_traces) >= 900
    assert len(scheduling_traces) >= 850


async def test_campaign_reports_trace_diversity_and_operation_coverage() -> None:
    result = await run_campaign(seeds=range(48), steps=24)

    assert result.failures == 0
    assert result.unique_operation_traces >= 22
    assert result.unique_scheduling_traces >= 22
    for operation in (
        "factory.block_next",
        "callback.block_once",
        "effect.block_next",
        "schedule.release_writes",
        "checkpoint.terminal",
    ):
        assert result.coverage[operation] > 0


async def test_runtime_failure_writes_a_replayable_human_readable_artifact(tmp_path) -> None:
    schedule = generate_schedule(seed=0, steps=24)

    with pytest.raises(RuntimeFuzzFailure) as caught:
        await run_schedule(
            schedule,
            mutation=RuntimeMutation.WRITER_FAILURE_NO_WAKE,
            artifacts_dir=tmp_path,
        )

    artifact = caught.value.artifact
    artifact_path = tmp_path / "runtime-seed0.json"
    assert artifact_path.exists()
    saved = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert saved["schema"] == "mqttium-runtime-fuzz-v1"
    assert saved["seed"] == 0
    assert saved["mutation"] == "writer_failure_no_wake"
    assert saved["operations"] == artifact.operations
    assert saved["timing"] == {
        "connect_timeout_seconds": 0.5,
        "watchdog_seconds": 2.0,
    }
    assert "writer" in saved["owners"]
    assert "waiter" in saved["failure"]
    assert "seed=0" in artifact.to_text()
    assert "operations:" in artifact.to_text()
    assert "timing=" in artifact.to_text()


@pytest.mark.parametrize(
    ("mutation", "minimum_detected"),
    [
        (RuntimeMutation.WRITER_FAILURE_NO_WAKE, 4),
        (RuntimeMutation.EPOCH_NOT_INVALIDATED, 18),
        (RuntimeMutation.EFFECT_NOT_SETTLED, 18),
        (RuntimeMutation.CALLBACK_CANCEL_STOPS_WORKER, 4),
        (RuntimeMutation.LATE_EFFECT_ABANDONED, 4),
        (RuntimeMutation.USER_TAKEOVER_LOSES, 1),
    ],
)
async def test_short_campaign_detects_known_runtime_bug_classes(
    mutation: RuntimeMutation,
    minimum_detected: int,
) -> None:
    result = await run_campaign(
        seeds=range(24),
        steps=24,
        mutation=mutation,
    )

    assert result.failures >= minimum_detected
    assert result.completed == 24
