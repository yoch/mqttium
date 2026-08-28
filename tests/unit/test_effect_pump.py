from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.api.models import PublishBatchReceipt, PublishReceipt
from mqttium.enums import ConnectionState, QoS
from mqttium.protocol.engine import PublishFailure
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def test_effect_operations_are_bound_directly_to_the_pump() -> None:
    client = AsyncClient(client_id="effect-owner")

    assert client._collect_effects_locked.__self__ is client._effect_pump
    assert client._drain_effects_inline.__self__ is client._effect_pump
    assert client._schedule_effect_flush.__self__ is client._effect_pump
    assert client._drain_effects.__self__ is client._effect_pump
    assert client._discard_connection_effects.__self__ is client._effect_pump


def test_effect_diagnostic_attributes_are_views() -> None:
    client = AsyncClient(client_id="effect-views")

    assert client._pending_effects is client._effect_pump.pending
    assert client._pending_effect_epoch is client._effect_pump.pending_epoch
    assert client._effect_enqueued == client._effect_pump.enqueued
    assert client._effect_applied == client._effect_pump.applied
    assert client._effect_flush_task is client._effect_pump.task


@pytest.mark.parametrize("qos", (QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE))
async def test_qosn_completion_with_idle_sync_callback_runs_inline(qos: QoS) -> None:
    client = AsyncClient(client_id=f"effect-qos{int(qos)}-callback")
    receipt = PublishReceipt(mid=7, qos=qos)
    batch = PublishBatchReceipt()
    batch._register(7)
    batch._seal()
    client._register_publish_receipt(7, receipt)
    client._register_batch_receipt(7, batch)
    seen: list[tuple[int | None, BaseException | None, bool, bool]] = []

    def on_publish(mid: int | None, reason: BaseException | None) -> None:
        seen.append((mid, reason, receipt.is_done(), batch.is_done()))

    client.on_publish = on_publish
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 7)
    client._collect_effects_locked()

    assert receipt.is_done()
    assert batch.is_done()
    assert not client._pending_effects
    assert client._effect_pump.enqueued == 0
    assert seen == [(7, None, True, True)]
    assert client._callback_worker_task is None


async def test_inline_completion_keeps_callback_exceptions_isolated() -> None:
    client = AsyncClient(client_id="effect-callback-error")
    receipt = PublishReceipt(mid=8, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(8, receipt)
    reported: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(context))

    def on_publish(_mid: int | None, _reason: BaseException | None) -> None:
        raise RuntimeError("callback failed")

    try:
        client.on_publish = on_publish
        client._engine._emit(EffectKind.PUBLISH_COMPLETE, 8)
        client._collect_effects_locked()
        await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    finally:
        loop.set_exception_handler(previous_handler)
        await client._shutdown_callback_worker(drain=False)

    assert receipt.is_done()
    assert len(reported) == 1
    assert reported[0]["message"] == "mqttium user callback failed"
    assert isinstance(reported[0]["exception"], RuntimeError)
    assert client._effect_pump.error is None


async def test_sync_callback_never_runs_under_engine_lock() -> None:
    client = AsyncClient(client_id="effect-lock-boundary")
    seen_lock_states: list[bool] = []
    client.on_publish = lambda _mid, _reason: seen_lock_states.append(client._engine_lock.locked())

    async with client._engine_lock:
        client._engine._emit(EffectKind.PUBLISH_COMPLETE, 31)
        client._collect_effects_locked()
        assert seen_lock_states == []

    client._drain_effects_inline()
    assert seen_lock_states == [False]


async def test_idle_sync_publish_failure_callback_runs_inline() -> None:
    client = AsyncClient(client_id="effect-failure-callback")
    receipt = PublishReceipt(mid=9, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(9, receipt)
    failure = RuntimeError("publish failed")
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, reason: seen.append((mid, reason))
    client._engine._emit(EffectKind.PUBLISH_FAILED, PublishFailure(9, failure))

    client._collect_effects_locked()

    assert receipt.is_done()
    assert receipt._error is failure
    assert not client._pending_effects
    assert client._effect_pump.enqueued == 0
    assert seen == [(9, failure)]
    assert client._callback_worker_task is None


async def test_async_publish_callback_stays_on_bounded_worker() -> None:
    client = AsyncClient(client_id="effect-async-callback")
    receipt = PublishReceipt(mid=10, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(10, receipt)
    seen: list[int | None] = []

    async def on_publish(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(mid)

    client.on_publish = on_publish
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 10)
    client._collect_effects_locked()

    assert receipt.is_done()
    assert seen == []
    assert client._callback_queue.qsize() == 1
    await client._callback_queue.join()
    assert seen == [10]
    await client._shutdown_callback_worker(drain=False)


async def test_idle_sync_message_callback_runs_inline_after_engine_lock() -> None:
    client = AsyncClient(client_id="effect-inline-message", message_delivery="callback")
    seen: list[tuple[str, bool]] = []
    client.on_message = lambda message: seen.append((message.topic, client._engine_lock.locked()))

    async with client._engine_lock:
        client._engine._emit(
            EffectKind.MESSAGE,
            Message(topic="inline/message", payload=b"x"),
        )
        client._collect_effects_locked()
        assert seen == []

    client._drain_effects_inline()
    assert seen == [("inline/message", False)]
    assert client._callback_worker_task is None


async def test_full_callback_queue_retains_async_completion_backpressure() -> None:
    client = AsyncClient(
        client_id="effect-callback-full",
        max_pending_callbacks=1,
        delivery_timeout=1.0,
    )
    started = asyncio.Event()
    release = asyncio.Event()
    seen: list[str | int] = []

    async def blocker(value: str) -> None:
        started.set()
        await release.wait()
        seen.append(value)

    await client._enqueue_callback(blocker, "running")
    await started.wait()
    await client._enqueue_callback(lambda value: seen.append(value), "queued")

    receipt = PublishReceipt(mid=11, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(11, receipt)
    client.on_publish = lambda mid, _reason: seen.append(mid if mid is not None else -1)
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 11)
    client._collect_effects_locked()

    assert not receipt.is_done()
    assert [effect.kind for effect in client._pending_effects] == [EffectKind.PUBLISH_COMPLETE]
    assert client._effect_pump.enqueued == 1

    client._drain_effects_inline()
    await asyncio.sleep(0)
    assert receipt.is_done(), "the slow path settles before waiting for callback capacity"
    assert client._effect_pump.apply_suspensions == 0

    release.set()
    await client._drain_effects()
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert seen == ["running", "queued", 11]
    assert not client._pending_effects
    assert client._effect_pump.enqueued == client._effect_pump.applied
    await client._shutdown_callback_worker(drain=False)


async def test_inline_publish_callback_reentrancy_falls_back_to_bounded_worker() -> None:
    client = AsyncClient(client_id="effect-reentrant", max_pending_callbacks=2)
    client._engine.state = ConnectionState.CONNECTED
    receipt = PublishReceipt(mid=17, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(17, receipt)
    seen: list[int | None] = []

    def on_publish(mid: int | None, _reason: BaseException | None) -> None:
        seen.append(mid)
        if mid == 17:
            client.publish_nowait("reentrant/qos0", b"x", qos=0)

    client.on_publish = on_publish
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 17)
    client._collect_effects_locked()

    assert receipt.is_done()
    assert seen == [17]
    assert client._callback_queue.qsize() == 1
    await client._callback_queue.join()
    assert seen == [17, None]
    await client._shutdown_callback_worker(drain=False)


async def test_cancelled_flush_honours_a_deferred_reschedule() -> None:
    client = AsyncClient(client_id="effect-cancelled-reschedule")
    cancellation_started = asyncio.Event()
    cancellation_release = asyncio.Event()
    applied = asyncio.Event()

    async def old_flush() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_started.set()
            await cancellation_release.wait()
            raise

    async def apply(effect, *, nowait: bool, epoch: int | None = None) -> None:
        applied.set()

    client._apply_effect = apply  # type: ignore[method-assign]
    old_task = asyncio.create_task(old_flush())
    client._effect_pump.task = old_task
    old_task.add_done_callback(client._effect_pump._done)
    await asyncio.sleep(0)
    old_task.cancel()
    await cancellation_started.wait()

    client._pending_effects.append(EngineEffect(EffectKind.PINGRESP, None))
    client._effect_pump.enqueued = 1
    client._schedule_effect_flush()
    cancellation_release.set()

    await asyncio.wait_for(applied.wait(), timeout=1.0)
    flush_task = client._effect_flush_task
    if flush_task is not None:
        await flush_task
    assert not client._pending_effects
    assert client._effect_pump.applied == client._effect_pump.enqueued


async def test_single_send_effect_bypasses_queue_and_accounting() -> None:
    client = AsyncClient(client_id="effect-fast-path")
    client._engine._emit(EffectKind.SEND, b"payload")

    client._collect_effects_locked()

    assert client._pending_effects == deque()
    assert client._effect_enqueued == 0
    assert client._effect_applied == 0
    assert client._outbound.get_nowait() == b"payload"
    client._outbound.task_done()


async def test_discard_preserves_terminal_publish_order() -> None:
    client = AsyncClient(client_id="effect-discard")
    client._connection_epoch = 2
    complete = EngineEffect(EffectKind.PUBLISH_COMPLETE, 41)
    failed = EngineEffect(EffectKind.PUBLISH_FAILED, object())
    send = EngineEffect(EffectKind.SEND, b"stale")
    client._pending_effects.extend((send, complete, failed))
    client._effect_pump.pending_epoch = 1
    client._effect_pump.enqueued = 3

    client._discard_connection_effects()

    assert list(client._pending_effects) == [complete, failed]
    assert client._pending_effect_epoch == 2
    assert client._effect_applied == 1


def _collect(client: AsyncClient, kinds: list[EffectKind]) -> list[EffectKind]:
    """Emit one batch through the pump and report the order it queued."""
    for kind in kinds:
        client._engine._emit(kind, None)
    client._collect_effects_locked()
    return [effect.kind for effect in client._effect_pump.pending]


def test_ordered_batch_is_queued_untouched() -> None:
    client = AsyncClient(client_id="effect-ordered")
    kinds = [EffectKind.SEND, EffectKind.SEND, EffectKind.PUBLISH_COMPLETE]

    assert _collect(client, kinds) == kinds
    assert client._effect_pump.multi_effect_batches == 1
    assert client._effect_pump.reordered_batches == 0


def test_completion_before_send_is_reordered_send_first() -> None:
    """The shape every pipelined PUBACK batch produces."""
    client = AsyncClient(client_id="effect-puback")

    order = _collect(client, [EffectKind.PUBLISH_COMPLETE, EffectKind.SEND])

    assert order == [EffectKind.SEND, EffectKind.PUBLISH_COMPLETE]
    assert client._effect_pump.reordered_batches == 1


def test_interleaved_batch_keeps_relative_order_within_each_group() -> None:
    client = AsyncClient(client_id="effect-interleaved")

    order = _collect(
        client,
        [
            EffectKind.SEND,
            EffectKind.PUBLISH_COMPLETE,
            EffectKind.SEND,
            EffectKind.SUBACK,
        ],
    )

    assert order == [
        EffectKind.SEND,
        EffectKind.SEND,
        EffectKind.PUBLISH_COMPLETE,
        EffectKind.SUBACK,
    ]
    assert client._effect_pump.reordered_batches == 1


def test_all_send_batch_is_not_counted_as_reordered() -> None:
    client = AsyncClient(client_id="effect-sends")

    order = _collect(client, [EffectKind.SEND] * 4)

    assert order == [EffectKind.SEND] * 4
    assert client._effect_pump.multi_effect_batches == 1
    assert client._effect_pump.reordered_batches == 0


def test_no_send_batch_is_not_counted_as_reordered() -> None:
    client = AsyncClient(client_id="effect-others")
    kinds = [EffectKind.PUBLISH_COMPLETE, EffectKind.SUBACK, EffectKind.UNSUBACK]

    assert _collect(client, kinds) == kinds
    assert client._effect_pump.reordered_batches == 0


def test_paired_benchmark_scenarios_exercise_the_branch_they_name() -> None:
    """A benchmark arm that measures the wrong branch is worse than none.

    The pump reorders as soon as a SEND follows a non-SEND, so an interleaved
    batch is reordered too. This pins each named arm to the branch it claims.
    """
    ordered = [EffectKind.SEND] * 4 + [EffectKind.PUBLISH_COMPLETE] * 4
    reordered = [EffectKind.PUBLISH_COMPLETE, EffectKind.SEND] * 4

    client = AsyncClient(client_id="effect-scenario-ordered")
    assert _collect(client, ordered) == ordered
    assert client._effect_pump.multi_effect_batches == 1
    assert client._effect_pump.reordered_batches == 0, "the ordered arm must not reorder"

    other = AsyncClient(client_id="effect-scenario-reordered")
    _collect(other, reordered)
    assert other._effect_pump.reordered_batches == 1, "the reordered arm must reorder"
