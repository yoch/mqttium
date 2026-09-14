from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.api.async_client import _fifo_register
from mqttium.api.models import PublishBatchReceipt, PublishReceipt
from mqttium.enums import QoS
from mqttium.protocol.engine import PublishFailure
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import accept_message


def test_effect_operations_are_bound_directly_to_the_pump() -> None:
    client = AsyncClient(client_id="effect-owner")

    assert client._effect_pump.collect_from_engine.__self__ is client._effect_pump
    assert client._effect_pump.drain_inline.__self__ is client._effect_pump
    assert client._effect_pump.schedule.__self__ is client._effect_pump
    assert client._effect_pump.drain.__self__ is client._effect_pump
    assert client._effect_pump.discard_connection_effects.__self__ is client._effect_pump


@pytest.mark.parametrize("qos", (QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE))
@pytest.mark.parametrize("failed", (False, True))
async def test_terminal_result_settles_receipts_without_callback_work(qos, failed) -> None:
    client = AsyncClient(client_id="terminal-receipts")
    receipt = PublishReceipt(mid=7, qos=qos)
    batch = PublishBatchReceipt()
    batch._register(7)
    batch._seal()
    _fifo_register(client._receipts, 7, receipt)
    _fifo_register(client._batch_receipts, 7, batch)
    error = RuntimeError("publish failed") if failed else None
    kind = EffectKind.PUBLISH_FAILED if failed else EffectKind.PUBLISH_COMPLETE
    client._engine._emit(kind, PublishFailure(7, error) if failed else 7)
    client._effect_pump.collect_from_engine()

    assert receipt.is_done() and batch.is_done()
    assert receipt._error is error
    assert not client._effect_pump.pending
    assert client._effect_pump.enqueued == 0
    assert client._delivery.callback_invocations == 0


async def test_reader_delivery_runs_sync_callback_outside_engine_lock() -> None:
    client = AsyncClient(message_delivery="callback")
    states = []
    client.on_message = lambda message: states.append(client._engine_lock.locked())
    async with client._engine_lock:
        client._engine._emit(EffectKind.MESSAGE, Message("topic", b"body"))
        client._effect_pump.collect_from_engine()
        assert not states
        assert not client._effect_pump.pending
    await client._delivery_lane.drain()
    assert states == [False]
    assert client._delivery.callback_invocations == 1


async def test_terminal_result_is_independent_of_pending_reader_delivery() -> None:
    client = AsyncClient(max_pending_messages=1)
    first = Message("first", b"one")
    second = Message("second", b"two")
    await accept_message(client._delivery, first, None)
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    _fifo_register(client._receipts, 7, receipt)
    client._engine._emit(EffectKind.MESSAGE, second)
    client._engine._emit(EffectKind.PUBLISH_COMPLETE, 7)
    client._effect_pump.collect_from_engine()
    await client._effect_pump.drain()
    assert receipt.is_done()
    assert client._delivery_lane.pending_count == 1
    stream = client.messages()
    assert await anext(stream) is first
    await client._delivery_lane.drain()
    assert await anext(stream) is second
    await stream.aclose()
    assert client._delivery.pending_bytes == 0


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

    client._effect_pump.pending.append(EngineEffect(EffectKind.PINGRESP, None))
    client._effect_pump.enqueued = 1
    client._effect_pump.schedule()
    cancellation_release.set()

    await asyncio.wait_for(applied.wait(), timeout=1.0)
    flush_task = client._effect_pump.task
    if flush_task is not None:
        await flush_task
    assert not client._effect_pump.pending
    assert client._effect_pump.applied == client._effect_pump.enqueued


async def test_single_send_effect_bypasses_queue_and_accounting() -> None:
    client = AsyncClient(client_id="effect-fast-path")
    client._engine._emit(EffectKind.SEND, b"payload")

    client._effect_pump.collect_from_engine()

    assert client._effect_pump.pending == deque()
    assert client._effect_pump.enqueued == 0
    assert client._effect_pump.applied == 0
    assert client._write_pump.queue.get_nowait() == b"payload"
    client._write_pump.queue.task_done()


async def test_discard_preserves_terminal_publish_order() -> None:
    client = AsyncClient(client_id="effect-discard")
    client._connection_epoch = 2
    complete = EngineEffect(EffectKind.PUBLISH_COMPLETE, 41)
    failed = EngineEffect(EffectKind.PUBLISH_FAILED, object())
    send = EngineEffect(EffectKind.SEND, b"stale")
    client._effect_pump.pending.extend((send, complete, failed))
    client._effect_pump.pending_epoch = 1
    client._effect_pump.enqueued = 3

    client._effect_pump.discard_connection_effects()

    assert list(client._effect_pump.pending) == [complete, failed]
    assert client._effect_pump.pending_epoch == 2
    assert client._effect_pump.applied == 1


def _collect(client: AsyncClient, kinds: list[EffectKind]) -> list[EffectKind]:
    """Emit one batch through the pump and report the order it queued."""
    for kind in kinds:
        client._engine._emit(kind, None)
    client._effect_pump.collect_from_engine()
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
