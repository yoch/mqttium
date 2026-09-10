"""Atomic effect transfer and bounded callback lifecycle."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.protocol.engine import EffectKind
from mqttium.types import Message


async def test_cancelled_backpressure_keeps_send_effect_for_same_connection() -> None:
    client = AsyncClient(
        client_id="effect-cancel",
        max_outbound_messages=1,
        max_outbound_bytes=1,
    )
    client._engine.state = ConnectionState.CONNECTED
    await client._write_pump.enqueue(b"x")

    publishing = asyncio.create_task(client.publish("effect/t", b"payload", qos=0))
    for _ in range(100):
        if client._write_pump.waiters:
            break
        await asyncio.sleep(0)
    assert client._write_pump.waiters == 1

    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    assert any(effect.kind is EffectKind.SEND for effect in client._effect_pump.pending)

    blocked = client._write_pump.queue.get_nowait()
    client._write_pump.queue.task_done()
    client._write_pump.queued_bytes -= len(blocked)
    async with client._write_pump.space:
        client._write_pump.space.notify_all()

    await client._effect_pump.drain()
    assert not client._effect_pump.pending
    assert client._write_pump.queue.qsize() == 1


async def test_callback_exception_reaches_loop_exception_handler() -> None:
    client = AsyncClient(client_id="callback-error")
    loop = asyncio.get_running_loop()
    contexts: list[dict[str, object]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))

    def fail(_message: Message) -> None:
        raise RuntimeError("callback failed")

    try:
        await client._delivery.enqueue_callback(fail, Message(topic="t", payload=b"x"))
        await asyncio.wait_for(client._delivery.callback_queue.join(), timeout=1.0)
        assert len(contexts) == 1
        assert isinstance(contexts[0].get("exception"), RuntimeError)
        assert contexts[0].get("callback") is fail
    finally:
        loop.set_exception_handler(previous)
        await client._delivery.shutdown_callbacks(drain=False)


async def test_force_close_stops_callback_worker() -> None:
    client = AsyncClient(
        client_id="callback-close",
        callback_shutdown_timeout=0.05,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(_message: Message) -> None:
        started.set()
        await release.wait()

    await client._delivery.enqueue_callback(slow, Message(topic="t", payload=b"x"))
    await started.wait()
    assert client._delivery.callback_task is not None

    await client._force_close()
    assert client._delivery.callback_task is None


async def test_force_close_requests_all_task_cancellations_before_awaiting() -> None:
    client = AsyncClient(client_id="connection-task-order")
    effect_cancelled = asyncio.Event()
    effect_release = asyncio.Event()

    async def slow_effect() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            effect_cancelled.set()
            await effect_release.wait()
            raise

    async def reader() -> None:
        await asyncio.Event().wait()

    effect_task = asyncio.create_task(slow_effect())
    reader_task = asyncio.create_task(reader())
    client._effect_pump.task = effect_task
    client._reader_task = reader_task
    await asyncio.sleep(0)

    closing = asyncio.create_task(client._force_close())
    await effect_cancelled.wait()
    reader_cancel_requested = reader_task.cancelling() > 0
    effect_release.set()
    await closing

    assert reader_cancel_requested


async def test_scheduled_flush_records_wakeup_while_active() -> None:
    client = AsyncClient(client_id="flush-wakeup")
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def controlled_apply(effect, *, nowait: bool, epoch: int | None = None) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()

    client._apply_effect = controlled_apply  # type: ignore[method-assign]
    client._engine._emit(
        EffectKind.MESSAGE,
        Message(topic="first", payload=b"1"),
        requires_delivery_mark=True,
    )
    client._effect_pump.collect_from_engine()
    client._effect_pump.schedule()
    await started.wait()

    client._engine._emit(
        EffectKind.MESSAGE,
        Message(topic="second", payload=b"2"),
        requires_delivery_mark=True,
    )
    client._effect_pump.collect_from_engine()
    client._effect_pump.schedule()
    release.set()

    task = client._effect_pump.task
    assert task is not None
    await task
    assert calls == 2
    assert client._effect_pump.applied == 2


def test_effect_collection_stably_prioritizes_sends() -> None:
    client = AsyncClient(client_id="stable-effect-partition")
    client._engine._emit(EffectKind.MESSAGE, Message(topic="first", payload=b"1"))
    client._engine._send(b"send-1")
    client._engine._emit(EffectKind.PINGRESP)
    client._engine._send(b"send-2")

    client._effect_pump.collect_from_engine()

    assert [(effect.kind, effect.data) for effect in client._effect_pump.pending] == [
        (EffectKind.SEND, b"send-1"),
        (EffectKind.SEND, b"send-2"),
        (EffectKind.MESSAGE, Message(topic="first", payload=b"1")),
        (EffectKind.PINGRESP, None),
    ]
