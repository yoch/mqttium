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
    await client._enqueue_outbound(b"x")

    publishing = asyncio.create_task(client.publish("effect/t", b"payload", qos=0))
    for _ in range(100):
        if client._outbound_waiters:
            break
        await asyncio.sleep(0)
    assert client._outbound_waiters == 1

    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    assert any(effect.kind is EffectKind.SEND for effect in client._pending_effects)

    blocked = client._outbound.get_nowait()
    client._outbound.task_done()
    client._outbound_bytes -= len(blocked)
    async with client._outbound_space:
        client._outbound_space.notify_all()

    await client._drain_effects()
    assert not client._pending_effects
    assert client._outbound.qsize() == 1


async def test_callback_exception_reaches_loop_exception_handler() -> None:
    client = AsyncClient(client_id="callback-error")
    loop = asyncio.get_running_loop()
    contexts: list[dict[str, object]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: contexts.append(context))

    def fail(_message: Message) -> None:
        raise RuntimeError("callback failed")

    try:
        await client._enqueue_callback(fail, Message(topic="t", payload=b"x"))
        await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
        assert len(contexts) == 1
        assert isinstance(contexts[0].get("exception"), RuntimeError)
        assert contexts[0].get("callback") is fail
    finally:
        loop.set_exception_handler(previous)
        await client._shutdown_callback_worker(drain=False)


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

    await client._enqueue_callback(slow, Message(topic="t", payload=b"x"))
    await started.wait()
    assert client._callback_worker_task is not None

    await client._force_close()
    assert client._callback_worker_task is None


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
    client._engine._emit(EffectKind.PINGRESP)
    client._collect_effects_locked()
    client._schedule_effect_flush()
    await started.wait()

    client._engine._emit(EffectKind.PINGRESP)
    client._collect_effects_locked()
    client._schedule_effect_flush()
    release.set()

    task = client._effect_flush_task
    assert task is not None
    await task
    assert calls == 2
    assert client._effect_applied == 2


def test_effect_collection_stably_prioritizes_sends() -> None:
    client = AsyncClient(client_id="stable-effect-partition")
    client._engine._emit(EffectKind.MESSAGE, Message(topic="first", payload=b"1"))
    client._engine._send(b"send-1")
    client._engine._emit(EffectKind.PINGRESP)
    client._engine._send(b"send-2")

    client._collect_effects_locked()

    assert [(effect.kind, effect.data) for effect in client._pending_effects] == [
        (EffectKind.SEND, b"send-1"),
        (EffectKind.SEND, b"send-2"),
        (EffectKind.MESSAGE, Message(topic="first", payload=b"1")),
        (EffectKind.PINGRESP, None),
    ]
