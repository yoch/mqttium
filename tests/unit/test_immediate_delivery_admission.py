"""Immediate admission shares ownership and bounds with the waiting path."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.types import Message


@pytest.mark.parametrize("mode", ["iterator", "callback"])
@pytest.mark.parametrize("deadline", [None, 5.0])
async def test_available_capacity_avoids_timeout_and_sizes_once(monkeypatch, mode, deadline):
    client = AsyncClient(message_delivery=mode, delivery_timeout=deadline)
    delivery = client._delivery
    seen = []
    sizes = []
    original = delivery.logical_size

    def logical_size(*args):
        sizes.append(args)
        return original(*args)

    def unexpected_timeout(*args):
        raise AssertionError("immediate admission must not enter a timeout context")

    monkeypatch.setattr(delivery, "logical_size", logical_size)
    message = Message(topic="t", payload=b"x")
    try:
        with monkeypatch.context() as patch:
            patch.setattr(asyncio, "timeout", unexpected_timeout)
            await delivery.accept(message, seen.append)
            assert seen == [], "immediate enqueue must not run user code"
            assert delivery.pending_bytes == 2
            assert len(sizes) == 1
        if mode == "iterator":
            assert await anext(client.messages()) is message
        else:
            await delivery.callback_queue.join()
            assert seen == [message]
        assert delivery.pending_bytes == 0
    finally:
        await delivery.shutdown_callbacks(drain=False)


@pytest.mark.parametrize("bound", ["count", "bytes"])
async def test_nowait_refusal_does_not_take_ownership(bound):
    client = AsyncClient(
        max_pending_messages=1 if bound == "count" else 2,
        max_pending_delivery_bytes=2 if bound == "bytes" else 100,
    )
    delivery = client._delivery
    first, second = Message(topic="t", payload=b"x"), Message(topic="t", payload=b"y")
    assert delivery.try_accept(first, None)
    assert not delivery.try_accept(second, None)
    assert delivery.pending_bytes == 2
    assert delivery.messages_queue.qsize() == 1
    assert await anext(client.messages()) is first
    assert delivery.try_accept(second, None)
    assert await anext(client.messages()) is second
    assert delivery.pending_bytes == 0


@pytest.mark.parametrize("mode", ["iterator", "callback"])
async def test_nowait_queue_failure_releases_reservation(monkeypatch, mode):
    client = AsyncClient(message_delivery=mode)
    delivery = client._delivery
    queue = delivery.messages_queue if mode == "iterator" else delivery.callback_queue

    def fail(_item):
        raise RuntimeError("queue failure")

    monkeypatch.setattr(queue, "put_nowait", fail)
    try:
        with pytest.raises(RuntimeError, match="queue failure"):
            delivery.try_accept(Message(topic="t", payload=b"x"), lambda _: None)
        assert delivery.pending_bytes == 0
        assert queue.empty()
    finally:
        await delivery.shutdown_callbacks(drain=False)


async def test_impossible_delivery_is_rejected_before_worker_or_reservation():
    client = AsyncClient(message_delivery="callback", max_pending_delivery_bytes=1)
    with pytest.raises(MessageDeliveryError, match="exceeding limit"):
        await client._delivery.accept(Message(topic="t", payload=b"xx"), lambda _: None)
    assert client.stats().delivery.pending_bytes == 0
    assert client._delivery.callback_task is None


@pytest.mark.parametrize("kind", ["MESSAGE", "DECODED_MESSAGE"])
@pytest.mark.parametrize("eager", [False, True])
async def test_inline_message_enqueue_keeps_callbacks_outside_engine_lock(kind, eager):
    from mqttium.protocol.effects import EffectKind

    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager task factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    client = AsyncClient(message_delivery="callback")
    observed = []
    client.on_message = lambda message: observed.append(
        (message.payload, client._engine_lock.locked())
    )
    try:
        async with client._engine_lock:
            client._engine._emit(EffectKind[kind], Message(topic="t", payload=b"x"))
            client._effect_pump.collect_from_engine()
            assert not client._effect_pump.pending
            assert client._delivery.callback_queue.qsize() == 1
            assert not observed
        await client._delivery.callback_queue.join()
        assert observed == [(b"x", False)]
        assert client.stats().delivery.pending_bytes == 0
    finally:
        await client._delivery.shutdown_callbacks(drain=False)
        loop.set_task_factory(previous)


@pytest.mark.parametrize("mark", [False, True])
async def test_delivery_mark_keeps_existing_lock_and_failure_boundary(monkeypatch, mark):
    from mqttium.enums import QoS
    from mqttium.protocol.effects import EffectKind, EngineEffect

    client = AsyncClient()
    message = Message(topic="t", payload=b"x", mid=7, qos=QoS.AT_LEAST_ONCE)
    effect = EngineEffect(EffectKind.MESSAGE, message, requires_delivery_mark=mark)
    observed = []
    failure = OSError("durable mark failed")

    def mark_delivered(self, mid):
        assert self is client._engine.inbound
        observed.append(
            (mid, client._engine_lock.locked(), client._delivery.messages_queue.qsize())
        )
        raise failure

    monkeypatch.setattr(type(client._engine.inbound), "mark_delivered", mark_delivered)
    if mark:
        assert not client._apply_effect_inline(effect, client._connection_epoch)
        assert client._delivery.pending_bytes == 0
        with pytest.raises(OSError) as caught:
            await client._apply_effect(effect, nowait=False, epoch=client._connection_epoch)
        assert caught.value is failure
        assert client._local_terminal_failure is failure
        assert observed == [(7, True, 1)]
    else:
        assert client._apply_effect_inline(effect, client._connection_epoch)
        assert not observed
        assert client._local_terminal_failure is None
    assert await anext(client.messages()) is message
    assert client.stats().delivery.pending_bytes == 0


async def test_inline_pressure_falls_back_in_order_and_stale_epochs_are_ignored():
    from mqttium.protocol.effects import EffectKind, EngineEffect

    client = AsyncClient(max_pending_messages=1)
    first, second = Message(topic="t", payload=b"a"), Message(topic="t", payload=b"b")
    await client._delivery.accept(first, None)
    client._engine._emit(EffectKind.MESSAGE, second)
    client._effect_pump.collect_from_engine()
    assert len(client._effect_pump.pending) == 1
    stale = EngineEffect(EffectKind.MESSAGE, Message(topic="t", payload=b"old"))
    assert client._apply_effect_inline(stale, client._connection_epoch - 1)
    assert client._delivery.pending_bytes == 2
    assert await anext(client.messages()) is first
    await client._effect_pump.drain()
    assert await anext(client.messages()) is second
    assert client._delivery.pending_bytes == 0
    assert not client._effect_pump.pending
