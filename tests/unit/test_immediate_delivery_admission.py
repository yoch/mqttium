"""Immediate admission shares ownership and bounds with the waiting path."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import QoS
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import accept_message


@pytest.mark.parametrize("deadline", [None, 5.0])
async def test_available_capacity_avoids_timeout_and_sizes_once(monkeypatch, deadline):
    client = AsyncClient(delivery_timeout=deadline)
    delivery = client._delivery
    sizes = []
    original = delivery.logical_size

    def logical_size(*args):
        sizes.append(args)
        return original(*args)

    def unexpected_timeout(*args):
        raise AssertionError("immediate admission must not enter a timeout context")

    monkeypatch.setattr(delivery, "logical_size", logical_size)
    message = Message(topic="t", payload=b"x")
    with monkeypatch.context() as patch:
        patch.setattr(asyncio, "timeout", unexpected_timeout)
        assert delivery.accept(message, None) is None
        assert delivery.pending_bytes == 2
        assert len(sizes) == 1
    assert await anext(client.messages()) is message
    assert delivery.pending_bytes == 0


@pytest.mark.parametrize("deadline", [None, 5.0])
async def test_callback_mode_delivers_inline_without_sizing_or_reservation(monkeypatch, deadline):
    client = AsyncClient(message_delivery="callback", delivery_timeout=deadline)
    delivery = client._delivery
    seen = []

    def unexpected_size(*args):
        raise AssertionError("callback delivery must not size or reserve bytes")

    def unexpected_timeout(*args):
        raise AssertionError("callback delivery must not enter a timeout context")

    monkeypatch.setattr(delivery, "logical_size", unexpected_size)
    message = Message(topic="t", payload=b"x")
    with monkeypatch.context() as patch:
        patch.setattr(asyncio, "timeout", unexpected_timeout)
        assert delivery.accept(message, seen.append) is None
    assert seen == [message]
    assert delivery.pending_bytes == 0
    assert delivery.messages_queue.empty()
    assert delivery.callback_invocations == 1
    assert client.stats().delivery.callback_invocations == 1


@pytest.mark.parametrize("bound", ["count", "bytes"])
async def test_waiting_path_takes_ownership_only_once_capacity_exists(bound):
    client = AsyncClient(
        max_pending_messages=1 if bound == "count" else 2,
        max_pending_delivery_bytes=2 if bound == "bytes" else 100,
    )
    delivery = client._delivery
    first, second = Message(topic="t", payload=b"x"), Message(topic="t", payload=b"y")
    assert delivery.accept(first, None) is None
    waiting = delivery.accept(second, None)
    assert waiting is not None
    assert delivery.pending_bytes == 2
    assert delivery.messages_queue.qsize() == 1
    assert await anext(client.messages()) is first
    assert delivery.pending_bytes == 0
    await waiting
    assert delivery.pending_bytes == 2
    assert await anext(client.messages()) is second
    assert delivery.pending_bytes == 0


async def test_impossible_delivery_is_rejected_before_reservation():
    client = AsyncClient(max_pending_delivery_bytes=1)
    with pytest.raises(MessageDeliveryError, match="exceeding limit"):
        client._delivery.accept(Message(topic="t", payload=b"xx"), None)
    assert client.stats().delivery.pending_bytes == 0
    assert client._delivery.messages_queue.empty()
    assert client._delivery.waiters == 0


@pytest.mark.parametrize("kind", ["MESSAGE", "DECODED_MESSAGE"])
async def test_reader_message_delivery_keeps_callbacks_outside_engine_lock(kind):
    client = AsyncClient(message_delivery="callback")
    observed = []
    client.on_message = lambda message: observed.append(
        (message.payload, client._engine_lock.locked())
    )
    async with client._engine_lock:
        client._engine._emit(EffectKind[kind], Message(topic="t", payload=b"x"))
        client._effect_pump.collect_from_engine()
        assert not client._effect_pump.pending
        assert client._delivery_lane.pending_count == 1
        assert not observed
    await client._delivery_lane.drain()
    assert observed == [(b"x", False)]
    assert client.stats().delivery.callback_invocations == 1
    assert client.stats().delivery.pending_bytes == 0


@pytest.mark.parametrize("path", ["immediate", "locked", "waiting"])
@pytest.mark.parametrize("mark", [False, True])
async def test_delivery_mark_keeps_lock_and_failure_boundary(monkeypatch, mark, path):
    client = AsyncClient(max_pending_messages=1)
    delivery = client._delivery
    message = Message(topic="t", payload=b"x", mid=7, qos=QoS.AT_LEAST_ONCE)
    effect = EngineEffect(EffectKind.MESSAGE, message, requires_delivery_mark=mark)
    observed = []
    failure = OSError("durable mark failed")

    def mark_delivered(self, mid):
        assert self is client._engine.inbound
        observed.append((mid, client._engine_lock.locked(), delivery.messages_queue.qsize()))
        raise failure

    monkeypatch.setattr(type(client._engine.inbound), "mark_delivered", mark_delivered)
    assert not client._apply_effect_inline(effect, client._connection_epoch)
    stream = client.messages()
    blocker = Message(topic="t", payload=b"0")
    if path == "waiting":
        assert delivery.accept(blocker, None) is None
    # The mark is deferred behind a contended lock or a capacity wait; the
    # free-lock immediate handoff completes it synchronously.
    deferred = path == "waiting" or (path == "locked" and mark)
    if path == "locked":
        await client._engine_lock.acquire()
    try:
        if mark and not deferred:
            with pytest.raises(OSError) as caught:
                client._apply_delivery_effect(effect, client._connection_epoch)
            assert caught.value is failure
            assert observed == [(7, False, 1)]
            pending = None
        else:
            pending = client._apply_delivery_effect(effect, client._connection_epoch)
            assert not observed
            assert (pending is not None) == deferred
    finally:
        if path == "locked":
            client._engine_lock.release()
    if path == "waiting":
        assert await anext(stream) is blocker
    if pending is not None:
        if mark:
            with pytest.raises(OSError) as caught:
                await pending
            assert caught.value is failure
            assert observed == [(7, True, 1)]
        else:
            await pending
            assert not observed
    assert client._local_terminal_failure is (failure if mark else None)
    assert await anext(stream) is message
    assert delivery.pending_bytes == 0
    await stream.aclose()


async def test_reader_lane_delivers_in_order_and_skips_stale_epochs():
    client = AsyncClient(max_pending_messages=1)
    first, second = Message(topic="t", payload=b"a"), Message(topic="t", payload=b"b")
    await accept_message(client._delivery, first, None)
    client._engine._emit(EffectKind.MESSAGE, second)
    client._effect_pump.collect_from_engine()
    assert not client._effect_pump.pending
    assert client._delivery_lane.pending_count == 1
    stale = EngineEffect(EffectKind.MESSAGE, Message(topic="t", payload=b"old"))
    client._delivery_lane.collect(
        [stale], client._connection_epoch - 1, client._effect_pump.enqueued
    )
    assert client._delivery_lane.pending_count == 2
    stream = client.messages()
    assert await anext(stream) is first
    await client._delivery_lane.drain()
    assert client._delivery.pending_bytes == 2
    assert client._delivery_lane.outstanding == 0
    assert await anext(stream) is second
    assert client._delivery.pending_bytes == 0
    assert not client._effect_pump.pending
    await stream.aclose()
