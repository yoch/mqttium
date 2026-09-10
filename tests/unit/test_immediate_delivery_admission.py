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
