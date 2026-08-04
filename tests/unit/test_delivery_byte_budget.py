"""Shared logical-byte backpressure for inbound application delivery."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(topic: str, payload: bytes) -> EngineEffect:
    return EngineEffect(EffectKind.MESSAGE, Message(topic=topic, payload=payload))


async def test_iterator_delivery_waits_for_shared_byte_capacity() -> None:
    first = Message(topic="delivery/first", payload=b"1234")
    logical_size = len(first.topic) + len(first.payload)
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=4,
        max_pending_delivery_bytes=logical_size,
    )

    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, first), nowait=False)
    blocked = asyncio.create_task(
        client._apply_effect(_effect("delivery/other", b"x"), nowait=False)
    )
    await asyncio.sleep(0)

    assert not blocked.done()
    assert client.pending_delivery_bytes == logical_size
    assert client._messages.qsize() == 1

    stream = client.messages()
    assert await anext(stream) is first
    await asyncio.wait_for(blocked, timeout=1.0)

    assert client._messages.qsize() == 1
    assert client.pending_delivery_bytes == len("delivery/other") + 1
    await stream.aclose()


async def test_both_delivery_counts_payload_once_until_both_consumers_release() -> None:
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()
    message = Message(topic="delivery/both", payload=b"payload")
    logical_size = len(message.topic) + len(message.payload)
    client = AsyncClient(
        message_delivery="both",
        max_pending_delivery_bytes=logical_size,
    )

    async def callback(received: Message) -> None:
        assert received is message
        callback_started.set()
        await callback_release.wait()

    client.on_message = callback
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    await callback_started.wait()

    assert client.pending_delivery_bytes == logical_size
    stream = client.messages()
    assert await anext(stream) is message
    assert client.pending_delivery_bytes == logical_size

    callback_release.set()
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert client.pending_delivery_bytes == 0
    await stream.aclose()
    await client._shutdown_callback_worker(drain=False)


async def test_callback_delivery_releases_bytes_after_callback_finishes() -> None:
    finished = asyncio.Event()
    message = Message(topic="delivery/callback", payload=b"payload")
    logical_size = len(message.topic) + len(message.payload)
    client = AsyncClient(
        message_delivery="callback",
        max_pending_delivery_bytes=logical_size,
    )

    async def callback(received: Message) -> None:
        assert received is message
        finished.set()

    client.on_message = callback
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    await finished.wait()
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)

    assert client.pending_delivery_bytes == 0
    assert client.pending_delivery_high_water_bytes == logical_size
    await client._shutdown_callback_worker(drain=False)


async def test_single_message_larger_than_delivery_budget_fails_explicitly() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_delivery_bytes=4,
    )

    with pytest.raises(MessageDeliveryError, match="exceeding limit"):
        await client._apply_effect(_effect("topic", b"payload"), nowait=False)

    assert client.pending_delivery_bytes == 0
    assert client._messages.empty()
