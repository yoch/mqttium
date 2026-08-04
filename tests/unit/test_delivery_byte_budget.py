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


async def test_delivery_budget_wakes_multiple_waiters_without_overcommit() -> None:
    messages = [Message(topic="delivery/same", payload=bytes([value])) for value in range(3)]
    logical_size = len(messages[0].topic) + len(messages[0].payload)
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=4,
        max_pending_delivery_bytes=logical_size,
    )

    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, messages[0]), nowait=False)
    blocked = {
        asyncio.create_task(
            client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
        )
        for message in messages[1:]
    }
    await asyncio.sleep(0)
    assert not any(task.done() for task in blocked)

    stream = client.messages()
    assert await anext(stream) is messages[0]
    done, pending = await asyncio.wait(
        blocked,
        timeout=1.0,
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert len(done) == 1
    assert len(pending) == 1
    assert client.pending_delivery_bytes == logical_size
    assert client._messages.qsize() == 1

    assert await anext(stream) in messages[1:]
    await asyncio.wait_for(next(iter(pending)), timeout=1.0)
    assert client.pending_delivery_bytes == logical_size
    assert client._messages.qsize() == 1

    assert await anext(stream) in messages[1:]
    assert client.pending_delivery_bytes == 0
    await stream.aclose()


async def test_default_budget_reserves_count_bounded_small_message_pool() -> None:
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=4,
        max_pending_delivery_bytes=64 * 1024 * 1024,
    )
    message = Message(topic="small/topic", payload=b"payload")

    async def callback(_message: Message) -> None:
        callback_started.set()
        await callback_release.wait()

    client.on_message = callback
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    await callback_started.wait()

    assert client.delivery_small_budget_bytes == 8 * 1024 * 1024
    assert client.delivery_small_message_limit is not None
    assert len(message.payload) + len(message.topic) <= client.delivery_small_message_limit
    # The small pool is statically reserved from queue-count bounds, so the
    # exact-accounted dynamic pool remains untouched.
    assert client.pending_delivery_bytes == 0

    callback_release.set()
    await client._callback_queue.join()
    await client._shutdown_callback_worker(drain=False)


def test_small_pool_is_disabled_if_it_reduces_single_packet_capacity() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_delivery_bytes=8 * 1024 * 1024,
    )

    assert client.delivery_small_budget_bytes == 0
    assert client.delivery_small_message_limit == 0
    assert client._delivery_accounted_limit == 8 * 1024 * 1024
