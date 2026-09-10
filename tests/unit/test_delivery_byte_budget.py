"""Shared logical-byte backpressure for inbound application delivery."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message, Properties


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
    assert client.stats().delivery.pending_bytes == logical_size
    assert client._delivery.messages_queue.qsize() == 1

    stream = client.messages()
    assert await anext(stream) is first
    await asyncio.wait_for(blocked, timeout=1.0)

    assert client._delivery.messages_queue.qsize() == 1
    assert client.stats().delivery.pending_bytes == len("delivery/other") + 1
    await stream.aclose()


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
    await asyncio.wait_for(client._delivery.callback_queue.join(), timeout=1.0)

    assert client.stats().delivery.pending_bytes == 0
    assert client.stats().delivery.pending_high_water_bytes == logical_size
    await client._delivery.shutdown_callbacks(drain=False)


async def test_single_message_larger_than_delivery_budget_fails_explicitly() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_delivery_bytes=4,
    )

    with pytest.raises(MessageDeliveryError, match="exceeding limit"):
        await client._apply_effect(_effect("topic", b"payload"), nowait=False)

    assert client.stats().delivery.pending_bytes == 0
    assert client._delivery.messages_queue.empty()


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
    assert client.stats().delivery.pending_bytes == logical_size
    assert client._delivery.messages_queue.qsize() == 1

    assert await anext(stream) in messages[1:]
    await asyncio.wait_for(next(iter(pending)), timeout=1.0)
    assert client.stats().delivery.pending_bytes == logical_size
    assert client._delivery.messages_queue.qsize() == 1

    assert await anext(stream) in messages[1:]
    assert client.stats().delivery.pending_bytes == 0
    await stream.aclose()


def _byte_budget_client(protocol: MQTTProtocolVersion) -> AsyncClient:
    """A client whose small-message fast path is enabled (see the test above)."""
    return AsyncClient(
        message_delivery="iterator",
        max_pending_messages=4,
        max_pending_delivery_bytes=64 * 1024 * 1024,
        protocol=protocol,
    )


async def test_mqtt5_publish_without_properties_is_accounted() -> None:
    # decode_properties() returns an empty Properties() rather than None for a
    # zero-length MQTT 5 property table, so identity testing would have pushed
    # every property-less v5 PUBLISH into exact accounting.
    client = _byte_budget_client(MQTTProtocolVersion.MQTTv5)
    message = Message(topic="small/topic", payload=b"payload", properties=Properties())

    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)

    assert client.stats().delivery.pending_bytes == len(message.topic) + len(message.payload)
    assert client._delivery.messages_queue.qsize() == 1


async def test_mqtt5_publish_with_properties_stays_exactly_accounted() -> None:
    client = _byte_budget_client(MQTTProtocolVersion.MQTTv5)
    properties = Properties()
    properties = Properties(
        {**properties.values, "user_property": (*properties.get("user_property", ()), ("k", "v"))}
    )
    message = Message(topic="small/topic", payload=b"payload", properties=properties)

    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)

    assert client.stats().delivery.pending_bytes == client._delivery.logical_size(message)
    assert client.stats().delivery.pending_bytes > len(message.topic) + len(message.payload)


async def test_mqtt311_delivery_is_accounted() -> None:
    client = _byte_budget_client(MQTTProtocolVersion.MQTTv311)
    message = Message(topic="small/topic", payload=b"payload")

    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)

    assert client.stats().delivery.pending_bytes == len(message.topic) + len(message.payload)
    assert client._delivery.messages_queue.qsize() == 1


async def test_small_budget_accounts_property_less_mqtt5() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_delivery_bytes=8 * 1024 * 1024,
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    message = Message(topic="small/topic", payload=b"payload", properties=Properties())

    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)

    assert client.stats().delivery.pending_bytes == len(message.topic) + len(message.payload)
