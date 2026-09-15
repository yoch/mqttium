"""Shared logical-byte backpressure for inbound application delivery."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message, Properties
from tests.support import apply_delivery_effect, deliver_message


def _effect(topic: str, payload: bytes) -> EngineEffect:
    return EngineEffect(EffectKind.MESSAGE, Message(topic=topic, payload=payload))


async def test_iterator_delivery_waits_for_shared_byte_capacity() -> None:
    first = Message(topic="delivery/first", payload=b"1234")
    logical_size = len(first.topic) + len(first.payload)
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_messages=4,
        max_iterator_bytes=logical_size,
    )

    await deliver_message(client, first)
    blocked = asyncio.create_task(apply_delivery_effect(client, _effect("delivery/other", b"x")))
    await asyncio.sleep(0)

    assert not blocked.done()
    assert client.stats().delivery.waiters == 1
    assert client.stats().delivery.iterator_bytes == logical_size
    assert client._delivery.messages_queue.qsize() == 1
    assert client._delivery.messages_queue._unfinished_tasks == 0

    stream = client.messages()
    assert await anext(stream) is first
    await asyncio.wait_for(blocked, timeout=1.0)

    assert client._delivery.messages_queue.qsize() == 1
    assert client.stats().delivery.iterator_bytes == len("delivery/other") + 1
    await stream.aclose()


async def test_unbounded_iterator_skips_byte_accounting() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_messages=4,
        max_iterator_bytes=None,
    )
    message = Message(topic="delivery/unbounded", payload=b"payload")

    def fail_logical_size(*_args: object, **_kwargs: object) -> int:
        raise AssertionError("unbounded iterator must not compute logical byte size")

    client._delivery.logical_size = fail_logical_size  # type: ignore[method-assign]
    await deliver_message(client, message)

    stats = client.stats().delivery
    assert stats.iterator_byte_limit is None
    assert stats.iterator_bytes == 0
    assert stats.iterator_high_water_bytes == 0
    assert client._delivery.messages_queue.qsize() == 1
    assert client._delivery.messages_queue._unfinished_tasks == 0

    stream = client.messages()
    assert await anext(stream) is message
    assert client.stats().delivery.iterator_bytes == 0
    assert client.stats().delivery.iterator_high_water_bytes == 0
    await stream.aclose()


async def test_unbounded_iterator_keeps_count_backpressure() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_messages=1,
        max_iterator_bytes=None,
    )
    first = Message(topic="delivery/first", payload=b"1")
    second = Message(topic="delivery/second", payload=b"2")

    await deliver_message(client, first)
    blocked = asyncio.create_task(deliver_message(client, second))
    await asyncio.sleep(0)

    assert not blocked.done()
    assert client.stats().delivery.waiters == 1
    assert client.stats().delivery.iterator_bytes == 0

    stream = client.messages()
    assert await anext(stream) is first
    await asyncio.wait_for(blocked, timeout=1.0)

    assert client._delivery.messages_queue.qsize() == 1
    assert client.stats().delivery.iterator_bytes == 0
    assert await anext(stream) is second
    await stream.aclose()


async def test_unbounded_iterator_reset_discards_bare_messages() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_messages=2,
        max_iterator_bytes=None,
    )
    await deliver_message(client, Message(topic="delivery/reset", payload=b"payload"))

    client._delivery.close()
    client._delivery.reset_stream()

    stats = client.stats().delivery
    assert client._delivery.messages_queue.empty()
    assert stats.iterator_bytes == 0
    assert stats.iterator_high_water_bytes == 0
    assert stats.iterator_byte_limit is None
    assert not client._delivery.closed.is_set()


async def test_callback_delivery_charges_no_bytes() -> None:
    finished = asyncio.Event()
    message = Message(topic="delivery/callback", payload=b"payload")
    client = AsyncClient(message_delivery="callback")

    def callback(received: Message) -> None:
        assert received is message
        finished.set()

    client.on_message = callback
    await deliver_message(client, message)
    assert finished.is_set()
    await deliver_message(client, message)

    assert client._delivery.callback_invocations == 2
    assert client.stats().delivery.iterator_bytes == 0
    assert client.stats().delivery.iterator_high_water_bytes == 0
    assert client.stats().delivery.waiters == 0


async def test_single_message_larger_than_delivery_budget_fails_explicitly() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_bytes=4,
    )

    with pytest.raises(MessageDeliveryError, match="exceeding limit"):
        await apply_delivery_effect(client, _effect("topic", b"payload"))

    assert client.stats().delivery.iterator_bytes == 0
    assert client._delivery.messages_queue.empty()


async def test_delivery_budget_wakes_multiple_waiters_without_overcommit() -> None:
    messages = [Message(topic="delivery/same", payload=bytes([value])) for value in range(3)]
    logical_size = len(messages[0].topic) + len(messages[0].payload)
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_messages=4,
        max_iterator_bytes=logical_size,
    )

    await deliver_message(client, messages[0])
    blocked = {asyncio.create_task(deliver_message(client, message)) for message in messages[1:]}
    await asyncio.sleep(0)
    assert not any(task.done() for task in blocked)
    assert client.stats().delivery.waiters == 2

    stream = client.messages()
    assert await anext(stream) is messages[0]
    done, pending = await asyncio.wait(
        blocked,
        timeout=1.0,
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert len(done) == 1
    assert len(pending) == 1
    assert client.stats().delivery.iterator_bytes == logical_size
    assert client._delivery.messages_queue.qsize() == 1

    assert await anext(stream) in messages[1:]
    await asyncio.wait_for(next(iter(pending)), timeout=1.0)
    assert client.stats().delivery.iterator_bytes == logical_size
    assert client._delivery.messages_queue.qsize() == 1

    assert await anext(stream) in messages[1:]
    assert client.stats().delivery.iterator_bytes == 0
    await stream.aclose()


def _byte_budget_client(protocol: MQTTProtocolVersion) -> AsyncClient:
    """A client whose small-message fast path is enabled (see the test above)."""
    return AsyncClient(
        message_delivery="iterator",
        max_iterator_messages=4,
        max_iterator_bytes=64 * 1024 * 1024,
        protocol=protocol,
    )


async def test_mqtt5_publish_without_properties_is_accounted() -> None:
    # decode_properties() returns an empty Properties() rather than None for a
    # zero-length MQTT 5 property table, so identity testing would have pushed
    # every property-less v5 PUBLISH into exact accounting.
    client = _byte_budget_client(MQTTProtocolVersion.MQTTv5)
    message = Message(topic="small/topic", payload=b"payload", properties=Properties())

    await deliver_message(client, message)

    assert client.stats().delivery.iterator_bytes == len(message.topic) + len(message.payload)
    assert client._delivery.messages_queue.qsize() == 1


async def test_mqtt5_publish_with_properties_stays_exactly_accounted() -> None:
    client = _byte_budget_client(MQTTProtocolVersion.MQTTv5)
    properties = Properties()
    properties = Properties(
        {**properties.values, "user_property": (*properties.get("user_property", ()), ("k", "v"))}
    )
    message = Message(topic="small/topic", payload=b"payload", properties=properties)

    await deliver_message(client, message)

    assert client.stats().delivery.iterator_bytes == client._delivery.logical_size(message)
    assert client.stats().delivery.iterator_bytes > len(message.topic) + len(message.payload)


async def test_mqtt311_delivery_is_accounted() -> None:
    client = _byte_budget_client(MQTTProtocolVersion.MQTTv311)
    message = Message(topic="small/topic", payload=b"payload")

    await deliver_message(client, message)

    assert client.stats().delivery.iterator_bytes == len(message.topic) + len(message.payload)
    assert client._delivery.messages_queue.qsize() == 1


async def test_small_budget_accounts_property_less_mqtt5() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_iterator_bytes=8 * 1024 * 1024,
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    message = Message(topic="small/topic", payload=b"payload", properties=Properties())

    await deliver_message(client, message)

    assert client.stats().delivery.iterator_bytes == len(message.topic) + len(message.payload)
