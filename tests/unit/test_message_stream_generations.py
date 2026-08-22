"""Iterator lifetime across explicit delivery-stream resets."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._delivery import ApplicationDelivery
from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE
from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Message


def _delivery(*, max_pending_delivery_bytes: int | None = None) -> ApplicationDelivery:
    return ApplicationDelivery(
        mode="iterator",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_pending_messages=8,
        max_pending_callbacks=1,
        max_pending_delivery_bytes=max_pending_delivery_bytes,
        maximum_packet_size=DEFAULT_MAX_PACKET_SIZE,
        delivery_timeout=1.0,
        callback_shutdown_timeout=1.0,
    )


async def test_suspended_iterator_does_not_cross_explicit_stream_reset() -> None:
    delivery = _delivery()
    old_stream = delivery.messages()
    pending = asyncio.create_task(anext(old_stream))
    await asyncio.sleep(0)

    # Explicit connect/reset starts a new application-delivery generation. The
    # close wakes the old iterator; reset happens before it gets scheduled again.
    delivery.close()
    delivery.reset_stream()

    new_message = Message(topic="new/generation", payload=b"new")
    await delivery.put_message(new_message)

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(pending, timeout=1.0)

    new_stream = delivery.messages()
    assert await asyncio.wait_for(anext(new_stream), timeout=1.0) == new_message


async def test_reopen_without_reset_keeps_the_same_stream_generation() -> None:
    delivery = _delivery()
    stream = delivery.messages()
    pending = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)

    # Reopen is connection-transient: unlike reset_stream(), it must not bind
    # consumers to a new queue/event generation.
    delivery.close()
    delivery.reopen()
    message = Message(topic="same/generation", payload=b"resume")
    await delivery.put_message(message)

    assert await asyncio.wait_for(pending, timeout=1.0) == message


async def test_reset_releases_discarded_accounting_exactly_once() -> None:
    delivery = _delivery(max_pending_delivery_bytes=4096)
    token = delivery.try_reserve(512, 1)
    assert token is not None
    delivery.messages_queue.put_nowait((Message(topic="old", payload=b"x"), token))
    delivery.message_ready.set()
    assert delivery.pending_bytes == 512

    delivery.close()
    delivery.reset_stream()

    assert delivery.pending_bytes == 0
    assert delivery.messages_queue.empty()
