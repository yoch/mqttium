"""Iterator lifetime across explicit delivery-stream resets."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._delivery import ApplicationDelivery
from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Message
from tests.support import accept_message


def _delivery(*, max_iterator_bytes: int | None = None) -> ApplicationDelivery:
    return ApplicationDelivery(
        mode="iterator",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_iterator_messages=8,
        max_iterator_bytes=max_iterator_bytes,
        iterator_admission_timeout=1.0,
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
    await accept_message(delivery, new_message)

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
    await accept_message(delivery, message)

    assert await asyncio.wait_for(pending, timeout=1.0) == message


async def test_reset_releases_discarded_accounting_exactly_once() -> None:
    delivery = _delivery(max_iterator_bytes=4096)
    await accept_message(delivery, Message(topic="old", payload=b"x"))
    assert delivery.pending_bytes == 4

    delivery.close()
    delivery.reset_stream()

    assert delivery.pending_bytes == 0
    assert delivery.messages_queue.empty()


@pytest.mark.parametrize("created_after_close", (False, True))
async def test_never_started_iterator_cannot_consume_replacement_generation(created_after_close):
    delivery = _delivery(max_iterator_bytes=4096)
    if created_after_close:
        delivery.close()
    old = delivery.messages()
    delivery.close()
    delivery.reset_stream()
    message = Message(topic="new", payload=b"value")
    await accept_message(delivery, message)
    charged = delivery.pending_bytes
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(old), 1)
    assert delivery.pending_bytes == charged
    assert delivery.messages_queue.qsize() == 1
    assert await anext(delivery.messages()) is message
    assert delivery.pending_bytes == 0


async def test_iterator_paused_after_yield_stays_in_old_generation():
    delivery = _delivery()
    old = delivery.messages()
    await accept_message(delivery, Message(topic="old", payload=b"first"))
    assert (await anext(old)).topic == "old"
    delivery.close()
    delivery.reset_stream()
    await accept_message(delivery, Message(topic="new", payload=b"second"))
    with pytest.raises(StopAsyncIteration):
        await anext(old)
    assert (await anext(delivery.messages())).topic == "new"
