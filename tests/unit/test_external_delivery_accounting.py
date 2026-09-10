"""Invariants for queue-owned inbound delivery accounting."""

from dataclasses import asdict

from mqttium.api import AsyncClient
from mqttium.types import Message


def test_message_keeps_delivery_accounting_external() -> None:
    message = Message(topic="test/topic", payload=b"payload")

    assert not hasattr(message, "_delivery_logical_bytes")
    assert not hasattr(message, "_delivery_references")
    assert list(asdict(message)) == [
        "topic",
        "payload",
        "qos",
        "retain",
        "dup",
        "mid",
        "properties",
        "_ack_token",
    ]
    assert message._ack_token is None


async def test_same_message_has_independent_queue_reservations() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1024)
    message = Message(topic="t", payload=b"data")
    await client._delivery.accept(message, None)
    await client._delivery.accept(message, None)
    assert client.stats().delivery.pending_bytes == 10
    stream = client.messages()
    assert await anext(stream) is message
    assert client.stats().delivery.pending_bytes == 5
    assert await anext(stream) is message
    assert client.stats().delivery.pending_bytes == 0
    await stream.aclose()
