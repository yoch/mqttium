"""Invariants for queue-owned inbound delivery accounting."""

from dataclasses import asdict

from mqttium.api import AsyncClient
from mqttium.types import Message
from tests.support import accept_message


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
    client = AsyncClient(max_iterator_bytes=1024)
    message = Message(topic="t", payload=b"data")
    await accept_message(client._delivery, message)
    await accept_message(client._delivery, message)
    assert client.stats().delivery.iterator_bytes == 10
    stream = client.messages()
    assert await anext(stream) is message
    assert client.stats().delivery.iterator_bytes == 5
    assert await anext(stream) is message
    assert client.stats().delivery.iterator_bytes == 0
    await stream.aclose()
