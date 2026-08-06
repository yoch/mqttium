from dataclasses import asdict

from mqttium.api import AsyncClient
from mqttium.types import Message


def test_message_has_only_public_delivery_data() -> None:
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
    ]


def test_single_consumer_uses_compact_integer_token() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="iterator")

    token = client._try_reserve_delivery(100, 1)

    assert token == 100
    assert client.pending_delivery_bytes == 100
    client._release_delivery_reference_nowait(token)
    assert client.pending_delivery_bytes == 0


def test_shared_reservation_releases_on_last_consumer() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="both")

    token = client._try_reserve_delivery(100, 2)

    assert token is not None
    assert not isinstance(token, int)
    assert client.pending_delivery_bytes == 100

    client._release_delivery_reference_nowait(token)
    assert client.pending_delivery_bytes == 100
    client._release_delivery_reference_nowait(token)
    assert client.pending_delivery_bytes == 0


def test_shared_reservation_ignores_duplicate_release() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="both")
    token = client._try_reserve_delivery(100, 2)
    assert token is not None

    client._release_delivery_reference_nowait(token)
    client._release_delivery_reference_nowait(token)
    client._release_delivery_reference_nowait(token)

    assert client.pending_delivery_bytes == 0


def test_same_message_can_have_independent_reservations() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="iterator")
    message = Message(topic="test/topic", payload=b"payload")

    first = client._try_reserve_delivery(client._delivery_logical_size(message), 1)
    second = client._try_reserve_delivery(client._delivery_logical_size(message), 1)

    assert first is not None
    assert second is not None
    assert client.pending_delivery_bytes == first + second
    client._release_delivery_reference_nowait(first)
    client._release_delivery_reference_nowait(second)
    assert client.pending_delivery_bytes == 0
