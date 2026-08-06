from mqttium.api import AsyncClient
from mqttium.types import Message


def test_message_has_no_delivery_accounting_slots() -> None:
    message = Message(topic="test/topic", payload=b"payload")

    assert not hasattr(message, "_delivery_logical_bytes")
    assert not hasattr(message, "_delivery_references")


def test_single_consumer_reservation_uses_compact_integer_token() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="iterator")
    message = Message(topic="test/topic", payload=b"payload")

    token = client._try_reserve_delivery(
        message,
        1,
        100,
        callback_delivery=False,
    )

    assert token == 100
    assert client.pending_delivery_bytes == 100
    client._release_delivery_reference_nowait(token)
    assert client.pending_delivery_bytes == 0


def test_shared_reservation_releases_on_last_consumer() -> None:
    client = AsyncClient(max_pending_delivery_bytes=1_024, message_delivery="both")
    message = Message(topic="test/topic", payload=b"payload")

    token = client._try_reserve_delivery(
        message,
        2,
        100,
        callback_delivery=True,
    )

    assert token is not None
    assert not isinstance(token, int)
    assert client.pending_delivery_bytes == 100

    client._release_delivery_reference_nowait(token)
    assert client.pending_delivery_bytes == 100
    client._release_delivery_reference_nowait(token)
    assert client.pending_delivery_bytes == 0
