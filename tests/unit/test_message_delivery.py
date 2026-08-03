"""Message delivery modes, ordering and stream lifecycle."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


async def _deliver(client: AsyncClient, payload: bytes = b"x") -> None:
    await client._apply_effect(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic="delivery/test", payload=payload),
        ),
        nowait=False,
    )


async def test_auto_callback_does_not_fill_iterator_queue() -> None:
    client = AsyncClient(
        client_id="delivery-auto",
        max_pending_messages=1,
        message_delivery="auto",
    )
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    for index in range(5):
        await _deliver(client, str(index).encode())
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)

    assert received == [b"0", b"1", b"2", b"3", b"4"]
    assert client._messages.empty()
    await client._shutdown_callback_worker(drain=False)


async def test_iterator_mode_ignores_callback() -> None:
    client = AsyncClient(client_id="delivery-iterator", message_delivery="iterator")
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    await _deliver(client)
    assert received == []
    assert client._messages.get_nowait().payload == b"x"


async def test_both_mode_delivers_to_callback_and_iterator() -> None:
    client = AsyncClient(client_id="delivery-both", message_delivery="both")
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    await _deliver(client)
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)
    assert received == [b"x"]
    assert client._messages.get_nowait().payload == b"x"
    await client._shutdown_callback_worker(drain=False)


async def test_stream_drains_messages_before_closed() -> None:
    client = AsyncClient(
        client_id="delivery-close",
        max_pending_messages=2,
        message_delivery="iterator",
    )
    await _deliver(client, b"1")
    await _deliver(client, b"2")
    client._closed.set()
    client._message_ready.set()

    received = [message.payload async for message in client.messages()]
    assert received == [b"1", b"2"]


async def test_explicit_reconnect_resets_closed_message_stream() -> None:
    client = AsyncClient(client_id="delivery-reset", max_pending_messages=2)
    original = client._messages
    client._closed.set()
    client._message_ready.set()

    client._reset_message_stream()

    assert client._messages is not original
    assert client._messages.maxsize == 2
    assert client._messages.empty()
    assert not client._closed.is_set()


def test_invalid_message_delivery_rejected() -> None:
    with pytest.raises(ValueError, match="message_delivery"):
        AsyncClient(message_delivery="invalid")  # type: ignore[arg-type]
