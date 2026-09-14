"""Message delivery modes, ordering and stream lifecycle."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.types import Message
from tests.support import deliver_message


async def _deliver(client: AsyncClient, payload: bytes = b"x") -> None:
    await deliver_message(client, Message(topic="delivery/test", payload=payload))


async def test_callback_does_not_fill_iterator_queue() -> None:
    client = AsyncClient(client_id="delivery-auto", message_delivery="callback")
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    for index in range(5):
        await _deliver(client, str(index).encode())

    assert received == [b"0", b"1", b"2", b"3", b"4"]
    assert client._delivery.messages_queue.empty()
    assert client._delivery.callback_invocations == 5
    assert client.stats().delivery.iterator_bytes == 0


async def test_callback_self_cancellation_does_not_stop_inline_delivery() -> None:
    client = AsyncClient(message_delivery="callback")
    received: list[bytes] = []
    reported: list[dict[str, object]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: reported.append(context))

    def on_message(message: Message) -> None:
        if message.payload == b"cancel-self":
            raise asyncio.CancelledError("user callback cancellation")
        received.append(message.payload)

    client.on_message = on_message
    try:
        await _deliver(client, b"cancel-self")
        await _deliver(client, b"after")

        assert received == [b"after"]
        assert client._delivery.callback_invocations == 2
        assert len(reported) == 1
        assert isinstance(reported[0].get("exception"), asyncio.CancelledError)
    finally:
        loop.set_exception_handler(previous_handler)


async def test_unlimited_bytes_keeps_selected_destination() -> None:
    iterator_client = AsyncClient(max_iterator_bytes=None)
    await _deliver(iterator_client, b"iterator")
    assert (await anext(iterator_client.messages())).payload == b"iterator"

    callback_client = AsyncClient(message_delivery="callback")
    received: list[bytes] = []
    callback_client.on_message = lambda message: received.append(message.payload)
    await _deliver(callback_client, b"callback")
    assert received == [b"callback"]


@pytest.mark.parametrize("mode", ["iterator", "callback"])
async def test_unaccounted_specialized_delivery_modes(mode: str) -> None:
    # Iterator bounds describe a queue callback delivery does not own.
    bounds = {"max_iterator_bytes": None} if mode == "iterator" else {}
    client = AsyncClient(
        message_delivery=mode,  # type: ignore[arg-type]
        **bounds,
    )
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    await _deliver(client, mode.encode())
    if mode == "callback":
        assert received == [mode.encode()]
        assert client._delivery.messages_queue.empty()
    else:
        assert received == []
        assert (await anext(client.messages())).payload == mode.encode()


async def test_iterator_mode_ignores_callback() -> None:
    client = AsyncClient(client_id="delivery-iterator", message_delivery="iterator")
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    await _deliver(client)
    assert received == []
    assert (await anext(client.messages())).payload == b"x"


@pytest.mark.parametrize("mode", ["auto", "both"])
def test_removed_modes_are_rejected(mode) -> None:
    with pytest.raises(ValueError, match="message_delivery"):
        AsyncClient(message_delivery=mode)


async def test_stream_drains_messages_before_closed() -> None:
    client = AsyncClient(
        client_id="delivery-close",
        max_iterator_messages=2,
        message_delivery="iterator",
    )
    await _deliver(client, b"1")
    await _deliver(client, b"2")
    client._delivery.closed.set()
    client._delivery.message_ready.set()

    received = [message.payload async for message in client.messages()]
    assert received == [b"1", b"2"]


async def test_explicit_reconnect_resets_closed_message_stream() -> None:
    client = AsyncClient(client_id="delivery-reset", max_iterator_messages=2)
    original = client._delivery.messages_queue
    client._delivery.closed.set()
    client._delivery.message_ready.set()

    await client._reset_message_stream()

    assert client._delivery.messages_queue is not original
    assert client._delivery.messages_queue.maxsize == 2
    assert client._delivery.messages_queue.empty()
    assert not client._delivery.closed.is_set()


def test_invalid_message_delivery_rejected() -> None:
    with pytest.raises(ValueError, match="message_delivery"):
        AsyncClient(message_delivery="invalid")  # type: ignore[arg-type]
