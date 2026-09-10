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


async def test_callback_does_not_fill_iterator_queue() -> None:
    client = AsyncClient(
        client_id="delivery-auto",
        max_pending_messages=1,
        message_delivery="callback",
    )
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    for index in range(5):
        await _deliver(client, str(index).encode())
    await asyncio.wait_for(client._delivery.callback_queue.join(), timeout=1.0)

    assert received == [b"0", b"1", b"2", b"3", b"4"]
    assert client._delivery.messages_queue.empty()
    await client._delivery.shutdown_callbacks(drain=False)


async def test_callback_self_cancellation_does_not_stop_worker() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=4)
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
        await asyncio.wait_for(client._delivery.callback_queue.join(), timeout=1)

        assert received == [b"after"]
        assert client._delivery.callback_task is not None
        assert not client._delivery.callback_task.done()
        assert len(reported) == 1
        assert isinstance(reported[0].get("exception"), asyncio.CancelledError)
    finally:
        loop.set_exception_handler(previous_handler)
        await client._delivery.shutdown_callbacks(drain=False)


async def test_unlimited_bytes_keeps_selected_destination() -> None:
    iterator_client = AsyncClient(max_pending_delivery_bytes=None)
    await _deliver(iterator_client, b"iterator")
    assert (await anext(iterator_client.messages())).payload == b"iterator"

    callback_client = AsyncClient(max_pending_delivery_bytes=None, message_delivery="callback")
    received: list[bytes] = []
    callback_client.on_message = lambda message: received.append(message.payload)
    await _deliver(callback_client, b"callback")
    await callback_client._delivery.callback_queue.join()
    assert received == [b"callback"]
    await callback_client._delivery.shutdown_callbacks(drain=False)


@pytest.mark.parametrize("mode", ["iterator", "callback"])
async def test_unaccounted_specialized_delivery_modes(mode: str) -> None:
    client = AsyncClient(
        message_delivery=mode,  # type: ignore[arg-type]
        max_pending_delivery_bytes=None,
    )
    received: list[bytes] = []
    client.on_message = lambda message: received.append(message.payload)

    await _deliver(client, mode.encode())
    if mode in ("callback", "both"):
        await client._delivery.callback_queue.join()
        assert received == [mode.encode()]
        await client._delivery.shutdown_callbacks(drain=False)
    if mode in ("iterator", "both"):
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
        max_pending_messages=2,
        message_delivery="iterator",
    )
    await _deliver(client, b"1")
    await _deliver(client, b"2")
    client._delivery.closed.set()
    client._delivery.message_ready.set()

    received = [message.payload async for message in client.messages()]
    assert received == [b"1", b"2"]


async def test_explicit_reconnect_resets_closed_message_stream() -> None:
    client = AsyncClient(client_id="delivery-reset", max_pending_messages=2)
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
