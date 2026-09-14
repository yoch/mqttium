"""Application delivery backpressure is bounded and explicit."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


async def test_iterator_queue_timeout_is_explicit() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=1,
        delivery_timeout=0.01,
    )
    client._delivery.messages_queue.put_nowait(Message(topic="full", payload=b"x"))

    with pytest.raises(MessageDeliveryError, match="Application delivery capacity"):
        await client._apply_delivery_effect(
            EngineEffect(
                kind=EffectKind.MESSAGE,
                data=Message(topic="overflow", payload=b"x"),
            ),
            epoch=client._connection_epoch,
        )


async def test_callback_queue_timeout_is_explicit_and_bounded(monkeypatch) -> None:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=1,
        delivery_timeout=0.01,
        callback_shutdown_timeout=0.01,
    )
    # Isolate admission from the worker: the queue remains saturated until the
    # timeout, without requiring a suspending user callback.
    monkeypatch.setattr(client._delivery, "ensure_callback_worker", lambda: None)
    first = Message(topic="one", payload=b"x")
    client.on_message = lambda _message: None
    await client._delivery.accept(first, client._message_callback)
    with pytest.raises(MessageDeliveryError, match="Application delivery capacity"):
        await client._delivery.accept(Message("two", b"x"), client._message_callback)
    assert client._delivery.callback_queue.qsize() == 1
    assert client._delivery.pending_bytes == client._delivery.logical_size(first)
    client._delivery._discard_callback_queue()
    assert client._delivery.pending_bytes == 0


async def test_callback_worker_preserves_order() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=8)
    seen: list[int] = []

    def callback(message: Message) -> None:
        seen.append(int(message.payload))

    for value in range(8):
        await client._delivery.accept(Message("t", str(value).encode()), callback)
    await asyncio.wait_for(client._delivery.callback_queue.join(), timeout=1.0)

    assert seen == list(range(8))
    await client._delivery.shutdown_callbacks(drain=False)


async def test_force_close_discards_all_old_connection_effects() -> None:
    client = AsyncClient(message_delivery="iterator")
    client._effect_pump.pending.extend(
        [
            EngineEffect(
                kind=EffectKind.MESSAGE,
                data=Message(topic="old", payload=b"old"),
            ),
            EngineEffect(kind=EffectKind.PUBLISH_COMPLETE, data=7),
        ]
    )
    await client._force_close()
    assert not client._effect_pump.pending


async def test_delivery_queue_fast_paths_avoid_timeout_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="iterator")

    async def unexpected_wait_for(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("wait_for must only be used after queue saturation")

    monkeypatch.setattr(asyncio, "wait_for", unexpected_wait_for)
    message = Message(topic="fast", payload=b"x")
    await client._delivery.accept(message, None)
    assert await anext(client.messages()) is message

    called = asyncio.Event()

    def callback(_message: Message) -> None:
        called.set()

    client._delivery.mode = "callback"
    await client._delivery.accept(message, callback)
    await client._delivery.callback_queue.join()
    assert called.is_set()
    await client._delivery.shutdown_callbacks(drain=False)
