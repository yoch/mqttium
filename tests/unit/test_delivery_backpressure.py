"""Application delivery backpressure is bounded and explicit."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import accept_message, apply_delivery_effect


async def test_iterator_queue_timeout_is_explicit() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=1,
        delivery_timeout=0.01,
    )
    await accept_message(client._delivery, Message(topic="full", payload=b"x"))

    with pytest.raises(MessageDeliveryError, match="Application delivery capacity"):
        await apply_delivery_effect(
            client,
            EngineEffect(
                kind=EffectKind.MESSAGE,
                data=Message(topic="overflow", payload=b"x"),
            ),
        )
    assert client._delivery.waiters == 0
    assert client._delivery.messages_queue.qsize() == 1


async def test_inline_callbacks_preserve_order() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[int] = []

    def callback(message: Message) -> None:
        seen.append(int(message.payload))

    for value in range(8):
        await accept_message(client._delivery, Message("t", str(value).encode()), callback)

    assert seen == list(range(8))
    assert client._delivery.callback_invocations == 8


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


async def test_delivery_fast_paths_complete_synchronously(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="iterator")

    def unexpected_timeout(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("timeouts must only be armed after queue saturation")

    monkeypatch.setattr(asyncio, "timeout", unexpected_timeout)
    message = Message(topic="fast", payload=b"x")
    assert client._delivery.accept(message, None) is None
    assert await anext(client.messages()) is message

    called = asyncio.Event()

    def callback(_message: Message) -> None:
        called.set()

    client._delivery.mode = "callback"
    assert client._delivery.accept(message, callback) is None
    assert called.is_set()
