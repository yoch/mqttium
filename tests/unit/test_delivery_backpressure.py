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
    client._messages.put_nowait(Message(topic="full", payload=b"x"))

    with pytest.raises(MessageDeliveryError, match="Iterator delivery queue"):
        await client._apply_effect(
            EngineEffect(
                kind=EffectKind.MESSAGE,
                data=Message(topic="overflow", payload=b"x"),
            ),
            nowait=False,
        )


async def test_callback_queue_timeout_is_explicit_and_bounded() -> None:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=1,
        delivery_timeout=0.01,
        callback_shutdown_timeout=0.01,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(_message: Message) -> None:
        started.set()
        await release.wait()

    await client._enqueue_callback(slow, Message(topic="one", payload=b"x"))
    await started.wait()
    await client._enqueue_callback(slow, Message(topic="two", payload=b"x"))

    with pytest.raises(MessageDeliveryError, match="Callback delivery queue"):
        await client._enqueue_callback(slow, Message(topic="three", payload=b"x"))
    await client._shutdown_callback_worker(drain=False)


async def test_callback_worker_preserves_order() -> None:
    client = AsyncClient(max_pending_callbacks=8)
    seen: list[int] = []

    async def callback(value: int) -> None:
        await asyncio.sleep(0)
        seen.append(value)

    for value in range(8):
        await client._enqueue_callback(callback, value)
    await asyncio.wait_for(client._callback_queue.join(), timeout=1.0)

    assert seen == list(range(8))
    await client._shutdown_callback_worker(drain=False)


async def test_force_close_discards_all_old_connection_effects() -> None:
    client = AsyncClient(message_delivery="iterator")
    client._pending_effects.extend(
        [
            EngineEffect(
                kind=EffectKind.MESSAGE,
                data=Message(topic="old", payload=b"old"),
            ),
            EngineEffect(kind=EffectKind.PUBLISH_COMPLETE, data=7),
        ]
    )
    await client._force_close()
    assert not client._pending_effects


async def test_delivery_queue_fast_paths_avoid_timeout_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="iterator")

    async def unexpected_wait_for(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("wait_for must only be used after queue saturation")

    monkeypatch.setattr(asyncio, "wait_for", unexpected_wait_for)
    message = Message(topic="fast", payload=b"x")
    await client._put_message(message)
    assert client._messages.get_nowait() is message

    called = asyncio.Event()

    def callback(_message: Message) -> None:
        called.set()

    await client._enqueue_callback(callback, message)
    await asyncio.sleep(0)
    assert called.is_set()
    await client._shutdown_callback_worker(drain=False)
