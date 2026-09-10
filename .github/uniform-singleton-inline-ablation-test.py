"""Focused invariants for the idle-sync singleton callback ablation."""

from __future__ import annotations

import asyncio
from collections import deque

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def message(payload: bytes) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="ablation/x", payload=payload),
        requires_delivery_mark=False,
    )


async def stop(client: AsyncClient) -> None:
    await client._shutdown_callback_worker(drain=False)


async def test_idle_sync_singleton_effect_runs_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(msg: Message) -> None:
        seen.append(msg.payload)

    effects = deque([message(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == [b"one"]
    assert client._callback_queue.empty()
    assert client._callback_worker_task is None
    await stop(client)


async def test_single_message_before_non_message_runs_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(msg: Message) -> None:
        seen.append(msg.payload)

    effects = deque([message(b"one"), EngineEffect(EffectKind.PINGRESP)])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == [b"one"]
    assert client._callback_queue.empty()
    await stop(client)


async def test_two_consecutive_sync_messages_stay_on_worker() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(msg: Message) -> None:
        seen.append(msg.payload)

    effects = deque([message(b"one"), message(b"two")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 2
    assert seen == []
    assert client._callback_queue.qsize() == 2
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one", b"two"]
    await stop(client)


async def test_async_singleton_stays_on_worker() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    async def callback(msg: Message) -> None:
        seen.append(msg.payload)

    effects = deque([message(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == []
    assert client._callback_queue.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one"]
    await stop(client)


async def test_both_mode_singleton_keeps_worker_ownership() -> None:
    client = AsyncClient(message_delivery="both")
    seen: list[bytes] = []

    def callback(msg: Message) -> None:
        seen.append(msg.payload)

    effects = deque([message(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == []
    assert client._callback_queue.qsize() == 1
    assert client._delivery.messages_queue.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one"]
    await stop(client)
