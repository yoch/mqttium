"""Focused invariants for the sole-pending-message sync-inline ablation."""

from __future__ import annotations

import asyncio
from collections import deque

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def effect(payload: bytes, *, persisted: bool = False) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="ablation/x", payload=payload),
        requires_delivery_mark=persisted,
    )


async def stop(client: AsyncClient) -> None:
    await client._shutdown_callback_worker(drain=False)


async def test_idle_sync_sole_pending_message_executes_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == [b"one"]
    assert client._callback_queue.empty()
    assert client._callback_worker_task is None
    await stop(client)


async def test_single_message_with_any_pending_tail_stays_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one"), EngineEffect(EffectKind.PINGRESP)])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == []
    assert client._callback_queue.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one"]
    await stop(client)


async def test_two_consecutive_messages_stay_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one"), effect(b"two")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 2
    assert seen == []
    assert client._callback_queue.qsize() == 2
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one", b"two"]
    await stop(client)


async def test_async_singleton_stays_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    async def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == []
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one"]
    await stop(client)


async def test_both_mode_singleton_stays_worker_owned() -> None:
    client = AsyncClient(message_delivery="both")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == []
    assert client._callback_queue.qsize() == 1
    assert client._messages.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one"]
    await stop(client)


async def test_persisted_singleton_stays_on_slow_path() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one", persisted=True)])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 0
    assert seen == []
    assert client._callback_queue.empty()
    await stop(client)


async def test_direct_qos0_singleton_remains_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    message = Message(topic="ablation/x", payload=b"one")
    assert client._delivery.deliver_callback_messages_inline([message], callback)
    assert seen == []
    assert client._callback_queue.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"one"]
    await stop(client)


async def test_inline_callback_reentrancy_falls_back_to_worker() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        if message.payload == b"outer":
            nested = deque([effect(b"nested")])
            assert client._delivery.deliver_message_batch_inline(nested, callback) == 1
            assert seen == [b"outer"]

    effects = deque([effect(b"outer")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == [b"outer"]
    assert client._callback_queue.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"outer", b"nested"]
    await stop(client)
