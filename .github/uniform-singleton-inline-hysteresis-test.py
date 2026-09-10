"""Focused invariants for singleton-inline before persistent worker takeover."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def effect(payload: bytes) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="hysteresis/x", payload=payload),
        requires_delivery_mark=False,
    )


async def stop(client: AsyncClient) -> None:
    await client._shutdown_callback_worker(drain=False)


async def test_serial_sync_stays_inline_before_worker_exists() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    for payload in (b"a", b"b", b"c"):
        effects = deque([effect(payload)])
        assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
        assert client._callback_worker_task is None
    assert seen == [b"a", b"b", b"c"]
    await stop(client)


async def test_burst_creates_worker_and_later_singleton_never_returns_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    burst = deque([effect(b"a"), effect(b"b")])
    assert client._delivery.deliver_message_batch_inline(burst, callback) == 2
    worker = client._callback_worker_task
    assert worker is not None
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"a", b"b"]
    assert client._callback_worker_task is worker

    singleton = deque([effect(b"c")])
    assert client._delivery.deliver_message_batch_inline(singleton, callback) == 1
    assert seen == [b"a", b"b"]
    assert client._callback_queue.qsize() == 1
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"a", b"b", b"c"]
    assert client._callback_worker_task is worker
    await stop(client)


async def test_async_singleton_starts_worker_and_sync_successor_is_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    async def async_callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"async")])
    assert client._delivery.deliver_message_batch_inline(effects, async_callback) == 1
    worker = client._callback_worker_task
    assert worker is not None
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"async"]

    def sync_callback(message: Message) -> None:
        seen.append(message.payload)

    successor = deque([effect(b"sync")])
    assert client._delivery.deliver_message_batch_inline(successor, sync_callback) == 1
    assert seen == [b"async"]
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"async", b"sync"]
    assert client._callback_worker_task is worker
    await stop(client)


async def test_direct_qos0_starts_worker_and_disables_later_effect_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    direct = Message(topic="hysteresis/x", payload=b"qos0")
    assert client._delivery.deliver_callback_messages_inline([direct], callback)
    worker = client._callback_worker_task
    assert worker is not None
    await asyncio.wait_for(client._callback_queue.join(), 1)

    later = deque([effect(b"qos1")])
    assert client._delivery.deliver_message_batch_inline(later, callback) == 1
    assert seen == [b"qos0"]
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"qos0", b"qos1"]
    assert client._callback_worker_task is worker
    await stop(client)


@pytest.mark.parametrize("state", ["draining", "closed"])
async def test_non_open_callback_state_never_falls_back_to_inline(state: str) -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    client._delivery._callback_state = state
    effects = deque([effect(b"late")])
    with pytest.raises(MessageDeliveryError, match="Callback delivery is closing"):
        client._delivery.deliver_message_batch_inline(effects, callback)
    assert seen == []
    assert client._callback_worker_task is None
    client._delivery._callback_state = "open"
    await stop(client)
