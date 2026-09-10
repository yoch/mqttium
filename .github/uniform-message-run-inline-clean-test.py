"""Focused invariants for the clean singleton-message-run sync-inline policy."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def effect(payload: bytes, *, persisted: bool = False) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="message-run/x", payload=payload),
        requires_delivery_mark=persisted,
    )


async def stop(client: AsyncClient) -> None:
    await client._shutdown_callback_worker(drain=False)


async def test_idle_sync_singleton_message_run_executes_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one"), EngineEffect(EffectKind.PINGRESP)])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == [b"one"]
    assert client._callback_queue.empty()
    await stop(client)


async def test_two_eligible_messages_make_whole_run_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one"), effect(b"two"), EngineEffect(EffectKind.PINGRESP)])
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


async def test_both_mode_stays_worker_owned() -> None:
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


async def test_direct_qos0_singleton_remains_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    message = Message(topic="message-run/x", payload=b"qos0")
    assert client._delivery.deliver_callback_messages_inline([message], callback)
    assert seen == []
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert seen == [b"qos0"]
    await stop(client)


async def test_persisted_singleton_stays_slow_path() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    effects = deque([effect(b"one", persisted=True)])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 0
    assert seen == []
    await stop(client)


async def test_reentrant_delivery_falls_back_to_worker() -> None:
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


@pytest.mark.parametrize("state", ["draining", "closed"])
async def test_non_open_callback_state_never_executes_inline(state: str) -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []

    def callback(message: Message) -> None:
        seen.append(message.payload)

    client._delivery._callback_state = state
    with pytest.raises(MessageDeliveryError, match="Callback delivery is closing"):
        client._delivery.deliver_message_batch_inline(deque([effect(b"late")]), callback)
    assert seen == []
    client._delivery._callback_state = "open"
    await stop(client)
