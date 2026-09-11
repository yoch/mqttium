"""Strict sole-pending MESSAGE fast-path invariants."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def msg(payload: bytes, *, persisted: bool = False) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="strict/x", payload=payload),
        requires_delivery_mark=persisted,
    )


async def stop(client: AsyncClient) -> None:
    await client._shutdown_callback_worker(drain=False)


async def test_sole_sync_message_runs_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []
    caller = asyncio.current_task()
    owners = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        owners.append(asyncio.current_task())

    effects = deque([msg(b"one")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert seen == [b"one"]
    assert owners == [caller]
    assert client._callback_queue.empty()
    assert client._callback_worker_task is None
    await stop(client)


async def test_following_non_message_effect_forces_worker() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []
    client.on_message = lambda message: seen.append(message.payload)
    effects = deque([msg(b"one"), EngineEffect(EffectKind.PINGRESP)])
    assert client._delivery.deliver_message_batch_inline(effects, client.on_message) == 1
    assert seen == []
    assert client._callback_queue.qsize() == 1
    await client._callback_queue.join()
    assert seen == [b"one"]
    await stop(client)


async def test_two_messages_keep_whole_run_worker_owned() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []
    callback = lambda message: seen.append(message.payload)
    effects = deque([msg(b"one"), msg(b"two")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 2
    assert seen == []
    assert client._callback_queue.qsize() == 2
    await client._callback_queue.join()
    assert seen == [b"one", b"two"]
    await stop(client)


async def test_async_and_both_stay_worker_owned() -> None:
    async def callback(message: Message) -> None:
        await asyncio.sleep(0)

    client = AsyncClient(message_delivery="callback")
    effects = deque([msg(b"async")])
    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1
    assert client._callback_queue.qsize() == 1
    await client._callback_queue.join()
    await stop(client)

    both = AsyncClient(message_delivery="both")
    seen: list[bytes] = []
    effects = deque([msg(b"both")])
    assert (
        both._delivery.deliver_message_batch_inline(
            effects, lambda message: seen.append(message.payload)
        )
        == 1
    )
    assert seen == []
    assert both._messages.qsize() == 1
    assert both._callback_queue.qsize() == 1
    await both._callback_queue.join()
    await stop(both)


async def test_persisted_message_stays_slow_path() -> None:
    client = AsyncClient(message_delivery="callback")
    effects = deque([msg(b"persisted", persisted=True)])
    assert client._delivery.deliver_message_batch_inline(effects, lambda _m: None) == 0
    await stop(client)


@pytest.mark.parametrize("state", ["draining", "closed"])
async def test_non_open_delivery_never_inlines(state: str) -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[bytes] = []
    client._delivery._callback_state = state
    with pytest.raises(MessageDeliveryError, match="Callback delivery is closing"):
        client._delivery.deliver_message_batch_inline(
            deque([msg(b"late")]), lambda message: seen.append(message.payload)
        )
    assert seen == []
    client._delivery._callback_state = "open"
    await stop(client)
