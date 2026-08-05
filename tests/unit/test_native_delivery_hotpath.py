from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import QoS
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


async def _apply_message(client: AsyncClient, qos: QoS) -> None:
    client.on_message = lambda _message: None
    await client._apply_effect(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=7),
        ),
        nowait=False,
    )
    await client._callback_queue.join()
    await client._shutdown_callback_worker(drain=False)


@pytest.mark.asyncio
async def test_auto_acked_qos1_delivery_skips_persistence_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="callback")
    marked: list[int] = []
    monkeypatch.setattr(client._engine, "mark_inbound_delivered", marked.append)

    await _apply_message(client, QoS.AT_LEAST_ONCE)

    assert marked == []


@pytest.mark.asyncio
async def test_qos2_delivery_keeps_persistence_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="callback")
    marked: list[int] = []
    monkeypatch.setattr(client._engine, "mark_inbound_delivered", marked.append)

    await _apply_message(client, QoS.EXACTLY_ONCE)

    assert marked == [7]


@pytest.mark.asyncio
async def test_manual_ack_qos1_delivery_keeps_persistence_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(message_delivery="callback", manual_ack=True)
    marked: list[int] = []
    monkeypatch.setattr(client._engine, "mark_inbound_delivered", marked.append)

    await _apply_message(client, QoS.AT_LEAST_ONCE)

    assert marked == [7]


@pytest.mark.asyncio
async def test_small_callback_delivery_applies_inline() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[Message] = []
    client.on_message = seen.append
    message = Message(topic="hot/path", payload=b"payload")

    applied = client._apply_effect_inline(
        EngineEffect(kind=EffectKind.MESSAGE, data=message),
        client._connection_epoch,
    )

    assert applied is True
    await client._callback_queue.join()
    assert seen == [message]
    await client._shutdown_callback_worker(drain=False)


def test_inline_both_delivery_is_atomic_when_one_queue_is_full() -> None:
    client = AsyncClient(
        message_delivery="both",
        max_pending_callbacks=1,
        max_pending_messages=1,
    )
    client.on_message = lambda _message: None
    sentinel = (lambda: None, (), None)
    client._callback_queue.put_nowait(sentinel)

    applied = client._apply_effect_inline(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(topic="hot/path", payload=b"payload"),
        ),
        client._connection_epoch,
    )

    assert applied is False
    assert client._messages.empty()
    assert client._callback_queue.get_nowait() is sentinel
    client._callback_queue.task_done()


def test_inline_delivery_defers_messages_requiring_persistence_mark() -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None

    applied = client._apply_effect_inline(
        EngineEffect(
            kind=EffectKind.MESSAGE,
            data=Message(
                topic="hot/path",
                payload=b"payload",
                qos=QoS.EXACTLY_ONCE,
                mid=7,
            ),
        ),
        client._connection_epoch,
    )

    assert applied is False
    assert client._callback_queue.empty()
