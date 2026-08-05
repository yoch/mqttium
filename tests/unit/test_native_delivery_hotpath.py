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
