"""Two iterators consumed at once would split the stream between them."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MQTTError
from mqttium.packets import PublishPacket
from mqttium.enums import MQTTProtocolVersion, QoS
from tests.support import ScriptedBrokerTransport, transport_factory


def _qos0(payload: bytes) -> bytes:
    return PublishPacket(
        topic="t", payload=payload, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    ).encode(MQTTProtocolVersion.MQTTv311)


async def test_second_waiting_iterator_is_refused() -> None:
    client = AsyncClient("two-consumers")
    broker = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        first = asyncio.create_task(anext(client.messages()))
        await asyncio.sleep(0)
        with pytest.raises(MQTTError, match="already being consumed"):
            await anext(client.messages())
        broker.push_rx(_qos0(b"one"))
        assert (await asyncio.wait_for(first, 2)).payload == b"one"
    finally:
        await client.disconnect()


async def test_sequential_iterators_stay_allowed() -> None:
    client = AsyncClient("sequential")
    broker = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        broker.push_rx(_qos0(b"a") + _qos0(b"b"))
        assert (await asyncio.wait_for(anext(client.messages()), 2)).payload == b"a"
        assert (await asyncio.wait_for(anext(client.messages()), 2)).payload == b"b"
        waiting = asyncio.create_task(anext(client.messages()))
        await asyncio.sleep(0)
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        # A cancelled waiter no longer counts as a consumer.
        again = asyncio.create_task(anext(client.messages()))
        await asyncio.sleep(0)
        broker.push_rx(_qos0(b"c"))
        assert (await asyncio.wait_for(again, 2)).payload == b"c"
    finally:
        await client.disconnect()
