"""Synchronous callbacks that publish from the delivering reader.

The reader hands one decoded lot to the application before decoding more, so
a responder callback runs while the lane is draining adjacent ``MESSAGE``
effects. Its reentrant ``publish_nowait`` must not observe a held engine lock,
must keep wire order per delivery, and must settle its receipts normally.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


@pytest.fixture(params=["normal", "eager"])
async def task_factory(request):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if request.param == "eager":
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield
    finally:
        loop.set_task_factory(previous)


def _inbound(protocol: MQTTProtocolVersion, index: int, qos: QoS) -> bytes:
    return PublishPacket(
        topic=f"request/{index}",
        payload=index.to_bytes(2, "big"),
        qos=qos,
        retain=False,
        dup=False,
        mid=None if qos is QoS.AT_MOST_ONCE else 100 + index,
    ).encode(protocol)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("inbound_qos", [QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE])
async def test_responder_callback_publishes_from_one_delivery_lot(
    task_factory, protocol: MQTTProtocolVersion, inbound_qos: QoS
) -> None:
    del task_factory
    broker = ScriptedBrokerTransport(protocol=protocol)
    client = AsyncClient("responder", protocol=protocol, message_delivery="callback")
    client._transport_factory = transport_factory(broker)
    receipts = []
    seen: list[int] = []

    def respond(message) -> None:
        assert not client._engine_lock.locked()
        assert not client._effect_pump.lock.locked()
        index = int.from_bytes(message.payload, "big")
        seen.append(index)
        receipts.append(client.publish_nowait(f"reply/{index}/qos1", message.payload, qos=1))
        client.publish_nowait(f"reply/{index}/qos0", message.payload, qos=0)

    client.on_message = respond
    try:
        await client.connect("fake")
        broker.publishes.clear()
        # One rx chunk carries the whole lot: adjacent MESSAGE effects drain
        # from a single reader-owned collection.
        broker.push_rx(b"".join(_inbound(protocol, index, inbound_qos) for index in range(5)))

        await wait_until(lambda: len(broker.publishes) == 10)
        for receipt in receipts:
            await receipt.wait()

        assert seen == list(range(5))
        # The burst was collected as one lot of five adjacent MESSAGE effects.
        assert client._delivery_lane.high_water == 5
        assert [packet.topic for packet in broker.publishes] == [
            f"reply/{index}/{qos}" for index in range(5) for qos in ("qos1", "qos0")
        ]
        stats = client.stats()
        assert stats.delivery.callback_invocations == 5
        assert stats.outbound.pending_messages == 0
        await wait_until(lambda: client.stats().inbound.inflight == 0)
        assert client.is_connected
    finally:
        await client.disconnect()
