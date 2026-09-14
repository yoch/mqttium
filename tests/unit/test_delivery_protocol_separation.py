"""Protocol admission and completion do not depend on application delivery."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, Message
from mqttium.enums import MQTTProtocolVersion
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind
from tests.support import (
    ScriptedBrokerTransport,
    accept_message,
    transport_factory,
    wait_until,
)


@pytest.mark.parametrize("protocol", (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5))
@pytest.mark.parametrize("qos", (0, 1))
async def test_publication_admission_preserves_a_full_iterator_queue(protocol, qos):
    broker = ScriptedBrokerTransport(protocol=protocol)
    client = AsyncClient("delivery-separation", protocol=protocol, max_pending_messages=1)
    client._transport_factory = transport_factory(broker)
    await client.connect("test")
    first = Message("incoming", b"first")
    await accept_message(client._delivery, first, None)
    entered = asyncio.Event()
    accept = client._delivery.accept

    def observe_accept(message, callback, property_wire_size=None):
        entered.set()
        return accept(message, callback, property_wire_size)

    client._delivery.accept = observe_accept
    stream = client.messages()
    try:
        broker.push_rx(PublishPacket("incoming", b"second", 0, False, False).encode(protocol))
        await asyncio.wait_for(entered.wait(), 1)
        receipt = await asyncio.wait_for(client.publish("outgoing", b"reply", qos=qos), 1)
        await wait_until(lambda: len(broker.publishes) == 1)
        assert client._delivery.messages_queue.qsize() == 1
        # The waiting publication charges no bytes until it is enqueued.
        assert client._delivery.pending_bytes == len("incomingfirst")
        if qos:
            # An ACK still on the transport is deliberately not read through
            # full delivery. Admission and receipt completion are distinct.
            assert not receipt.is_done()
        assert await anext(stream) is first
        assert (await asyncio.wait_for(anext(stream), 1)).payload == b"second"
        await asyncio.wait_for(receipt.wait(), 1)
        assert client._delivery.pending_bytes == 0
    finally:
        await stream.aclose()
        await client.disconnect()


async def test_failure_interrupts_reader_delivery_and_releases_its_reservation():
    broker = ScriptedBrokerTransport()
    client = AsyncClient(max_pending_messages=1)
    client._transport_factory = transport_factory(broker)
    await client.connect("test")
    first = Message("in", b"first")
    await accept_message(client._delivery, first, None)
    entered = asyncio.Event()
    accept = client._delivery.accept

    def observe_accept(message, callback, property_wire_size=None):
        entered.set()
        return accept(message, callback, property_wire_size)

    client._delivery.accept = observe_accept
    broker.push_rx(
        PublishPacket("in", b"second", 0, False, False).encode(MQTTProtocolVersion.MQTTv311)
    )
    await asyncio.wait_for(entered.wait(), 1)
    reader = client._reader_task
    assert reader is not None
    error = OSError("writer stopped")
    await client._writer_failed(error)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(reader, 1)
    assert client._disconnect_exc is error
    assert client._delivery.pending_bytes == len("infirst")
    stream = client.messages()
    assert await anext(stream) is first
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
    assert client._delivery.pending_bytes == 0
    await client.disconnect()


async def test_delivery_fence_does_not_wait_for_later_protocol_work():
    client = AsyncClient()
    client._engine._emit(EffectKind.MESSAGE, Message("in", b"accepted"))
    client._effect_pump.collect_from_engine()
    # This later wire operation cannot fit yet. The delivery collection's
    # already-satisfied protocol fence must not expand to include it.
    client._write_pump.max_messages = 1
    assert client._write_pump.try_enqueue(b"occupied", epoch=client._connection_epoch)
    client._engine._emit(EffectKind.SEND, b"later")
    client._effect_pump.collect_from_engine()
    await asyncio.wait_for(client._delivery_lane.drain(), 1)
    stream = client.messages()
    assert (await anext(stream)).payload == b"accepted"
    assert client._effect_pump.pending
    await stream.aclose()
    await client._force_close()


async def test_replaced_epoch_discards_unaccepted_delivery():
    client = AsyncClient()
    client._engine._emit(EffectKind.MESSAGE, Message("old", b"old"))
    client._effect_pump.collect_from_engine()
    assert client._delivery_lane.pending_count == 1
    await client._invalidate_connection_epoch()
    await client._delivery_lane.drain()
    assert client._delivery_lane.pending_count == 0
    assert client._delivery.messages_queue.empty()
    assert client._delivery.pending_bytes == 0
