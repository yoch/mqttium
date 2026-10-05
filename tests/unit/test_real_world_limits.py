"""MQTT 3.1.1 enforces only the inbound limits the application set explicitly.

An MQTT 3.1.1 broker is never told a Receive Maximum or a Maximum Packet Size,
so exceeding a default it could not know about is not a protocol violation.
MQTT 5 still advertises and enforces its defaults.
"""

from __future__ import annotations

import asyncio

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


def _publish(mid: int, payload: bytes, qos: QoS = QoS.AT_LEAST_ONCE) -> bytes:
    return PublishPacket(
        topic="zigbee/bridge", payload=payload, qos=qos, retain=True, dup=False, mid=mid
    ).encode(MQTTProtocolVersion.MQTTv311)


async def test_a_retained_message_above_16_mib_is_delivered() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("large-retained")
    client._transport_factory = transport_factory(broker)
    payload = b"x" * (17 * 1024 * 1024)
    try:
        await client.connect("broker")
        broker.push_rx(_publish(1, payload))
        async with asyncio.timeout(5):
            message = await anext(client.messages())
        assert len(message.payload) == len(payload)
        assert client.state is ConnectionState.CONNECTED
    finally:
        await client.disconnect()


async def test_more_than_100_unacknowledged_messages_keep_the_connection() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("many-unacked", manual_ack=True)
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        for mid in range(1, 301):
            broker.push_rx(_publish(mid, b"p"))
        received = []
        async with asyncio.timeout(5):
            async for message in client.messages():
                received.append(message)
                if len(received) == 300:
                    break
        assert client.state is ConnectionState.CONNECTED
        for message in received:
            await client.ack(message)
    finally:
        await client.disconnect()


async def test_an_explicit_limit_is_still_enforced() -> None:
    broker = ScriptedBrokerTransport()
    causes: list[BaseException | None] = []
    client = AsyncClient("explicit-limit", manual_ack=True, max_inbound_inflight=10)
    client.on_disconnect = causes.append
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        for mid in range(1, 12):
            broker.push_rx(_publish(mid, b"p"))
        await wait_until(lambda: causes != [])
        assert client.state is ConnectionState.DISCONNECTED
    finally:
        await client.disconnect()


def test_mqtt5_keeps_its_advertised_defaults() -> None:
    client = AsyncClient("v5-defaults", protocol=MQTTProtocolVersion.MQTTv5)
    config = client._engine.config
    assert config.max_inbound_inflight == 100
    assert config.maximum_packet_size == 16 * 1024 * 1024
    assert config.max_inbound_inflight_bytes is None


async def test_awaited_publish_larger_than_the_byte_budget_completes() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("large-publish", max_unacknowledged_bytes=1024)
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker")
        small = await client.publish("t/small", b"s", qos=1)
        large = await client.publish("t/large", b"x" * 100_000, qos=1)
        async with asyncio.timeout(5):
            await small.wait()
            await large.wait()
        assert [len(p.payload) for p in broker.publishes] == [1, 100_000]
    finally:
        await client.disconnect()


async def test_mqtt311_outbound_window_defaults_to_20() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("window-311")
    client._transport_factory = transport_factory(broker)
    explicit = AsyncClient("window-explicit", max_outbound_inflight=500)
    explicit._transport_factory = transport_factory(ScriptedBrokerTransport())
    try:
        await client.connect("broker")
        await explicit.connect("broker")
        assert client.stats().outbound.inflight_limit == 20
        assert explicit.stats().outbound.inflight_limit == 500
    finally:
        await client.disconnect()
        await explicit.disconnect()
