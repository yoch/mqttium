"""Delivered properties can be forwarded: no connection-local Topic Alias."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Properties
from tests.support import ScriptedBrokerTransport, transport_factory

V5 = MQTTProtocolVersion.MQTTv5


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
async def test_aliased_qos_publish_is_delivered_without_its_alias(qos: QoS) -> None:
    broker = ScriptedBrokerTransport(protocol=V5)
    client = AsyncClient("aliases", protocol=V5, topic_alias_maximum=4)
    client._transport_factory = transport_factory(broker)
    properties = Properties({"topic_alias": 2, "user_property": [("k", "v")]})
    try:
        await client.connect("broker")
        for mid, topic in ((1, "plant/line/1"), (2, "")):
            broker.push_rx(
                PublishPacket(
                    topic=topic,
                    payload=b"x",
                    qos=qos,
                    retain=False,
                    dup=False,
                    mid=mid,
                    properties=properties,
                ).encode(V5)
            )
        stream = client.messages()
        async with asyncio.timeout(2):
            received = [await anext(stream), await anext(stream)]
        for message in received:
            assert message.topic == "plant/line/1"
            assert message.properties is not None
            assert message.properties.get("topic_alias") is None
            assert message.properties.get("user_property") == (("k", "v"),)
    finally:
        await client.disconnect()
