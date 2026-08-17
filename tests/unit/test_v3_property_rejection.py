from __future__ import annotations

import pytest

from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import (
    ConnectPacket,
    PublishPacket,
    SubscribeOptions,
    SubscribePacket,
    Subscription,
    UnsubscribePacket,
)
from mqttium.packets.publish import encode_publish_item
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


V3_PROTOCOLS = (MQTTProtocolVersion.MQTTv31, MQTTProtocolVersion.MQTTv311)


def _props() -> Properties:
    return Properties({"user_property": [("key", "value")]})


@pytest.mark.parametrize("protocol", V3_PROTOCOLS)
def test_engine_rejects_connect_properties_without_mutating_state(
    protocol: MQTTProtocolVersion,
) -> None:
    engine = ProtocolEngine(EngineConfig(protocol=protocol, connect_properties=_props()))
    with pytest.raises(ProtocolError, match="CONNECT properties require MQTT 5"):
        engine.begin_connect()
    assert engine.state is ConnectionState.NEW
    assert not engine.has_pending_effects


@pytest.mark.parametrize("protocol", V3_PROTOCOLS)
def test_engine_rejects_will_properties_without_mutating_state(
    protocol: MQTTProtocolVersion,
) -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=protocol,
            will=Message(topic="will/topic", payload=b"will"),
            will_properties=_props(),
        )
    )
    with pytest.raises(ProtocolError, match="Will properties require MQTT 5"):
        engine.begin_connect()
    assert engine.state is ConnectionState.NEW
    assert not engine.has_pending_effects


@pytest.mark.parametrize("protocol", V3_PROTOCOLS)
def test_engine_rejects_publish_properties_before_admission(
    protocol: MQTTProtocolVersion,
) -> None:
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    with pytest.raises(ProtocolError, match="PUBLISH properties require MQTT 5"):
        engine.queue_publish("topic", b"payload", qos=QoS.AT_LEAST_ONCE, properties=_props())
    assert engine.pending_outbound_messages == 0
    assert len(engine.packet_ids) == 0
    assert not engine.has_pending_effects


@pytest.mark.parametrize("protocol", V3_PROTOCOLS)
def test_engine_rejects_subscribe_properties_before_mid_allocation(
    protocol: MQTTProtocolVersion,
) -> None:
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    with pytest.raises(ProtocolError, match="SUBSCRIBE properties require MQTT 5"):
        engine.queue_subscribe("topic/#", properties=_props())
    assert len(engine.packet_ids) == 0
    assert not engine.has_pending_effects


@pytest.mark.parametrize("protocol", V3_PROTOCOLS)
def test_packet_views_reject_nonempty_v5_properties(protocol: MQTTProtocolVersion) -> None:
    props = _props()
    with pytest.raises(ProtocolError, match="CONNECT properties require MQTT 5"):
        ConnectPacket(client_id="c", protocol=protocol, properties=props).encode()
    with pytest.raises(ProtocolError, match="PUBLISH properties require MQTT 5"):
        PublishPacket(
            "topic", b"x", QoS.AT_MOST_ONCE, False, False, properties=props
        ).encode(protocol)
    with pytest.raises(ProtocolError, match="PUBLISH properties require MQTT 5"):
        encode_publish_item(
            "topic",
            b"x",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            mid=None,
            properties=props,
            protocol=protocol,
        )
    sub = Subscription("topic/#", SubscribeOptions())
    with pytest.raises(ProtocolError, match="SUBSCRIBE properties require MQTT 5"):
        SubscribePacket(1, (sub,), props).encode(protocol)
    with pytest.raises(ProtocolError, match="UNSUBSCRIBE properties require MQTT 5"):
        UnsubscribePacket(1, ("topic/#",), props).encode(protocol)


@pytest.mark.parametrize("protocol", V3_PROTOCOLS)
def test_empty_properties_remain_wire_equivalent_to_none(protocol: MQTTProtocolVersion) -> None:
    empty = Properties()
    assert PublishPacket(
        "topic", b"x", QoS.AT_MOST_ONCE, False, False, properties=empty
    ).encode(protocol) == PublishPacket(
        "topic", b"x", QoS.AT_MOST_ONCE, False, False
    ).encode(protocol)
    sub = Subscription("topic/#", SubscribeOptions())
    assert SubscribePacket(1, (sub,), empty).encode(protocol) == SubscribePacket(
        1, (sub,)
    ).encode(protocol)
    assert UnsubscribePacket(1, ("topic/#",), empty).encode(
        protocol
    ) == UnsubscribePacket(1, ("topic/#",)).encode(protocol)
