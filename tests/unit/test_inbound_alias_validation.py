"""Regression coverage for inbound Topic Alias validation ordering."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="alias-validation",
            protocol=MQTTProtocolVersion.MQTTv5,
            topic_alias_maximum=10,
        )
    )
    engine.begin_connect()
    body = bytearray((0, 0))
    body.extend(encode_properties(None, CONNACK))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    return engine


def test_invalid_topic_does_not_establish_inbound_alias() -> None:
    engine = _connected_engine()
    properties = Properties()
    properties.set("topic_alias", 1)

    wire = PublishPacket(
        topic="bad/#",
        payload=b"payload",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)
    _feed(engine, wire)
    effects = engine.take_effects()

    assert engine.inbound._aliases == {}
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.MESSAGE for effect in effects)


def test_valid_topic_still_establishes_inbound_alias() -> None:
    engine = _connected_engine()
    properties = Properties()
    properties.set("topic_alias", 1)

    first = PublishPacket(
        topic="valid/topic",
        payload=b"first",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)
    _feed(engine, first)
    engine.take_effects()

    assert engine.inbound._aliases == {1: "valid/topic"}

    second = PublishPacket(
        topic="",
        payload=b"second",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)
    _feed(engine, second)
    effects = engine.take_effects()

    message = next(effect.data for effect in effects if effect.kind is EffectKind.MESSAGE)
    assert message.topic == "valid/topic"
    assert message.payload == b"second"
