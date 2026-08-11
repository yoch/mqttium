"""Regression coverage for broker Maximum Packet Size on automatic ACKs."""

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


def _connected_engine(maximum_packet_size: int) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="ack-size", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    properties = Properties()
    properties.set("maximum_packet_size", maximum_packet_size)
    body = bytearray((0, 0))
    body.extend(encode_properties(properties, CONNACK))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    return engine


def _qos1_publish(mid: int = 7) -> bytes:
    return PublishPacket(
        topic="inbound/topic",
        payload=b"payload",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_automatic_puback_is_not_emitted_above_broker_packet_limit() -> None:
    engine = _connected_engine(maximum_packet_size=3)

    _feed(engine, _qos1_publish())
    effects = engine.take_effects()

    assert not any(effect.kind is EffectKind.SEND for effect in effects)
    assert not any(effect.kind is EffectKind.MESSAGE for effect in effects)
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "maximum_packet_size 3" in error


def test_automatic_puback_at_exact_broker_packet_limit_is_emitted() -> None:
    engine = _connected_engine(maximum_packet_size=4)

    _feed(engine, _qos1_publish())
    effects = engine.take_effects()

    sent = [effect.data for effect in effects if effect.kind is EffectKind.SEND]
    assert sent == [b"\x40\x02\x00\x07"]
    assert any(effect.kind is EffectKind.MESSAGE for effect in effects)
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
