"""Receive Maximum must include auto-PUBACKs still inside an engine batch."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _engine(receive_maximum: int = 1) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="autoack-window",
            protocol=MQTTProtocolVersion.MQTTv5,
            manual_ack=False,
            local_receive_maximum=receive_maximum,
        )
    )
    engine.begin_connect()
    body = bytearray((0, 0))
    body.extend(encode_properties(None, CONNACK))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    return engine


def _publish(mid: int, *, dup: bool = False) -> bytes:
    return PublishPacket(
        topic="receive/window",
        payload=bytes((mid,)),
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=dup,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_pipelined_qos1_exceeding_receive_maximum_is_refused_before_delivery() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(1))
    _feed(engine, _publish(2))
    effects = engine.take_effects()

    messages = [effect.data.payload for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert messages == [b"\x01"]
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_effect_handoff_releases_autoack_receive_maximum_slot() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(1))
    first = engine.take_effects()
    assert any(effect.kind is EffectKind.SEND for effect in first)
    assert engine.state is ConnectionState.CONNECTED

    _feed(engine, _publish(2))
    second = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert [effect.data.payload for effect in second if effect.kind is EffectKind.MESSAGE] == [
        b"\x02"
    ]
    assert any(effect.kind is EffectKind.SEND for effect in second)
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in second)


def test_duplicate_qos1_before_puback_handoff_does_not_consume_second_slot() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(7))
    _feed(engine, _publish(7, dup=True))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert len([effect for effect in effects if effect.kind is EffectKind.MESSAGE]) == 2
    assert len([effect for effect in effects if effect.kind is EffectKind.SEND]) == 2
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
