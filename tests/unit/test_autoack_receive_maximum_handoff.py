"""Receive Maximum must include auto-PUBACKs still inside an engine batch."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16, pack_utf8
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


def _publish(
    mid: int,
    *,
    qos: QoS = QoS.AT_LEAST_ONCE,
    dup: bool = False,
) -> bytes:
    return PublishPacket(
        topic="receive/window",
        payload=bytes((mid,)),
        qos=qos,
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


def test_pending_qos1_puback_counts_against_fresh_qos2() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(1))
    _feed(engine, _publish(2, qos=QoS.EXACTLY_ONCE))
    effects = engine.take_effects()

    messages = [effect.data.payload for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert messages == [b"\x01"]
    assert engine.state is ConnectionState.DISCONNECTED
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "Receive Maximum exceeded" in str(error)


def test_cross_qos_reuse_of_pending_autoack_mid_is_protocol_error() -> None:
    engine = _engine(receive_maximum=2)

    _feed(engine, _publish(7))
    _feed(engine, _publish(7, qos=QoS.EXACTLY_ONCE))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    messages = [effect.data.payload for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert messages == [b"\x07"]
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "reused by QoS 2 while QoS 1 PUBACK is pending" in str(error)


def test_qos2_duplicate_does_not_double_charge_pending_autoack_window() -> None:
    engine = _engine(receive_maximum=2)

    _feed(engine, _publish(10, qos=QoS.EXACTLY_ONCE))
    _feed(engine, _publish(11))
    _feed(engine, _publish(10, qos=QoS.EXACTLY_ONCE, dup=True))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    messages = [effect.data.payload for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert messages == [b"\x0a", b"\x0b"]
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_effect_handoff_releases_autoack_receive_maximum_slot() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(1))
    first = engine.take_effects()
    assert any(effect.kind is EffectKind.SEND_ACK for effect in first)
    assert engine.state is ConnectionState.CONNECTED

    _feed(engine, _publish(2))
    second = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert [effect.data.payload for effect in second if effect.kind is EffectKind.MESSAGE] == [
        b"\x02"
    ]
    assert any(effect.kind is EffectKind.SEND_ACK for effect in second)
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in second)


def test_duplicate_qos1_before_puback_handoff_does_not_consume_second_slot() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(7))
    _feed(engine, _publish(7, dup=True))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert len([effect for effect in effects if effect.kind is EffectKind.MESSAGE]) == 2
    assert len([effect for effect in effects if effect.kind is EffectKind.SEND_ACK]) == 2
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_autoack_receive_maximum_slot_is_held_until_handoff() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(1))
    assert engine._inbound_inflight == 1
    engine.take_effects()
    assert engine._inbound_inflight == 0


def test_pipelined_qos1_does_not_use_generic_publish_decode(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _engine(receive_maximum=8)
    for mid in range(1, 5):
        _feed(engine, _publish(mid))
    effects = engine.take_effects()

    assert calls == 0
    assert engine.state is ConnectionState.CONNECTED
    assert len([effect for effect in effects if effect.kind is EffectKind.MESSAGE]) == 4


def test_mqtt5_wildcard_is_rejected_before_receive_maximum() -> None:
    engine = _engine(receive_maximum=1)

    _feed(engine, _publish(1))
    remaining = pack_utf8("a/+/b") + pack_u16(2) + b"\x00" + b"x"
    _feed(engine, encode_frame(PacketType.PUBLISH, 0x02, remaining))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "wildcards" in str(error)
    assert not any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    messages = [effect.data.payload for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert messages == [b"\x01"]
