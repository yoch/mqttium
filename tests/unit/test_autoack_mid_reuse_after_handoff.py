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


def _engine() -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="autoack-mid-reuse",
            protocol=MQTTProtocolVersion.MQTTv5,
            manual_ack=False,
            local_receive_maximum=1,
        )
    )
    engine.begin_connect()
    body = bytearray((0, 0))
    body.extend(encode_properties(None, CONNACK))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    return engine


def _publish(mid: int, payload: bytes, *, dup: bool) -> bytes:
    return PublishPacket(
        topic="receive/window",
        payload=payload,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=dup,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_qos1_mid_can_be_reused_after_effect_handoff() -> None:
    engine = _engine()

    _feed(engine, _publish(7, b"first", dup=False))
    first = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert [effect.data.payload for effect in first if effect.kind is EffectKind.MESSAGE] == [
        b"first"
    ]
    assert engine._inbound_inflight == 0

    _feed(engine, _publish(7, b"second", dup=True))
    second = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert [effect.data.payload for effect in second if effect.kind is EffectKind.MESSAGE] == [
        b"second"
    ]
    assert any(effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE for effect in second)
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in second)
    assert engine._inbound_inflight == 0
