"""Recovered auto-QoS1 PUBACKs retain Receive Maximum ownership until handoff."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _publish(mid: int, *, qos: QoS = QoS.AT_LEAST_ONCE, dup: bool = False) -> bytes:
    return PublishPacket(
        topic="recover/window",
        payload=bytes((mid,)),
        qos=qos,
        retain=False,
        dup=dup,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv5)


def _recovered_engine(receive_maximum: int) -> ProtocolEngine:
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=99,
            topic="resume/state",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    store.put_in(
        InboundMessage(
            mid=7,
            topic="recover/window",
            payload=b"old",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBACK,
            delivered=True,
        )
    )
    engine = ProtocolEngine(
        EngineConfig(
            client_id="recovered-autoack-window",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            manual_ack=False,
            local_receive_maximum=receive_maximum,
        ),
        store=store,
    )
    engine.begin_connect()
    engine.take_effects()
    body = bytearray((1, 0))
    body.extend(encode_properties(None, CONNACK))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    assert engine._inbound_inflight == 1
    return engine


def test_recovered_autoack_puback_holds_receive_maximum_slot_until_handoff() -> None:
    engine = _recovered_engine(receive_maximum=1)

    _feed(engine, _publish(7, dup=True))
    assert engine._inbound_inflight == 1
    assert engine.store.get_in(7) is None

    _feed(engine, _publish(8))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert not [effect for effect in effects if effect.kind is EffectKind.MESSAGE]
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "Receive Maximum exceeded" in error


def test_recovered_autoack_mid_cannot_be_reused_by_qos2_before_handoff() -> None:
    engine = _recovered_engine(receive_maximum=2)

    _feed(engine, _publish(7, dup=True))
    _feed(engine, _publish(7, qos=QoS.EXACTLY_ONCE))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    error = next(effect.data for effect in effects if effect.kind is EffectKind.PROTOCOL_ERROR)
    assert "reused by QoS 2 while QoS 1 PUBACK is pending" in error


def test_recovered_autoack_slot_is_released_at_effect_handoff() -> None:
    engine = _recovered_engine(receive_maximum=1)

    _feed(engine, _publish(7, dup=True))
    first = engine.take_effects()

    assert any(effect.kind is EffectKind.SEND for effect in first)
    assert engine._inbound_inflight == 0
    assert engine.state is ConnectionState.CONNECTED

    _feed(engine, _publish(8))
    second = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert [effect.data.payload for effect in second if effect.kind is EffectKind.MESSAGE] == [
        b"\x08"
    ]
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in second)
