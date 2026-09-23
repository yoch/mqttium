from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubRecPacket, PubRelPacket, PublishPacket, encode_frame
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect(engine: ProtocolEngine, protocol: MQTTProtocolVersion) -> None:
    engine.begin_connect()
    engine.take_effects()
    body = bytes((0, 0, 0)) if protocol == MQTTProtocolVersion.MQTTv5 else bytes((0, 0))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED


def _send_qos2(engine: ProtocolEngine, protocol: MQTTProtocolVersion, *, dup: bool) -> None:
    _feed(
        engine,
        PublishPacket(
            topic="qos2/phase",
            payload=b"x",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=dup,
            mid=7,
        ).encode(protocol),
    )


@pytest.mark.parametrize(
    "protocol",
    [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5],
)
def test_qos2_publish_after_pubrel_is_protocol_error(protocol: MQTTProtocolVersion) -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="qos2-phase", protocol=protocol, manual_ack=True)
    )
    _connect(engine, protocol)

    _send_qos2(engine, protocol, dup=False)
    first = engine.take_effects()
    assert any(effect.kind is EffectKind.SEND_ACK for effect in first)
    assert engine.store.in_meta(7).state is InboundQoSState.WAIT_PUBREL

    _feed(engine, PubRelPacket(mid=7).encode(protocol))
    engine.take_effects()
    assert engine.store.in_meta(7).state is InboundQoSState.WAIT_USER_ACK

    _send_qos2(engine, protocol, dup=True)
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert engine.store.in_meta(7).state is InboundQoSState.WAIT_USER_ACK
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(
        effect.kind is EffectKind.SEND_ACK
        and effect.data == PubRecPacket(mid=7).encode(protocol)
        for effect in effects
    )


def test_duplicate_publish_before_pubrel_still_repeats_pubrec() -> None:
    protocol = MQTTProtocolVersion.MQTTv5
    engine = ProtocolEngine(
        EngineConfig(client_id="qos2-phase", protocol=protocol, manual_ack=True)
    )
    _connect(engine, protocol)

    _send_qos2(engine, protocol, dup=False)
    engine.take_effects()
    _send_qos2(engine, protocol, dup=True)
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert engine.store.in_meta(7).state is InboundQoSState.WAIT_PUBREL
    assert any(
        effect.kind is EffectKind.SEND_ACK
        and effect.data == PubRecPacket(mid=7).encode(protocol)
        for effect in effects
    )
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_duplicate_pubrel_while_waiting_for_user_ack_remains_a_noop() -> None:
    protocol = MQTTProtocolVersion.MQTTv5
    engine = ProtocolEngine(
        EngineConfig(client_id="qos2-phase", protocol=protocol, manual_ack=True)
    )
    _connect(engine, protocol)

    _send_qos2(engine, protocol, dup=False)
    engine.take_effects()
    _feed(engine, PubRelPacket(mid=7).encode(protocol))
    engine.take_effects()
    _feed(engine, PubRelPacket(mid=7).encode(protocol))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.CONNECTED
    assert engine.store.in_meta(7).state is InboundQoSState.WAIT_USER_ACK
    assert effects == []
