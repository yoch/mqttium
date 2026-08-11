"""Regression coverage for critical protocol follow-up findings."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import NotConnectedError, PacketTooLargeError, ProtocolError
from mqttium.packets import SubscribeOptions, encode_disconnect, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack_v5(properties: Properties | None = None) -> bytes:
    body = bytearray((0, 0))
    body.extend(encode_properties(properties, CONNACK))
    return encode_frame(PacketType.CONNACK, 0, body)


def _connected(
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5,
) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="followup", protocol=protocol))
    engine.begin_connect()
    if protocol is MQTTProtocolVersion.MQTTv5:
        _feed(engine, _connack_v5())
    else:
        _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    return engine


def _ack_frame(packet_type: PacketType, mid: int, reasons: list[int]) -> bytes:
    body = bytes((mid >> 8, mid & 0xFF, 0)) + bytes(reasons)
    return encode_frame(packet_type, 0, body)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
def test_pingresp_rejects_nonzero_remaining_length(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)

    _feed(engine, b"\xd0\x01\xff")
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.PINGRESP for effect in effects)


@pytest.mark.parametrize("topic", ["", "bad/#", "bad/+"])
def test_connect_rejects_invalid_will_topic_before_state_mutation(topic: str) -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="will-topic", will=Message(topic=topic, payload=b"bye"))
    )

    with pytest.raises(ProtocolError, match="topic"):
        engine.begin_connect()

    assert engine.state is ConnectionState.NEW
    assert engine.take_effects() == []


def test_shared_subscription_rejects_no_local_before_mid_allocation() -> None:
    engine = _connected()

    with pytest.raises(ProtocolError, match="No Local.*shared"):
        engine.queue_subscribe(
            (("$share/group/topic", SubscribeOptions(qos=QoS.AT_MOST_ONCE, no_local=True)),)
        )

    assert not engine._pending_sub_mids
    assert len(engine.packet_ids) == 0
    assert engine.take_effects() == []


def test_suback_reason_count_must_match_request() -> None:
    engine = _connected()
    mid = engine.queue_subscribe(["one", "two"])
    engine.take_effects()

    _feed(engine, _ack_frame(PacketType.SUBACK, mid, [0]))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.SUBACK for effect in effects)
    assert mid in engine._pending_sub_mids
    assert mid in engine._pending_sub_requests


def test_cross_type_subscription_ack_does_not_release_mid() -> None:
    engine = _connected()
    mid = engine.queue_unsubscribe("topic")
    engine.take_effects()

    _feed(engine, _ack_frame(PacketType.SUBACK, mid, [0]))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.SUBACK for effect in effects)
    assert mid in engine._pending_sub_mids
    assert mid in engine._pending_sub_requests


def test_matching_subscription_ack_consumes_request_shape() -> None:
    engine = _connected()
    mid = engine.queue_subscribe(["one", "two"])
    engine.take_effects()

    _feed(engine, _ack_frame(PacketType.SUBACK, mid, [0, 1]))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.SUBACK for effect in effects)
    assert mid not in engine._pending_sub_mids
    assert mid not in engine._pending_sub_requests


def test_empty_client_id_requires_assigned_identifier_in_success_connack() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()

    _feed(engine, _connack_v5())
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.CONNACK for effect in effects)


def test_empty_client_id_accepts_nonempty_assigned_identifier() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    properties = Properties()
    properties.set("assigned_client_identifier", "server-assigned")

    _feed(engine, _connack_v5(properties))

    assert engine.state is ConnectionState.CONNECTED
    assert any(effect.kind is EffectKind.CONNACK for effect in engine.take_effects())


def test_server_disconnect_must_not_carry_session_expiry_interval() -> None:
    engine = _connected()
    properties = Properties()
    properties.set("session_expiry_interval", 30)

    _feed(
        engine,
        encode_disconnect(0, MQTTProtocolVersion.MQTTv5, properties),
    )
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_pingreq_honors_negotiated_maximum_packet_size() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="packet-size", protocol=MQTTProtocolVersion.MQTTv5)
    )
    engine.begin_connect()
    properties = Properties()
    properties.set("maximum_packet_size", 1)
    _feed(engine, _connack_v5(properties))
    engine.take_effects()

    with pytest.raises(PacketTooLargeError):
        engine.queue_ping()

    assert engine.state is ConnectionState.CONNECTED
    assert engine.take_effects() == []


def test_mqtt311_auth_tears_down_instead_of_staying_connected() -> None:
    engine = _connected(MQTTProtocolVersion.MQTTv311)

    _feed(engine, encode_frame(PacketType.AUTH, 0, b""))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.SEND for effect in effects)


def test_manual_ack_requires_active_connection_before_store_mutation() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="ack", manual_ack=True))
    engine.state = ConnectionState.DISCONNECTED

    with pytest.raises(NotConnectedError, match="active connection"):
        engine.ack(7)
