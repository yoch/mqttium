"""Hardening tests: flags, MID=0, session resume, inbound RM, offline limits."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MalformedPacketError
from mqttium.packets import DisconnectPacket, PublishPacket, encode_frame
from mqttium.protocol.engine import (
    DisconnectInfo,
    EffectKind,
    EngineConfig,
    ProtocolEngine,
)
from mqttium.protocol.validate import validate_raw_packet
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def test_validate_pubrel_flags() -> None:
    raw = RawPacket(PacketType.PUBREL, 0x00, b"\x00\x01")
    try:
        validate_raw_packet(raw)
        raise AssertionError("expected malformed")
    except MalformedPacketError:
        pass
    validate_raw_packet(RawPacket(PacketType.PUBREL, 0x02, b"\x00\x01"))


def test_mid_zero_rejected() -> None:
    # topic "t", mid 0, payload "x", qos1 flags
    remaining = b"\x00\x01t\x00\x00x"
    try:
        PublishPacket.decode(0x02, remaining)
        raise AssertionError("expected malformed")
    except MalformedPacketError:
        pass


def test_disconnect_packet_parsed() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    body = bytes((0x00, 0x00)) + encode_properties(Properties(), "CONNACK")
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    remaining = bytes((0x8E,)) + encode_properties(Properties(), "DISCONNECT")
    packet = DisconnectPacket.decode(remaining, MQTTProtocolVersion.MQTTv5)
    assert packet.reason_code == 0x8E
    wire = encode_frame(PacketType.DISCONNECT, 0, remaining)
    _feed(engine, wire)
    effects = engine.take_effects()
    disc_fx = [e for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert disc_fx
    info = disc_fx[0].data
    assert isinstance(info, DisconnectInfo)
    assert info.from_broker is True
    assert info.reason_code == 0x8E


def test_session_resume_uses_clean_start_false() -> None:
    props = Properties()
    props.set("session_expiry_interval", 3600)
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=True,
            connect_properties=props,
        )
    )
    engine.begin_connect()
    body = bytes((0x00, 0x00)) + encode_properties(Properties(), "CONNACK")
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()
    assert engine._prefer_session_resume is True
    engine.notify_transport_closed()
    engine.take_effects()
    wire = engine.begin_connect()
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    rem = raw.remaining
    nlen = int.from_bytes(rem[0:2], "big")
    flags = rem[2 + nlen + 1]
    assert (flags & 0x02) == 0


def test_inbound_receive_maximum() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", local_receive_maximum=1, manual_ack=True))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    pub1 = PublishPacket(topic="a", payload=b"1", qos=1, retain=False, dup=False, mid=1).encode()
    _feed(engine, pub1)
    engine.take_effects()
    pub2 = PublishPacket(topic="b", payload=b"2", qos=1, retain=False, dup=False, mid=2).encode()
    _feed(engine, pub2)
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)


def test_offline_qos_rejected_after_max_qos_connack() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5))
    handle = engine.queue_publish("t", b"x", qos=2)
    assert handle.mid is not None
    engine.begin_connect()
    props = Properties()
    props.set("maximum_qos", 1)
    body = bytes((0x00, 0x00)) + encode_properties(props, "CONNACK")
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PUBLISH_FAILED and e.data.mid == handle.mid for e in effects)
