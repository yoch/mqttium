from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.packets._publish import (
    decode_publish_fields_v5,
    decode_qos0_message_v5 as _decode_v5_qos0_message,
)
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import InboundSession
from mqttium.types import Properties


def _connected() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, client_id="probe"))
    engine.state = ConnectionState.CONNECTED
    return engine


def _raw(
    topic: str,
    payload: bytes,
    properties: Properties | None = None,
    *,
    flags: int = 0,
) -> RawPacket:
    table = encode_properties(properties, PUBLISH) if properties is not None else b"\x00"
    return RawPacket(PacketType.PUBLISH, flags, pack_utf8(topic) + table + payload)


@pytest.mark.parametrize(
    ("topic", "payload", "retain"),
    [
        ("bench/telemetry", b"payload", False),
        ("capteur/température", b"", True),
        ("a/b/c", b"x" * 4096, False),
    ],
)
def test_v5_qos0_direct_decode_matches_generic_packet(
    topic: str,
    payload: bytes,
    retain: bool,
) -> None:
    raw = _raw(topic, payload, flags=0x01 if retain else 0x00)
    message = _decode_v5_qos0_message(raw)
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)

    assert message.topic == packet.topic == topic
    assert message.payload == packet.payload == payload
    assert message.qos is packet.qos is QoS.AT_MOST_ONCE
    assert message.retain is packet.retain is retain
    assert message.dup is packet.dup is False
    assert message.mid is packet.mid is None
    assert message.properties is not None
    assert packet.properties is not None
    assert message.properties.values == packet.properties.values == {}


def test_v5_qos0_direct_decode_preserves_properties() -> None:
    props = Properties()
    props.set("content_type", "text/plain")
    raw = _raw("bench/v5/props", b"body", props)
    message = _decode_v5_qos0_message(raw)
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)

    assert message.properties is not None
    assert message.properties.get("content_type") == "text/plain"
    assert message.properties.values == packet.properties.values
    assert message.payload == b"body"


def test_v5_qos0_engine_skips_generic_field_decoder(monkeypatch) -> None:
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return decode_publish_fields_v5(*args, **kwargs)

    monkeypatch.setattr(
        "mqttium.protocol.inbound.decode_publish_fields_v5",
        counted,
    )
    engine = _connected()
    engine.handle_raw(_raw("bench/direct", b"payload"))

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "bench/direct"
    assert effects[0].data.payload == b"payload"
    assert effects[0].data.properties is not None
    assert not effects[0].data.properties
    assert calls == 0


def test_v5_qos0_empty_table_skips_alias_resolution(monkeypatch) -> None:
    calls = 0
    original = InboundSession._resolve_topic_fields

    def counted(self, topic, props):
        nonlocal calls
        calls += 1
        return original(self, topic, props)

    monkeypatch.setattr(InboundSession, "_resolve_topic_fields", counted)
    engine = _connected()
    engine.handle_raw(_raw("bench/no-alias", b"x"))
    effects = engine.take_effects()
    assert effects[0].data.topic == "bench/no-alias"
    assert calls == 0


def test_v5_qos0_properties_without_alias_skip_resolution(monkeypatch) -> None:
    calls = 0
    original = InboundSession._resolve_topic_fields

    def counted(self, topic, props):
        nonlocal calls
        calls += 1
        return original(self, topic, props)

    monkeypatch.setattr(InboundSession, "_resolve_topic_fields", counted)
    props = Properties()
    props.set("content_type", "text/plain")
    engine = _connected()
    engine.handle_raw(_raw("bench/typed", b"x", props))
    engine.take_effects()
    assert calls == 0


def test_v5_qos1_empty_table_skips_alias_resolution(monkeypatch) -> None:
    calls = 0
    original = InboundSession._resolve_topic_fields

    def counted(self, topic, props):
        nonlocal calls
        calls += 1
        return original(self, topic, props)

    monkeypatch.setattr(InboundSession, "_resolve_topic_fields", counted)
    engine = _connected()
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/v5/qos1") + pack_u16(9) + b"\x00" + b"payload",
        )
    )
    engine.take_effects()
    assert calls == 0


def test_v5_qos1_still_uses_generic_field_decoder(monkeypatch) -> None:
    calls = 0

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return decode_publish_fields_v5(*args, **kwargs)

    monkeypatch.setattr(
        "mqttium.protocol.inbound.decode_publish_fields_v5",
        counted,
    )
    engine = _connected()
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/v5/qos1") + pack_u16(9) + b"\x00" + b"payload",
        )
    )
    effects = engine.take_effects()
    assert effects[1].data.qos is QoS.AT_LEAST_ONCE
    assert calls == 1


@pytest.mark.parametrize(
    ("flags", "remaining", "error_type", "match"),
    [
        (0x08, pack_utf8("bench/dup") + b"\x00x", MalformedPacketError, "must not set DUP"),
        (
            0x00,
            pack_utf8("bench/+") + b"\x00x",
            MalformedPacketError,
            "wildcards",
        ),
    ],
)
def test_v5_qos0_direct_decode_preserves_validation(
    flags: int,
    remaining: bytes,
    error_type: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error_type, match=match):
        _decode_v5_qos0_message(RawPacket(PacketType.PUBLISH, flags, remaining))


def test_v5_qos0_empty_topic_without_alias_is_protocol_error() -> None:
    engine = _connected()
    engine.handle_raw(_raw("", b"x"))
    effects = engine.take_effects()
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.MESSAGE for effect in effects)


def test_v5_qos0_topic_alias_still_resolves() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            client_id="alias",
            topic_alias_maximum=10,
        )
    )
    engine.state = ConnectionState.CONNECTED
    props = Properties()
    props.set("topic_alias", 1)
    engine.handle_raw(_raw("sensors/temp", b"first", props))
    engine.take_effects()
    assert engine.inbound._aliases == {1: "sensors/temp"}

    engine.handle_raw(_raw("", b"second", props))
    effects = engine.take_effects()
    message = next(effect.data for effect in effects if effect.kind is EffectKind.MESSAGE)
    assert message.topic == "sensors/temp"
    assert message.payload == b"second"
