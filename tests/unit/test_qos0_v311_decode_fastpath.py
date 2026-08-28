from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.packets._publish import decode_qos0_message_v311 as _decode_v311_qos0_message
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine


def _connected(
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize(
    ("topic", "payload", "retain"),
    [
        ("bench/telemetry", b"payload", False),
        ("capteur/température", b"", True),
        ("a/b/c", b"x" * 4096, False),
    ],
)
def test_v311_qos0_direct_decode_matches_generic_packet(
    topic: str,
    payload: bytes,
    retain: bool,
) -> None:
    raw = RawPacket(
        packet_type=PacketType.PUBLISH,
        flags=0x01 if retain else 0x00,
        remaining=pack_utf8(topic) + payload,
    )
    message = _decode_v311_qos0_message(raw)
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv311)

    assert message.topic == packet.topic == topic
    assert message.payload == packet.payload == payload
    assert message.qos is packet.qos is QoS.AT_MOST_ONCE
    assert message.retain is packet.retain is retain
    assert message.dup is packet.dup is False
    assert message.mid is packet.mid is None
    assert message.properties is packet.properties is None


def test_v311_qos0_engine_emits_direct_message(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected()
    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0x00, pack_utf8("bench/direct") + b"payload"))

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "bench/direct"
    assert effects[0].data.payload == b"payload"
    assert calls == 0


@pytest.mark.parametrize(
    ("flags", "remaining", "error_type", "match"),
    [
        (0x08, pack_utf8("bench/dup") + b"x", MalformedPacketError, "must not set DUP"),
        (0x00, pack_utf8("") + b"x", MalformedPacketError, "must not be empty"),
        (
            0x00,
            pack_utf8("bench/+") + b"x",
            MalformedPacketError,
            "wildcards",
        ),
    ],
)
def test_v311_qos0_direct_decode_preserves_validation(
    flags: int,
    remaining: bytes,
    error_type: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error_type, match=match):
        _decode_v311_qos0_message(RawPacket(PacketType.PUBLISH, flags, remaining))


def test_qos1_uses_direct_acknowledged_path_after_composition(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected()
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/qos1") + pack_u16(7) + b"payload",
        )
    )

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [
        EffectKind.SEND_PROTOCOL_RESPONSE,
        EffectKind.MESSAGE,
    ]
    assert effects[1].data.mid == 7
    assert effects[1].data.qos is QoS.AT_LEAST_ONCE
    assert calls == 0


def test_mqtt5_qos2_emits_pubrec_before_message() -> None:
    engine = _connected(MQTTProtocolVersion.MQTTv5)
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x04,
            pack_utf8("bench/qos2") + pack_u16(8) + b"\x00payload",
        )
    )

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [
        EffectKind.SEND_PROTOCOL_RESPONSE,
        EffectKind.MESSAGE,
    ]
    assert effects[1].data.mid == 8
    assert effects[1].data.qos is QoS.EXACTLY_ONCE


def test_mqtt5_qos0_uses_direct_property_aware_path(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected(MQTTProtocolVersion.MQTTv5)
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x00,
            pack_utf8("bench/v5") + b"\x00" + b"payload",
        )
    )

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "bench/v5"
    assert effects[0].data.payload == b"payload"
    assert calls == 0


def test_mqtt5_qos0_direct_path_preserves_properties(monkeypatch) -> None:
    from mqttium.codec.properties import PUBLISH, encode_properties
    from mqttium.types import Properties

    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    props = Properties()
    props.set("content_type", "text/plain")
    wire_props = encode_properties(props, PUBLISH)
    engine = _connected(MQTTProtocolVersion.MQTTv5)
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x00,
            pack_utf8("bench/v5/props") + wire_props + b"body",
        )
    )

    effects = engine.take_effects()
    assert effects[0].data.properties is not None
    assert effects[0].data.properties.get("content_type") == "text/plain"
    assert effects[0].data.payload == b"body"
    assert calls == 0


def test_mqtt5_qos1_uses_direct_property_aware_path(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected(MQTTProtocolVersion.MQTTv5)
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/v5/qos1") + pack_u16(9) + b"\x00" + b"payload",
        )
    )

    effects = engine.take_effects()
    assert effects[1].data.qos is QoS.AT_LEAST_ONCE
    assert calls == 0
