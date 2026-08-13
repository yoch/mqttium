from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    PacketType,
    QoS,
)
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.packets._publish_v311 import decode_qos12_fields_v311 as _decode_v311_qos1_fields


def _connected(*, manual_ack: bool = False) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv311,
            manual_ack=manual_ack,
        )
    )
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize(
    ("topic", "payload", "retain", "dup", "mid", "qos_flag"),
    [
        ("bench/request", b"payload", False, False, 1, 0x02),
        ("capteur/température", b"", True, True, 65535, 0x02),
        ("a/b/c", b"x" * 4096, False, True, 42, 0x02),
        ("bench/exactly", b"payload", False, False, 3, 0x04),
        ("capteur/température", b"", True, True, 65535, 0x04),
    ],
)
def test_v311_identified_fields_match_generic_packet(
    topic: str,
    payload: bytes,
    retain: bool,
    dup: bool,
    mid: int,
    qos_flag: int,
) -> None:
    flags = qos_flag | int(retain) | (0x08 if dup else 0)
    raw = RawPacket(
        PacketType.PUBLISH,
        flags,
        pack_utf8(topic) + pack_u16(mid) + payload,
    )
    fields = _decode_v311_qos1_fields(raw)
    packet = PublishPacket.decode(flags, raw.remaining, MQTTProtocolVersion.MQTTv311)
    assert fields == (
        packet.topic,
        packet.payload,
        packet.mid,
        packet.retain,
        packet.dup,
    )


def test_v311_qos1_zero_mid_preserves_error() -> None:
    raw = RawPacket(
        PacketType.PUBLISH,
        0x02,
        pack_utf8("bench/zero") + b"\x00\x00payload",
    )
    with pytest.raises(MalformedPacketError, match="PUBLISH packet identifier"):
        _decode_v311_qos1_fields(raw)


def test_v311_qos0_and_qos1_both_avoid_generic_packet(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected()
    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0x00, pack_utf8("bench/qos0") + b"zero"))
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/qos1") + pack_u16(7) + b"one",
        )
    )
    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [
        EffectKind.MESSAGE,
        EffectKind.SEND,
        EffectKind.MESSAGE,
    ]
    assert effects[0].data.qos is QoS.AT_MOST_ONCE
    assert effects[2].data.qos is QoS.AT_LEAST_ONCE
    assert effects[2].data.mid == 7
    assert calls == 0


def test_v311_qos1_manual_ack_preserves_duplicate_state() -> None:
    engine = _connected(manual_ack=True)
    first = RawPacket(
        PacketType.PUBLISH,
        0x02,
        pack_utf8("bench/manual") + pack_u16(9) + b"payload",
    )
    engine.handle_raw(first)
    first_effects = engine.take_effects()
    assert [effect.kind for effect in first_effects] == [EffectKind.MESSAGE]
    stored = engine.store.get_in(9)
    assert stored is not None
    assert stored.state is InboundQoSState.WAIT_PUBACK

    duplicate = RawPacket(
        PacketType.PUBLISH,
        0x0A,
        pack_utf8("bench/manual") + pack_u16(9) + b"payload",
    )
    engine.handle_raw(duplicate)
    duplicate_effects = engine.take_effects()
    assert [effect.kind for effect in duplicate_effects] == [EffectKind.MESSAGE]
    assert duplicate_effects[0].data.mid == 9
    assert duplicate_effects[0].data.dup is True


def test_mqtt5_qos1_and_qos2_use_direct_fields(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/v5/qos1") + pack_u16(3) + b"\x00payload",
        )
    )
    assert calls == 0

    # A fresh engine keeps this test focused on on_publish: otherwise the
    # Receive-Maximum preflight intentionally decodes a second PUBLISH while
    # the first auto-PUBACK is still pending in the current effect batch.
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x04,
            pack_utf8("bench/v5/qos2") + pack_u16(4) + b"\x00payload",
        )
    )
    assert calls == 0


def test_v311_qos2_avoids_generic_packet(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x04,
            pack_utf8("bench/qos2") + pack_u16(3) + b"payload",
        )
    )
    effects = engine.take_effects()
    assert calls == 0
    assert effects[0].kind is EffectKind.SEND
    assert effects[1].kind is EffectKind.MESSAGE
    assert effects[1].data.qos is QoS.EXACTLY_ONCE
    assert effects[1].data.mid == 3
