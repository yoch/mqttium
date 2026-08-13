"""Hot-path contracts for specialized acknowledgement codecs."""

from __future__ import annotations

import mqttium.codec.properties as properties_module
import mqttium.packets._ack_v5 as ack_v5
import mqttium.protocol.inbound as inbound_module

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import PubAckPacket, PubCompPacket, PubRecPacket, PubRelPacket
from mqttium.packets._ack_v5 import decode_puback_v5
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def test_auto_qos1_puback_skips_packet_dataclass(monkeypatch) -> None:
    calls: list[PacketType] = []
    original = inbound_module.encode_success_ack

    def counted(packet_type, mid, **kwargs):
        calls.append(packet_type)
        return original(packet_type, mid, **kwargs)

    monkeypatch.setattr(inbound_module, "encode_success_ack", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED

    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("ack/hot") + pack_u16(7) + b"payload",
        )
    )
    effects = engine.take_effects()

    assert calls == [PacketType.PUBACK]
    assert [effect.kind for effect in effects] == [EffectKind.SEND, EffectKind.MESSAGE]
    assert effects[0].data == b"\x40\x02\x00\x07"


def test_success_puback_settle_skips_property_decoder(monkeypatch) -> None:
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ack_v5, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid)))
    effects = engine.take_effects()

    assert calls == 0
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_auto_qos2_pubrec_skips_packet_dataclass(monkeypatch) -> None:
    calls: list[PacketType] = []
    original = inbound_module.encode_success_ack

    def counted(packet_type, mid, **kwargs):
        calls.append(packet_type)
        return original(packet_type, mid, **kwargs)

    monkeypatch.setattr(inbound_module, "encode_success_ack", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED

    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x04,
            pack_utf8("ack/hot") + pack_u16(7) + b"payload",
        )
    )
    effects = engine.take_effects()

    assert calls == [PacketType.PUBREC]
    assert [effect.kind for effect in effects] == [EffectKind.SEND, EffectKind.MESSAGE]
    assert effects[0].data == b"\x50\x02\x00\x07"


def test_success_pubrec_settle_skips_property_decoder(monkeypatch) -> None:
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(properties_module, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBREC, 0x00, pack_u16(handle.mid)))
    effects = engine.take_effects()

    assert calls == 0
    assert any(effect.kind is EffectKind.SEND for effect in effects)
    assert any(effect.data == b"\x62\x02" + pack_u16(handle.mid) for effect in effects)


def test_success_pubcomp_settle_skips_property_decoder(monkeypatch) -> None:
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(properties_module, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.PUBREC, 0x00, pack_u16(handle.mid)))
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBCOMP, 0x00, pack_u16(handle.mid)))
    effects = engine.take_effects()

    assert calls == 0
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_success_pubrel_skips_property_decoder(monkeypatch) -> None:
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(properties_module, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x04,
            pack_utf8("ack/hot") + pack_u16(9) + b"payload",
        )
    )
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBREL, 0x02, pack_u16(9)))
    effects = engine.take_effects()

    assert calls == 0
    assert any(effect.data == b"\x70\x02\x00\x09" for effect in effects)


def test_reason_bearing_puback_skips_property_decoder(monkeypatch) -> None:
    """An MQTT 5 reason code without properties stays off decode_properties.

    Mosquitto answers 0x10 (No matching subscribers) whenever nothing is
    subscribed, which is a three-byte PUBACK. That shape must not allocate an
    empty Properties() either.
    """
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(properties_module, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x10"))
    effects = engine.take_effects()

    assert calls == 0
    mid, reason, props = decode_puback_v5(pack_u16(handle.mid) + b"\x10")
    assert mid == handle.mid
    assert reason == 0x10
    assert props is None
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_reason_bearing_pubrec_skips_property_decoder(monkeypatch) -> None:
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(properties_module, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBREC, 0x00, pack_u16(handle.mid) + b"\x10"))
    engine.take_effects()

    assert calls == 0


def test_property_bearing_puback_uses_property_decoder(monkeypatch) -> None:
    """Anything carrying a property table still needs decode_properties."""
    calls = 0
    original = properties_module.decode_properties

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(properties_module, "decode_properties", counted)
    monkeypatch.setattr(ack_v5, "decode_properties", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.outbound._decode_puback = ack_v5.decode_puback_v5
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    # mid + reason 0x00 + a one-byte property table holding nothing.
    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x00\x00"))
    effects = engine.take_effects()

    assert calls == 1
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_three_byte_ack_is_still_malformed_under_mqtt311() -> None:
    """The three-byte shape is MQTT 5 only; 3.1.1 must still reject it."""
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x00"))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert not any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_invalid_reason_code_on_the_fast_path_is_rejected() -> None:
    """The specialized decoder must not skip reason-code validation."""
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    # 0x7F is not a defined PUBACK reason code.
    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x7f"))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_packet_views_match_specialized_primitives() -> None:
    """Provisional packet dataclasses remain factories over the primitives."""
    remaining = b"\x00\x09\x10"
    packet = PubAckPacket.decode(remaining, MQTTProtocolVersion.MQTTv5)
    mid, reason, props = decode_puback_v5(remaining)
    assert packet.mid == mid
    assert packet.reason_code == reason
    assert packet.properties is props is None

    assert PubAckPacket(mid=9).encode(MQTTProtocolVersion.MQTTv311) == b"\x40\x02\x00\x09"
    assert PubAckPacket(mid=9, reason_code=0x10).encode(MQTTProtocolVersion.MQTTv5) == (
        b"\x40\x03\x00\x09\x10"
    )
    assert PubRelPacket(mid=9).encode(MQTTProtocolVersion.MQTTv311) == b"\x62\x02\x00\x09"
    assert PubRecPacket(mid=9).encode() == b"\x50\x02\x00\x09"
    assert PubCompPacket(mid=9).encode() == b"\x70\x02\x00\x09"


def test_empty_properties_object_encodes_as_success_frame() -> None:
    assert len(PubAckPacket(mid=9, properties=Properties()).encode()) == 4
