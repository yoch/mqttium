"""Hot-path contracts for the common success/no-properties PUBACK shape."""

from __future__ import annotations

import mqttium.protocol.inbound as inbound_module
import mqttium.protocol.outbound as outbound_module

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


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


def test_success_puback_settle_skips_full_decoder(monkeypatch) -> None:
    calls = 0
    original = outbound_module.PubAckPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module.PubAckPacket, "decode", counted)
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


def test_success_pubrec_settle_skips_full_decoder(monkeypatch) -> None:
    calls = 0
    original = outbound_module.PubRecPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module.PubRecPacket, "decode", counted)
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


def test_success_pubcomp_settle_skips_full_decoder(monkeypatch) -> None:
    calls = 0
    original = outbound_module.PubCompPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module.PubCompPacket, "decode", counted)
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


def test_success_pubrel_skips_full_decoder(monkeypatch) -> None:
    calls = 0
    original = inbound_module.PubRelPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(inbound_module.PubRelPacket, "decode", counted)
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


def test_reason_bearing_puback_skips_full_decoder(monkeypatch) -> None:
    """An MQTT 5 reason code without properties stays on the fast path.

    Mosquitto answers 0x10 (No matching subscribers) whenever nothing is
    subscribed, which is a three-byte PUBACK. Sending that through the full
    decoder cost 7.7% of QoS 1 publish capacity under MQTT 5 against 3.1.1,
    because the two-byte fast path never fired.
    """
    calls = 0
    original = outbound_module.PubAckPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module.PubAckPacket, "decode", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x10"))
    effects = engine.take_effects()

    assert calls == 0
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_reason_bearing_pubrec_skips_full_decoder(monkeypatch) -> None:
    calls = 0
    original = outbound_module.PubRecPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module.PubRecPacket, "decode", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()

    engine.handle_raw(RawPacket(PacketType.PUBREC, 0x00, pack_u16(handle.mid) + b"\x10"))
    engine.take_effects()

    assert calls == 0


def test_property_bearing_puback_keeps_full_decoder(monkeypatch) -> None:
    """The boundary moved to properties, not to the reason code.

    Anything carrying a property table still needs the decoder, both to read it
    and to enforce the Request Problem Information obligation against it.
    """
    calls = 0
    original = outbound_module.PubAckPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(outbound_module.PubAckPacket, "decode", counted)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
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
    """The fast path must not skip reason-code validation."""
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/hot", b"payload", qos=1)
    assert handle.mid is not None
    engine.take_effects()

    # 0x7F is not a defined PUBACK reason code.
    engine.handle_raw(RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x7f"))
    effects = engine.take_effects()

    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
