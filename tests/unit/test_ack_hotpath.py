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
    def fail_packet(*args, **kwargs):
        raise AssertionError("auto-PUBACK must use the fixed frame helper")

    monkeypatch.setattr(inbound_module, "PubAckPacket", fail_packet)
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

    engine.handle_raw(
        RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid))
    )
    effects = engine.take_effects()

    assert calls == 0
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)


def test_reason_bearing_puback_keeps_full_decoder(monkeypatch) -> None:
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

    engine.handle_raw(
        RawPacket(PacketType.PUBACK, 0x00, pack_u16(handle.mid) + b"\x10")
    )
    effects = engine.take_effects()

    assert calls == 1
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)
