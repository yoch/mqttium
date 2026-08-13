"""MQTT 3.1 PUBLISH packets never contain MQTT 5 property tables."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv31))
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize("payload", [b"\x00body", b"\x05body"])
def test_v31_qos0_preserves_payload_prefix(payload: bytes) -> None:
    engine = _engine()
    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0x00, pack_utf8("v31/qos0") + payload))

    effects = engine.take_effects()
    messages = [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert len(messages) == 1
    assert messages[0].qos is QoS.AT_MOST_ONCE
    assert messages[0].payload == payload
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_v31_qos12_preserves_payload_prefix(qos: QoS) -> None:
    engine = _engine()
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            int(qos) << 1,
            pack_utf8(f"v31/qos{int(qos)}") + pack_u16(7) + b"\x00body",
        )
    )

    effects = engine.take_effects()
    messages = [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert len(messages) == 1
    assert messages[0].qos is qos
    assert messages[0].payload == b"\x00body"
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_v31_qos2_still_uses_generic_packet(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _engine()
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x04,
            pack_utf8("v31/qos2") + pack_u16(3) + b"payload",
        )
    )
    assert calls == 1
