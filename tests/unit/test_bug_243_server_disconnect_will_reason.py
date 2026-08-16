"""Regression coverage for GitHub issue #243.

MQTT 5 Table 3-10: DISCONNECT reason 0x04 (Disconnect with Will Message) is
Client-only. A Server sending it is a Protocol Error (MQTT-3.14.2-1).
"""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="bug243", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    return engine


def test_server_disconnect_with_will_reason_is_protocol_error() -> None:
    engine = _connected_engine()

    _feed(engine, encode_frame(PacketType.DISCONNECT, 0, b"\x04\x00"))
    effects = engine.take_effects()

    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert not any(
        e.kind is EffectKind.DISCONNECTED and getattr(e.data, "reason_code", None) == 0x04
        for e in effects
    )
    assert engine.state is ConnectionState.CONNECTED


def test_server_only_disconnect_reason_remains_accepted() -> None:
    engine = _connected_engine()

    _feed(engine, encode_frame(PacketType.DISCONNECT, 0, b"\x8c\x00"))
    effects = engine.take_effects()

    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert any(
        e.kind is EffectKind.DISCONNECTED and getattr(e.data, "reason_code", None) == 0x8C
        for e in effects
    )
    assert engine.state is ConnectionState.DISCONNECTED
