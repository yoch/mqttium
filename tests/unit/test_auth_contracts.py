"""Enhanced authentication method and state contracts."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import AuthPacket
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import Properties


def _props(method: str = "demo") -> Properties:
    properties = Properties()
    properties.set("authentication_method", method)
    return properties


def _raw_auth(reason: int, method: str | None = None) -> RawPacket:
    properties = _props(method) if method is not None else Properties()
    wire = AuthPacket(reason_code=reason, properties=properties).encode()
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    packet = decoder.next_packet()
    assert packet is not None
    return packet


def test_auth_handler_requires_method_in_connect() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
        )
    )
    with pytest.raises(ProtocolError, match="authentication_method"):
        engine.begin_connect()
    assert engine.state is ConnectionState.NEW


def test_inbound_auth_method_mismatch_disconnects_with_bad_method() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=_props("expected"),
        )
    )
    engine.begin_connect()
    engine.handle_raw(_raw_auth(0x18, "other"))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.SEND for effect in effects)
    disconnected = next(effect.data for effect in effects if effect.kind is EffectKind.DISCONNECTED)
    assert disconnected.reason_code == 0x8C


def test_outbound_auth_injects_configured_method() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=_props("expected"),
        )
    )
    engine.begin_connect()
    engine.take_effects()

    engine.queue_auth(reason_code=0x18, properties=Properties())
    wire = next(effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND)
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None and raw.packet_type is PacketType.AUTH
    packet = AuthPacket.decode(raw.remaining)
    assert packet.properties is not None
    assert packet.properties.get("authentication_method") == "expected"


def test_outbound_auth_rejects_method_change() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=_props("expected"),
        )
    )
    engine.begin_connect()
    with pytest.raises(ProtocolError, match="does not match"):
        engine.queue_auth(reason_code=0x18, properties=_props("other"))


def test_reauthenticate_requires_connected_state() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=_props(),
        )
    )
    engine.begin_connect()
    with pytest.raises(ProtocolError, match="connected session"):
        engine.queue_auth(reason_code=0x19)


def test_broker_cannot_send_reauthenticate_reason_after_connack() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="auth-direction",
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=_props(),
        )
    )
    engine.begin_connect()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    engine.handle_raw(_raw_auth(0x19, "demo"))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert not any(effect.kind is EffectKind.AUTH for effect in effects)
    disconnected = next(effect.data for effect in effects if effect.kind is EffectKind.DISCONNECTED)
    assert disconnected.reason_code == 0x82
    assert any(effect.kind is EffectKind.SEND for effect in effects)
