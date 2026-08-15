"""Reproduction for GitHub issue #241.

MQTT 5 §4.12.1: AUTH Success (0x00) while CONNECTED completes a Client-initiated
re-authentication. Without a pending Re-authenticate (0x19), it is a Protocol Error.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _engine() -> ProtocolEngine:
    return ProtocolEngine(
        EngineConfig(
            client_id="bug241",
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=Properties(values={"authentication_method": "demo"}),
        )
    )


def _connack_with_method() -> bytes:
    props = Properties()
    props.set("authentication_method", "demo")
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(props, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def _auth_success() -> bytes:
    props = Properties()
    props.set("authentication_method", "demo")
    body = bytearray([0x00])
    body.extend(encode_properties(props, "AUTH"))
    return encode_frame(PacketType.AUTH, 0, body)


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/241 — unsolicited AUTH Success while CONNECTED",
)
def test_unsolicited_auth_success_while_connected_is_protocol_error() -> None:
    engine = _engine()
    engine.begin_connect()
    engine.take_effects()
    _feed(engine, _connack_with_method())
    engine.take_effects()

    _feed(engine, _auth_success())
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert not any(e.kind is EffectKind.AUTH for e in effects)
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
