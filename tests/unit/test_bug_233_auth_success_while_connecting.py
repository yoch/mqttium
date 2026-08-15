"""Reproduction for GitHub issue #233.

MQTT-4.12.0-2: during the CONNECT→CONNACK enhanced-auth exchange, a Server AUTH
MUST use Reason Code 0x18 (Continue authentication). AUTH Success (0x00) belongs
to the re-authentication flow (§4.12.1), not the initial handshake.
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
            client_id="bug233",
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=Properties(values={"authentication_method": "demo"}),
        )
    )


def _auth_success() -> bytes:
    props = Properties()
    props.set("authentication_method", "demo")
    body = bytearray([0x00])
    body.extend(encode_properties(props, "AUTH"))
    return encode_frame(PacketType.AUTH, 0, body)


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/233 — AUTH Success during CONNECTING",
)
def test_auth_success_during_connecting_is_protocol_error() -> None:
    engine = _engine()
    engine.begin_connect()
    engine.take_effects()

    _feed(engine, _auth_success())
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert not any(e.kind is EffectKind.AUTH for e in effects)
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
