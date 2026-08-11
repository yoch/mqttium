"""Reproduction for GitHub issue #85.

MQTT 5 Table 3-11: reason code 0x19 (Re-authenticate) is Client-sent only.
A broker-sent AUTH with 0x19 after CONNACK must be a Protocol Error.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import AuthPacket, encode_frame
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


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/85 — broker AUTH 0x19 accepted when connected",
)
def test_broker_reauthenticate_auth_after_connack_is_protocol_error() -> None:
    connect_props = Properties()
    connect_props.set("authentication_method", "demo")
    engine = ProtocolEngine(
        EngineConfig(
            client_id="bug85",
            protocol=MQTTProtocolVersion.MQTTv5,
            accept_auth=True,
            connect_properties=connect_props,
        )
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    auth_props = Properties()
    auth_props.set("authentication_method", "demo")
    _feed(engine, AuthPacket(reason_code=0x19, properties=auth_props).encode())
    effects = engine.take_effects()

    assert not any(e.kind is EffectKind.AUTH for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED
    disconnects = [e.data for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert disconnects
    assert disconnects[0].reason_code == 0x82
