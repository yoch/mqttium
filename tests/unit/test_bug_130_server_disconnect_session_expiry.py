"""Reproduction for GitHub issue #130."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from mqttium.packets.control import encode_disconnect
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
    reason="https://github.com/yoch/mqttium/issues/130 — server DISCONNECT Session Expiry accepted",
)
def test_server_disconnect_with_session_expiry_is_protocol_error() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug130", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()

    dprops = Properties()
    dprops.set("session_expiry_interval", 30)
    _feed(engine, encode_disconnect(0, MQTTProtocolVersion.MQTTv5, dprops))
    effects = engine.take_effects()

    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    discs = [e.data for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert not (
        len(discs) == 1
        and discs[0].from_broker
        and discs[0].properties is not None
        and discs[0].properties.get("session_expiry_interval") == 30
    )
