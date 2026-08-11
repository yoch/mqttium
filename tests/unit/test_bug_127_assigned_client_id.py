"""Reproduction for GitHub issue #127."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/127 — missing Assigned Client Identifier accepted",
)
def test_empty_client_id_connack_requires_assigned_client_identifier() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
