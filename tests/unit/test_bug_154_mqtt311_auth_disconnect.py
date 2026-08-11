"""Reproduction for GitHub issue #154.

MQTT 3.1.1 has no AUTH packet. Receiving one must disconnect, but
``_on_auth`` calls ``encode_disconnect(0x82, MQTTv311)`` which raises, and
``handle_raw`` converts that into a non-fatal ``PROTOCOL_ERROR`` while
leaving the session ``CONNECTED``.
"""

from __future__ import annotations

import pytest

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


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/154 — v3.1.1 AUTH fails to disconnect",
)
def test_mqtt311_auth_disconnects_the_session() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug154", protocol=MQTTProtocolVersion.MQTTv311)
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()

    _feed(engine, b"\xf0\x00")
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert any(e.kind is EffectKind.DISCONNECTED for e in effects)
    assert not any(
        e.kind is EffectKind.PROTOCOL_ERROR
        and "DISCONNECT reason/properties require MQTT 5" in str(e.data)
        for e in effects
    )
