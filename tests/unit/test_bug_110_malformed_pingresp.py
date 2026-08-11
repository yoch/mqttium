"""Reproduction for GitHub issue #110.

PINGRESP Remaining Length must be 0 (MQTT §3.13.1). A non-empty remaining
payload is currently accepted as a successful keepalive response.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, PacketType
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
    reason="https://github.com/yoch/mqttium/issues/110 — malformed PINGRESP accepted",
)
def test_pingresp_with_nonzero_remaining_length_is_protocol_error() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug110"))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()

    _feed(engine, b"\xd0\x01\xff")
    effects = engine.take_effects()

    assert not any(e.kind is EffectKind.PINGRESP for e in effects)
    assert engine.state is ConnectionState.DISCONNECTED or any(
        e.kind is EffectKind.PROTOCOL_ERROR for e in effects
    )
