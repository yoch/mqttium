"""Reproduction for GitHub issue #86.

MQTT 5 Maximum Packet Size applies to all control packets sent to the peer,
including DISCONNECT. ``begin_disconnect()`` currently skips the size check.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import PacketTooLargeError
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack_v5(props: Properties) -> bytes:
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(props, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/86 — DISCONNECT ignores maximum_packet_size",
)
def test_begin_disconnect_enforces_negotiated_maximum_packet_size() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug86", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    props = Properties()
    props.set("maximum_packet_size", 10)
    _feed(engine, _connack_v5(props))
    engine.take_effects()
    assert engine.negotiated.maximum_packet_size == 10

    dprops = Properties()
    dprops.set("reason_string", "x" * 50)
    with pytest.raises(PacketTooLargeError):
        engine.begin_disconnect(properties=dprops)
    assert engine.state is ConnectionState.CONNECTED
