"""Reproduction for GitHub issue #129."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
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


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/129 — empty topic with unestablished alias accepted",
)
def test_outbound_empty_topic_requires_established_alias() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="bug129", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    props = Properties()
    props.set("topic_alias_maximum", 10)
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(props, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()

    op = Properties()
    op.set("topic_alias", 1)
    with pytest.raises(ProtocolError):
        engine.queue_publish("", b"payload", qos=0, properties=op)
