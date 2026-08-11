"""Reproduction for GitHub issue #128."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
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
    reason="https://github.com/yoch/mqttium/issues/128 — alias stored before topic validation",
)
def test_invalid_topic_does_not_establish_inbound_alias() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug128", protocol=MQTTProtocolVersion.MQTTv5, topic_alias_maximum=10)
    )
    engine.begin_connect()
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    engine.take_effects()

    props = Properties()
    props.set("topic_alias", 1)
    _feed(
        engine,
        PublishPacket(
            topic="bad/#",
            payload=b"x",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            properties=props,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    engine.take_effects()
    assert 1 not in engine.inbound._aliases
