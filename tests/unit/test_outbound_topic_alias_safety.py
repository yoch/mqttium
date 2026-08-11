"""Outbound Topic Alias omission requires connection-scoped alias state."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="alias-safety", protocol=MQTTProtocolVersion.MQTTv5)
    )
    engine.begin_connect()
    connack_properties = Properties()
    connack_properties.set("topic_alias_maximum", 10)
    body = bytearray((0, 0))
    body.extend(encode_properties(connack_properties, CONNACK))
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, body))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    engine.take_effects()
    return engine


@pytest.mark.parametrize("qos", [0, 1, 2])
def test_empty_outbound_topic_with_alias_is_refused(qos: int) -> None:
    engine = _connected_engine()
    properties = Properties()
    properties.set("topic_alias", 1)

    with pytest.raises(ProtocolError, match="topic must not be empty"):
        engine.queue_publish("", b"payload", qos=qos, properties=properties)

    assert engine.take_effects() == []
    assert engine.pending_outbound_messages == 0
    assert len(engine.packet_ids) == 0


def test_nonempty_topic_with_alias_remains_supported() -> None:
    engine = _connected_engine()
    properties = Properties()
    properties.set("topic_alias", 1)

    engine.queue_publish("canonical/topic", b"payload", qos=0, properties=properties)
    effects = engine.take_effects()

    assert effects
