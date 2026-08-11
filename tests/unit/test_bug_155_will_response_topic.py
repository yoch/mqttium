"""Reproduction for GitHub issue #155.

Will Properties ``response_topic`` must be a Topic Name: non-empty and without
wildcards. Distinct from #88 (PUBLISH property path).
"""

from __future__ import annotations

import pytest

from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import ProtocolError
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/155 — Will response_topic not validated",
)
@pytest.mark.parametrize("response_topic", ["out/#", "reply/+", ""])
def test_will_response_topic_rejects_invalid_topic_names(response_topic: str) -> None:
    props = Properties()
    props.set("response_topic", response_topic)
    engine = ProtocolEngine(
        EngineConfig(
            client_id="bug155",
            protocol=MQTTProtocolVersion.MQTTv5,
            will=Message(topic="will/topic", payload=b"bye"),
            will_properties=props,
        )
    )
    with pytest.raises(ProtocolError):
        engine.begin_connect()


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/155 — empty PUBLISH response_topic accepted",
)
def test_publish_response_topic_rejects_empty_string() -> None:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.codec.properties import encode_properties
    from mqttium.enums import PacketType
    from mqttium.packets import encode_frame

    engine = ProtocolEngine(
        EngineConfig(client_id="bug155", protocol=MQTTProtocolVersion.MQTTv5)
    )
    engine.begin_connect()
    body = bytearray([0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, body))
    engine.handle_raw(decoder.next_packet())  # type: ignore[arg-type]
    engine.take_effects()

    props = Properties()
    props.set("response_topic", "")
    with pytest.raises(ProtocolError):
        engine.queue_publish("t", b"x", qos=0, properties=props)
