"""Regression coverage for MQTT 5 Response Topic semantics."""

from __future__ import annotations

import pytest

from mqttium.codec.properties import PUBLISH, WILL, decode_properties, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


@pytest.mark.parametrize("response_topic", ["", "reply/#", "reply/+"])
def test_response_topic_encode_requires_a_topic_name(response_topic: str) -> None:
    properties = Properties({"response_topic": response_topic})

    with pytest.raises(ProtocolError, match="response_topic"):
        encode_properties(properties, PUBLISH)
    with pytest.raises(ProtocolError, match="response_topic"):
        encode_properties(properties, WILL)


@pytest.mark.parametrize("response_topic", ["", "reply/#", "reply/+"])
def test_response_topic_decode_requires_a_topic_name(response_topic: str) -> None:
    encoded_topic = response_topic.encode("utf-8")
    body = b"\x08" + len(encoded_topic).to_bytes(2, "big") + encoded_topic
    wire = bytes((len(body),)) + body

    with pytest.raises(MalformedPacketError, match="response_topic"):
        decode_properties(wire, 0, PUBLISH)


@pytest.mark.parametrize("response_topic", ["", "reply/#", "reply/+"])
def test_invalid_will_response_topic_fails_before_connect_state_mutation(
    response_topic: str,
) -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="response-topic",
            protocol=MQTTProtocolVersion.MQTTv5,
            will=Message(topic="will/topic", payload=b"bye"),
            will_properties=Properties({"response_topic": response_topic}),
        )
    )

    with pytest.raises(ProtocolError, match="response_topic"):
        engine.begin_connect()

    assert engine.state is ConnectionState.NEW
    assert engine.take_effects() == []
