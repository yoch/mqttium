"""QoS 0 reuses the Topic Name bytes produced by outbound validation."""

from __future__ import annotations

import mqttium.packets._publish as publish_v5_module
import mqttium.protocol.outbound as outbound_module
import mqttium.topics as topics_module

from mqttium.enums import ConnectionState, MQTTProtocolVersion
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    return engine


def test_qos0_validation_hands_topic_bytes_to_encoder(monkeypatch) -> None:
    validation_calls = 0
    encoder_calls = 0
    original_validation_encode = topics_module.encode_utf8
    original_packet_encode = publish_v5_module.encode_utf8

    def validation_encode(topic: str) -> bytes:
        nonlocal validation_calls
        validation_calls += 1
        return original_validation_encode(topic)

    def packet_encode(topic: str) -> bytes:
        nonlocal encoder_calls
        encoder_calls += 1
        return original_packet_encode(topic)

    monkeypatch.setattr(topics_module, "encode_utf8", validation_encode)
    monkeypatch.setattr(publish_v5_module, "encode_utf8", packet_encode)

    engine = _connected_engine()
    item = engine.outbound.prepare_qos0("capteurs/été/température", b"payload")

    assert item
    assert validation_calls == 1
    assert encoder_calls == 0


def test_qos1_validation_does_not_preencode_topic(monkeypatch) -> None:
    def unexpected_preencode(_topic: str) -> bytes:
        raise AssertionError("QoS 1 validation must not allocate Topic Name bytes")

    monkeypatch.setattr(outbound_module, "encode_validated_publish_topic", unexpected_preencode)

    engine = _connected_engine()
    handle = engine.queue_publish("capteurs/qos1", b"payload", qos=1)

    assert handle.mid is not None
