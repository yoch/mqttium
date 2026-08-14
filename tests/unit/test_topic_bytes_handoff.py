"""Outbound PUBLISH reuses Topic Name bytes produced by validation on the launch path."""

from __future__ import annotations

import mqttium.packets._publish as publish_v5_module
import mqttium.protocol.outbound as outbound_module
import mqttium.topics as topics_module

from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets._publish import encode_publish_item_v5
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from tests.support import write_item_bytes


def _counting_str(value: str) -> tuple[str, list[int]]:
    original = str.encode
    encodes: list[int] = []

    class CountingStr(str):
        def encode(self, *args, **kwargs):  # type: ignore[override]
            encodes.append(1)
            return original(self, *args, **kwargs)

    return CountingStr(value), encodes


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


def test_qos1_connected_hands_topic_bytes_to_encoder(monkeypatch) -> None:
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
    handle = engine.queue_publish("capteurs/été/température", b"payload", qos=1)

    assert handle.mid is not None
    assert validation_calls == 1
    assert encoder_calls == 0


def test_qos1_offline_validation_does_not_preencode_topic(monkeypatch) -> None:
    def unexpected_preencode(_topic: str) -> bytes:
        raise AssertionError("offline QoS 1 validation must not allocate Topic Name bytes")

    monkeypatch.setattr(outbound_module, "encode_validated_publish_topic", unexpected_preencode)

    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    handle = engine.queue_publish("capteurs/qos1", b"payload", qos=1)

    assert handle.mid is not None


def test_qos1_connected_utf8_encodes_topic_once() -> None:
    topic, encodes = _counting_str("é" * 256)
    engine = _connected_engine()
    handle = engine.queue_publish(topic, b"payload", qos=1)

    assert handle.mid is not None
    assert len(encodes) == 1


def test_qos1_connected_ascii_encodes_topic_once() -> None:
    topic, encodes = _counting_str("sensors/" + "a" * 256)
    engine = _connected_engine()
    handle = engine.queue_publish(topic, b"payload", qos=1)

    assert handle.mid is not None
    assert len(encodes) == 1


def test_qos1_offline_utf8_encodes_topic_once_for_length() -> None:
    topic, encodes = _counting_str("é" * 256)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    handle = engine.queue_publish(topic, b"payload", qos=1)

    assert handle.mid is not None
    assert len(encodes) == 1


def test_qos1_connected_v311_utf8_encodes_topic_once() -> None:
    topic, encodes = _counting_str("é" * 256)
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv311))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish(topic, b"payload", qos=1)

    assert handle.mid is not None
    assert len(encodes) == 1


def test_qos1_topic_handoff_matches_direct_encode() -> None:
    topic = "capteurs/été/température"
    engine = _connected_engine()
    handle = engine.queue_publish(topic, b"payload", qos=1)
    sends = [effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND]
    assert len(sends) == 1

    expected = encode_publish_item_v5(
        topic,
        b"payload",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=handle.mid,
        properties=None,
    )
    assert write_item_bytes(sends[0]) == write_item_bytes(expected)
