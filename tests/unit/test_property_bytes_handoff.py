"""MQTT 5 QoS 1/2 admission reuses the property table encoded for sizing."""

from __future__ import annotations

import pytest

import mqttium.packets._publish as publish_v5_module
import mqttium.protocol.outbound as outbound_module

from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PubAckPacket
from mqttium.packets._publish import encode_publish_item_v5
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties
from tests.support import feed_engine, write_item_bytes


def _iot_properties() -> Properties:
    properties = Properties()
    properties.set("content_type", "application/octet-stream")
    properties.set("payload_format_indicator", 1)
    properties.set("message_expiry_interval", 60)
    properties.add_user_property("device", "probe")
    properties.add_user_property("site", "lab")
    return properties


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_connected_qos12_validation_hands_property_bytes_to_encoder(monkeypatch, qos: QoS) -> None:
    admission_calls = 0
    encoder_calls = 0
    original_admission = outbound_module.encode_properties
    original_encoder = publish_v5_module.encode_properties

    def admission_encode(properties: object, packet: object) -> bytes:
        nonlocal admission_calls
        admission_calls += 1
        return original_admission(properties, packet)

    def encoder_encode(properties: object, packet: object) -> bytes:
        nonlocal encoder_calls
        encoder_calls += 1
        return original_encoder(properties, packet)

    monkeypatch.setattr(outbound_module, "encode_properties", admission_encode)
    monkeypatch.setattr(publish_v5_module, "encode_properties", encoder_encode)

    engine = _connected_engine()
    handle = engine.queue_publish("capteurs/qos", b"payload", qos=qos, properties=_iot_properties())

    assert handle.mid is not None
    assert admission_calls == 1
    assert encoder_calls == 0


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_connected_qos12_property_handoff_matches_direct_encode(qos: QoS) -> None:
    properties = _iot_properties()
    engine = _connected_engine()
    handle = engine.queue_publish("capteurs/qos", b"payload", qos=qos, properties=properties)
    sends = [effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND]
    assert len(sends) == 1

    expected = encode_publish_item_v5(
        "capteurs/qos",
        b"payload",
        qos=qos,
        retain=False,
        dup=False,
        mid=handle.mid,
        properties=properties,
    )
    assert write_item_bytes(sends[0]) == write_item_bytes(expected)


def test_encoder_property_bytes_match_fresh_encode() -> None:
    properties = _iot_properties()
    encoded = encode_properties(properties, PUBLISH)
    handed = encode_publish_item_v5(
        "t",
        b"p",
        qos=1,
        retain=False,
        dup=False,
        mid=7,
        properties=properties,
        _property_bytes=encoded,
    )
    fresh = encode_publish_item_v5(
        "t",
        b"p",
        qos=1,
        retain=False,
        dup=False,
        mid=7,
        properties=properties,
    )
    assert handed == fresh


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_empty_mqtt5_table_does_not_call_encode_properties(monkeypatch, qos: QoS) -> None:
    def unexpected_encode(properties: object, packet: object) -> bytes:
        raise AssertionError("empty MQTT 5 property table must use the 0x00 fast path")

    monkeypatch.setattr(outbound_module, "encode_properties", unexpected_encode)
    monkeypatch.setattr(publish_v5_module, "encode_properties", unexpected_encode)

    engine = _connected_engine()
    handle = engine.queue_publish("capteurs/qos", b"payload", qos=qos)

    assert handle.mid is not None


def test_queued_publish_reencodes_current_properties_when_drain_launches(monkeypatch) -> None:
    """Admission bytes must not freeze mutable properties while a publish waits."""

    encoder_calls = 0
    original_encoder = publish_v5_module.encode_properties

    def encoder_encode(properties: object, packet: object) -> bytes:
        nonlocal encoder_calls
        encoder_calls += 1
        return original_encoder(properties, packet)

    monkeypatch.setattr(publish_v5_module, "encode_properties", encoder_encode)

    engine = _connected_engine()
    engine.outbound.flow.limit = 1

    blocker = engine.queue_publish("capteurs/blocker", b"one", qos=QoS.AT_LEAST_ONCE)
    assert blocker.mid is not None
    engine.take_effects()

    queued_properties = _iot_properties()
    queued = engine.queue_publish(
        "capteurs/queued",
        b"two",
        qos=QoS.AT_LEAST_ONCE,
        properties=queued_properties,
    )
    assert queued.mid is not None
    assert engine.outbound.stats().queued_messages == 1
    assert encoder_calls == 0

    # Direct mutation is part of the Properties cache contract. Keep the encoded
    # width unchanged so this test is only about which property snapshot reaches
    # the delayed wire encode, not about pending-byte accounting.
    queued_properties.values["message_expiry_interval"] = 61

    feed_engine(engine, PubAckPacket(mid=blocker.mid).encode(engine.config.protocol))
    effects = engine.take_effects()
    sends = [effect.data for effect in effects if effect.kind is EffectKind.SEND]

    assert encoder_calls == 1
    assert len(sends) == 1
    expected = encode_publish_item_v5(
        "capteurs/queued",
        b"two",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=queued.mid,
        properties=queued_properties,
    )
    assert write_item_bytes(sends[0]) == write_item_bytes(expected)
