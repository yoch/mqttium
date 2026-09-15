"""Decoded MQTT 5 property sizes may use the count-bounded small reserve."""

from __future__ import annotations


import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


def _iot_properties() -> Properties:
    properties = Properties()
    properties = Properties({**properties.values, "content_type": "application/octet-stream"})
    properties = Properties({**properties.values, "payload_format_indicator": 1})
    properties = Properties({**properties.values, "message_expiry_interval": 60})
    properties = Properties(
        {
            **properties.values,
            "user_property": (*properties.get("user_property", ()), ("device", "probe")),
        }
    )
    properties = Properties(
        {
            **properties.values,
            "user_property": (*properties.get("user_property", ()), ("site", "lab")),
        }
    )
    return properties


def _connected_engine() -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, client_id="probe"))
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    connack = decoder.next_packet()
    assert connack is not None
    engine.handle_raw(connack)
    engine.take_effects()
    return engine


def _fresh_message_effect(
    properties: Properties | None,
    *,
    qos: QoS = QoS.AT_MOST_ONCE,
    payload: bytes = b"hello",
    mid: int | None = None,
) -> tuple[ProtocolEngine, EngineEffect]:
    engine = _connected_engine()
    packet = PublishPacket(
        topic="sensors/t",
        payload=payload,
        qos=qos,
        retain=False,
        dup=False,
        mid=mid,
        properties=properties,
    )
    decoder = IncrementalDecoder()
    decoder.feed(packet.encode(MQTTProtocolVersion.MQTTv5))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    effects = engine.take_effects()
    message_effects = [
        effect
        for effect in effects
        if effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
    ]
    assert len(message_effects) == 1
    return engine, message_effects[0]


def test_inbound_logical_size_reuses_fresh_decoded_table(monkeypatch) -> None:
    engine, effect = _fresh_message_effect(_iot_properties())
    message = effect.data
    assert isinstance(message, Message)
    assert message.properties is not None
    property_wire_size = effect.decoded_property_wire_size
    assert property_wire_size is not None

    def fail_encode(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("fresh inbound sizing must reuse the decoded table size")

    # The formula moved to protocol/_sizing.py, shared with OutboundSession.
    monkeypatch.setattr("mqttium.protocol._sizing.encode_properties", fail_encode)
    size = engine.inbound.logical_size(
        message.topic, message.payload, message.properties, property_wire_size
    )
    assert size == len(message.payload) + len(message.topic) + property_wire_size


def test_empty_decoded_property_table_keeps_generic_path() -> None:
    _engine, effect = _fresh_message_effect(Properties())
    assert effect.kind is EffectKind.MESSAGE
    assert effect.decoded_property_wire_size is None


@pytest.mark.parametrize(
    ("qos", "mid"),
    [
        (QoS.AT_MOST_ONCE, None),
        (QoS.AT_LEAST_ONCE, 7),
        (QoS.EXACTLY_ONCE, 7),
    ],
)
def test_all_fresh_property_bearing_qos_levels_use_decoded_effect_kind(
    qos: QoS, mid: int | None
) -> None:
    _engine, effect = _fresh_message_effect(_iot_properties(), qos=qos, mid=mid)
    assert effect.kind is EffectKind.DECODED_MESSAGE
    assert effect.decoded_property_wire_size is not None
