"""Decoded MQTT 5 properties that fit the small-delivery limit use that path."""

from __future__ import annotations

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties


def _iot_properties() -> Properties:
    properties = Properties()
    properties.set("content_type", "application/octet-stream")
    properties.set("payload_format_indicator", 1)
    properties.set("message_expiry_interval", 60)
    properties.add_user_property("device", "probe")
    properties.add_user_property("site", "lab")
    return properties


def _decoded_qos0_message(properties: Properties | None, *, payload: bytes = b"hello") -> Message:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, client_id="probe"))
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    connack = decoder.next_packet()
    assert connack is not None
    engine.handle_raw(connack)
    engine.take_effects()

    wire = PublishPacket(
        topic="sensors/t",
        payload=payload,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=properties,
    ).encode(MQTTProtocolVersion.MQTTv5)
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    messages = [
        effect.data for effect in engine.take_effects() if effect.kind is EffectKind.MESSAGE
    ]
    assert len(messages) == 1
    return messages[0]


async def test_decoded_iot_properties_use_small_delivery() -> None:
    message = _decoded_qos0_message(_iot_properties())
    assert message.properties is not None
    assert message.properties._wire_size == len(encode_properties(message.properties, PUBLISH))

    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    item = client._messages.get_nowait()
    assert item is message
    assert client.pending_delivery_bytes == 0


async def test_application_built_properties_stay_accounted() -> None:
    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    message = Message(topic="sensors/t", payload=b"hello", properties=_iot_properties())
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert item[0] is message
    assert client.pending_delivery_bytes > 0


async def test_decoded_oversized_properties_stay_accounted() -> None:
    properties = Properties()
    properties.set("correlation_data", b"x" * 200)
    message = _decoded_qos0_message(properties)
    assert message.properties is not None
    assert message.properties._wire_size > 0

    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    item = client._messages.get_nowait()
    assert isinstance(item, tuple)
    assert client.pending_delivery_bytes > 0


async def test_decoded_oversized_properties_do_not_reencode_for_size(monkeypatch) -> None:
    properties = Properties()
    properties.set("correlation_data", b"x" * 200)
    message = _decoded_qos0_message(properties)
    assert message.properties is not None
    expected = len(message.payload) + len(message.topic) + message.properties._wire_size

    def unexpected_encode(_properties: object, _packet: object) -> bytes:
        raise AssertionError("decoded property table length must not be re-encoded")

    monkeypatch.setattr("mqttium.api._delivery.encode_properties", unexpected_encode)
    monkeypatch.setattr("mqttium.protocol.inbound.encode_properties", unexpected_encode)

    client = AsyncClient(
        message_delivery="iterator",
        protocol=MQTTProtocolVersion.MQTTv5,
        client_id="probe",
    )
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    item = client._messages.get_nowait()
    assert item == (message, expected)


def test_inbound_logical_size_uses_decoded_wire_size(monkeypatch) -> None:
    def unexpected_encode(_properties: object, _packet: object) -> bytes:
        raise AssertionError("decoded property table length must not be re-encoded")

    monkeypatch.setattr("mqttium.protocol.inbound.encode_properties", unexpected_encode)
    message = _decoded_qos0_message(_iot_properties())
    assert message.properties is not None
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5, client_id="probe"))
    size = engine.inbound.logical_size(message.topic, message.payload, message.properties)
    assert size == len(message.payload) + len(message.topic) + message.properties._wire_size


def test_decode_properties_records_wire_size() -> None:
    encoded = encode_properties(_iot_properties(), PUBLISH)
    decoded, end = decode_properties(encoded, 0, PUBLISH)
    assert end == len(encoded)
    assert decoded._wire_size == len(encoded)
    decoded.set("content_type", "text/plain")
    assert decoded._wire_size == 0


def test_inplace_values_mutation_does_not_clear_decoded_wire_size() -> None:
    """Inbound sizing trusts the decoded table; encode cache still signatures.

    Direct ``values`` writes are the encode-cache contract (Finding 2). Applying
    the same snapshot to ``_wire_size`` would re-walk every inbound PUBLISH.
    Accept runs before user code sees the message; ``set()`` still invalidates.
    """
    encoded = encode_properties(_iot_properties(), PUBLISH)
    decoded, _end = decode_properties(encoded, 0, PUBLISH)
    wire = decoded._wire_size
    assert wire > 0
    decoded.values["content_type"] = "text/plain"
    assert decoded._wire_size == wire
    decoded.set("payload_format_indicator", 0)
    assert decoded._wire_size == 0
