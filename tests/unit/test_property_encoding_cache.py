"""Cross-layer contracts for cached MQTT 5 property encoding."""

from __future__ import annotations

import mqttium.codec.properties as properties_module

from mqttium.api.async_client import AsyncClient
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Message, Properties


def test_delivery_logical_size_reuses_cached_mqtt5_property_body(monkeypatch) -> None:
    calls = 0
    original = properties_module._encode_value

    def counting(prop_type: object, value: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(prop_type, value)

    monkeypatch.setattr(properties_module, "_encode_value", counting)
    properties = Properties()
    properties.add_user_property("source", "delivery")
    message = Message(topic="cache/topic", payload=b"payload", properties=properties)
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5)

    first = client._delivery.logical_size(message)
    second = client._delivery.logical_size(message)

    assert first == second
    assert calls == 1


def test_binary_property_cache_detects_in_place_bytearray_mutation() -> None:
    data = bytearray(b"before")
    properties = Properties()
    properties.set("correlation_data", data)

    first = encode_properties(properties, PUBLISH)
    data[:] = b"after"
    second = encode_properties(properties, PUBLISH)

    assert first != second
    decoded, pos = decode_properties(second, 0, PUBLISH)
    assert pos == len(second)
    assert decoded.get("correlation_data") == b"after"
