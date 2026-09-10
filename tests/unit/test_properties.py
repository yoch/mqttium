"""MQTT 5 properties codec tests (IMPLEMENTATION-GUIDE §12)."""

from __future__ import annotations

import pytest

from mqttium.codec.properties import (
    CONNACK,
    PUBLISH,
    SUBSCRIBE,
    decode_properties,
    encode_properties,
)
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.types import Properties


def test_empty_fast_path() -> None:
    assert encode_properties(None, PUBLISH) == b"\x00"
    assert encode_properties(Properties(), PUBLISH) == b"\x00"
    props, end = decode_properties(b"\x00", 0, PUBLISH)
    assert not props
    assert end == 1


def test_roundtrip_common_publish_props() -> None:
    props = Properties()
    props = Properties({**props.values, "payload_format_indicator": 1})
    props = Properties({**props.values, "message_expiry_interval": 60})
    props = Properties({**props.values, "content_type": "application/json"})
    props = Properties({**props.values, "response_topic": "resp/t"})
    props = Properties({**props.values, "correlation_data": b"corr"})
    props = Properties({**props.values, "topic_alias": 7})
    props = Properties(
        {**props.values, "user_property": (*props.get("user_property", ()), ("a", "1"))}
    )
    props = Properties(
        {**props.values, "user_property": (*props.get("user_property", ()), ("b", "2"))}
    )
    props = Properties({**props.values, "subscription_identifier": [42, 64]})

    encoded = encode_properties(props, PUBLISH)
    decoded, end = decode_properties(encoded, 0, PUBLISH)
    assert end == len(encoded)
    assert decoded.get("payload_format_indicator") == 1
    assert decoded.get("message_expiry_interval") == 60
    assert decoded.get("content_type") == "application/json"
    assert decoded.get("response_topic") == "resp/t"
    assert decoded.get("correlation_data") == b"corr"
    assert decoded.get("topic_alias") == 7
    assert decoded.get("user_property") == (("a", "1"), ("b", "2"))
    assert decoded.get("subscription_identifier") == (42, 64)


def test_roundtrip_connack_props() -> None:
    props = Properties()
    props = Properties({**props.values, "receive_maximum": 100})
    props = Properties({**props.values, "maximum_qos": 1})
    props = Properties({**props.values, "retain_available": 0})
    props = Properties({**props.values, "maximum_packet_size": 1024})
    props = Properties({**props.values, "topic_alias_maximum": 10})
    props = Properties({**props.values, "server_keep_alive": 30})
    props = Properties({**props.values, "assigned_client_identifier": "assigned-1"})
    props = Properties({**props.values, "session_expiry_interval": 3600})
    encoded = encode_properties(props, CONNACK)
    decoded, _ = decode_properties(encoded, 0, CONNACK)
    assert decoded.get("receive_maximum") == 100
    assert decoded.get("maximum_qos") == 1
    assert decoded.get("retain_available") == 0
    assert decoded.get("maximum_packet_size") == 1024
    assert decoded.get("assigned_client_identifier") == "assigned-1"


def test_duplicate_singleton_rejected() -> None:
    # Manually craft: length + two topic_alias properties
    from mqttium.codec.vbi import encode_vbi
    from mqttium.codec.primitives import pack_u16

    body = bytes([0x23]) + pack_u16(1) + bytes([0x23]) + pack_u16(2)
    wire = encode_vbi(len(body)) + body
    with pytest.raises(MalformedPacketError, match="Duplicate"):
        decode_properties(wire, 0, PUBLISH)


def test_property_not_allowed_on_packet() -> None:
    props = Properties()
    props = Properties({**props.values, "topic_alias": 1})
    with pytest.raises(ProtocolError, match="not allowed"):
        encode_properties(props, CONNACK)


def test_unknown_property_id() -> None:
    from mqttium.codec.vbi import encode_vbi

    body = bytes([0xFE, 0x01])
    wire = encode_vbi(len(body)) + body
    with pytest.raises(MalformedPacketError, match="Unknown property"):
        decode_properties(wire, 0, PUBLISH)


def test_subscription_identifier_zero_forbidden() -> None:
    props = Properties()
    props = Properties({**props.values, "subscription_identifier": 0})
    with pytest.raises(ProtocolError, match="must not be zero"):
        encode_properties(props, PUBLISH)


def test_subscribe_single_subscription_identifier() -> None:
    props = Properties()
    props = Properties({**props.values, "subscription_identifier": [1, 2]})
    with pytest.raises(ProtocolError, match="one subscription_identifier"):
        encode_properties(props, SUBSCRIBE)


def test_length_mismatch() -> None:
    # Claim length 5 but only provide 2 bytes after VBI.
    wire = bytes([0x05, 0x01, 0x00])
    with pytest.raises(MalformedPacketError, match="exceeds"):
        decode_properties(wire, 0, PUBLISH)


def test_receive_maximum_zero_forbidden() -> None:
    props = Properties()
    props = Properties({**props.values, "receive_maximum": 0})
    with pytest.raises(ProtocolError, match="must not be zero"):
        encode_properties(props, CONNACK)


@pytest.mark.parametrize("value", [-1, 268_435_456, "1", None])
def test_subscription_identifier_rejects_invalid_vbi_values(value: object) -> None:
    props = Properties()
    props = Properties({**props.values, "subscription_identifier": value})

    with pytest.raises(ProtocolError, match="Invalid VBI property value"):
        encode_properties(props, PUBLISH)


def test_subscription_identifier_accepts_maximum_vbi_value() -> None:
    props = Properties()
    props = Properties({**props.values, "subscription_identifier": 268435455})

    encoded = encode_properties(props, PUBLISH)
    decoded, offset = decode_properties(encoded, 0, PUBLISH)

    assert offset == len(encoded)
    assert decoded.get("subscription_identifier") == (268435455,)


def test_binary_property_rejects_values_larger_than_mqtt_u16_length() -> None:
    props = Properties()
    props = Properties({**props.values, "correlation_data": b"x" * 65536})

    with pytest.raises(ProtocolError, match="Binary data too long") as caught:
        encode_properties(props, PUBLISH)

    assert isinstance(caught.value.__cause__, ValueError)


@pytest.mark.parametrize(
    "value",
    [
        ("valid-key", 1),
        (object(), "valid-value"),
        ("missing-value",),
        ["not", "a", "pair"],
    ],
)
def test_user_property_rejects_malformed_pairs(value: object) -> None:
    with pytest.raises(ProtocolError):
        encode_properties(Properties({"user_property": value}), PUBLISH)


def test_encode_properties_reuses_unchanged_table_bytes() -> None:
    props = Properties()
    props = Properties(
        {**props.values, "user_property": (*props.get("user_property", ()), ("source", "cache"))}
    )
    first = encode_properties(props, PUBLISH)
    second = encode_properties(props, PUBLISH)

    assert second is first


def test_properties_own_mutable_inputs_and_cache_remains_valid() -> None:
    binary = bytearray(b"initial")
    pairs = [["key", "value"]]
    values = {"user_property": pairs, "correlation_data": binary}
    props = Properties(values)
    first = encode_properties(props, PUBLISH)
    binary[:] = b"changed"
    pairs[0][1] = "changed"
    pairs.append(["other", "entry"])
    values["content_type"] = "text/plain"
    assert props.get("user_property") == (("key", "value"),)
    assert props.get("correlation_data") == b"initial"
    assert encode_properties(props, PUBLISH) is first
    with pytest.raises(TypeError):
        props.values["content_type"] = "changed"
    with pytest.raises(AttributeError):
        props.values["user_property"].append(("new", "entry"))
    decoded, _ = decode_properties(first, 0, PUBLISH)
    assert decoded == props
