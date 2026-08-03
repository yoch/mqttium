"""Symmetric MQTT UTF-8 validation for outbound and inbound strings."""

from __future__ import annotations

import pytest

from mqttium.codec.primitives import append_utf8, pack_utf8, unpack_utf8
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets import PublishPacket
from mqttium.types import Properties


@pytest.mark.parametrize("value", ["a\x00b", "\ufeff", "\ud800"])
def test_pack_utf8_rejects_forbidden_outbound_text(value: str) -> None:
    with pytest.raises(ProtocolError):
        pack_utf8(value)


def test_pack_utf8_rejects_raw_bytes() -> None:
    with pytest.raises(ProtocolError, match="must be str"):
        pack_utf8(b"raw")  # type: ignore[arg-type]


def test_append_utf8_uses_same_validation() -> None:
    with pytest.raises(ProtocolError):
        append_utf8(bytearray(), "bad\x00value")


def test_string_property_rejects_surrogate_as_protocol_error() -> None:
    properties = Properties()
    properties.set("content_type", "\ud800")
    with pytest.raises(ProtocolError):
        encode_properties(properties, PUBLISH)


def test_publish_topic_rejects_surrogate_as_protocol_error() -> None:
    packet = PublishPacket(
        topic="bad/\ud800",
        payload=b"x",
        qos=0,
        retain=False,
        dup=False,
    )
    with pytest.raises(ProtocolError):
        packet.encode()


def test_valid_unicode_roundtrip_is_unchanged() -> None:
    encoded = pack_utf8("capteur/électricité")
    decoded, offset = unpack_utf8(encoded)
    assert decoded == "capteur/électricité"
    assert offset == len(encoded)


def test_inbound_forbidden_text_remains_malformed_packet() -> None:
    wire = b"\x00\x03a\x00b"
    with pytest.raises(MalformedPacketError):
        unpack_utf8(wire)
