from __future__ import annotations

import pytest

from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError
from mqttium.packets._publish import (
    decode_qos0_message_v311,
    decode_qos0_message_v5,
)
from mqttium.codec.buffer import RawPacket


@pytest.mark.parametrize(
    ("body", "flags", "error_text"),
    [
        (b"\x00\x01ax", 0x08, "QoS 0 PUBLISH must not set DUP"),
        (b"\x00", 0, "Incomplete uint16"),
        (b"\x00\x02a", 0, "Incomplete UTF-8 string"),
        (b"\x00\x01\xff", 0, "Invalid UTF-8 data"),
        (b"\x00\x00", 0, "PUBLISH topic must not be empty"),
    ],
)
def test_owned_v311_rejects_malformed_fields(body: bytes, flags: int, error_text: str) -> None:
    with pytest.raises(MalformedPacketError) as error:
        decode_qos0_message_v311(RawPacket(PacketType.PUBLISH, flags, body))
    assert error_text in str(error.value)


@pytest.mark.parametrize(
    ("body", "flags", "error_text"),
    [
        (b"\x00\x01a\x00", 0x08, "QoS 0 PUBLISH must not set DUP"),
        (b"\x00", 0, "Incomplete uint16"),
        (b"\x00\x02a", 0, "Incomplete UTF-8 string"),
        (b"\x00\x01\xff\x00", 0, "Invalid UTF-8 data"),
        (b"\x00\x01\x00\x00", 0, "Null in UTF-8 data"),
        (b"\x00\x01a", 0, "Missing properties length"),
        (b"\x00\x01a\x80", 0, "Incomplete Variable Byte Integer"),
        (b"\x00\x01a\x80\x00", 0, "Non-canonical Variable Byte Integer"),
        (b"\x00\x01a\xff\xff\xff\xff\x01", 0, "Malformed Variable Byte Integer"),
    ],
)
def test_owned_v5_rejects_malformed_fields(body: bytes, flags: int, error_text: str) -> None:
    with pytest.raises(MalformedPacketError) as error:
        decode_qos0_message_v5(RawPacket(PacketType.PUBLISH, flags, body))
    assert error_text in str(error.value)
