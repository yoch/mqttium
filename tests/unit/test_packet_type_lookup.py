"""Packet type header lookup invariants."""

from __future__ import annotations

import pytest

from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError


@pytest.mark.parametrize("packet_type", PacketType)
def test_packet_type_from_byte_ignores_low_flags(packet_type: PacketType) -> None:
    for flags in range(16):
        assert PacketType.from_byte(packet_type.value | flags) is packet_type


@pytest.mark.parametrize("byte", range(16))
def test_packet_type_from_byte_rejects_reserved_zero_nibble(byte: int) -> None:
    with pytest.raises(
        MalformedPacketError,
        match=rf"Unknown MQTT packet type byte 0x{byte:02x}",
    ):
        PacketType.from_byte(byte)
