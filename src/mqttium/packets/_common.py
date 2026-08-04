"""Framing and validation helpers shared by every packet module."""

from __future__ import annotations

from mqttium.codec.properties import encode_properties
from mqttium.codec.vbi import append_vbi
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.types import Properties

_EMPTY_PROPS_V5 = b"\x00"


def encode_frame(packet_type: PacketType, flags: int, remaining: bytes | bytearray) -> bytes:
    if flags & 0xF0:
        raise ValueError("flags must fit in low nibble")
    header = bytearray(1)
    header[0] = int(packet_type) | (flags & 0x0F)
    append_vbi(header, len(remaining))
    header.extend(remaining)
    return bytes(header)


def _props_or_empty(
    props: Properties | None,
    packet: str,
    protocol: MQTTProtocolVersion,
) -> bytes:
    if protocol != MQTTProtocolVersion.MQTTv5:
        return b""
    if not props or not props.values:
        return _EMPTY_PROPS_V5
    return encode_properties(props, packet)


def _require_outbound_mid(mid: int, what: str) -> None:
    if not 1 <= mid <= 65535:
        raise ProtocolError(f"{what} packet identifier must be in 1..65535")


def _require_outbound_reason(reason: int, allowed: frozenset[int], what: str) -> None:
    if reason not in allowed:
        raise ProtocolError(f"{what} contains invalid reason code 0x{reason:02x}")
