"""Specialized MQTT 5 acknowledgement encode/decode.

One function per packet type. The common two- and three-byte bodies are the
body of the decoder; property tables are the uncommon fall-through inside the
same function. Absent properties return ``None`` (never an empty
``Properties()``).
"""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end
from mqttium.codec.properties import (
    PUBACK,
    PUBCOMP,
    PUBREC,
    PUBREL,
    decode_properties,
    encode_properties,
)
from mqttium.codec.vbi import append_vbi
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.types import Properties

_PUBACK_REASONS = frozenset({0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99})
_PUBREC_REASONS = _PUBACK_REASONS
_PUBREL_REASONS = frozenset({0x00, 0x92})
_PUBCOMP_REASONS = _PUBREL_REASONS


def _success_frame(packet_type: PacketType, mid: int, *, flags: int = 0) -> bytes:
    return bytes((int(packet_type) | (flags & 0x0F), 2, mid >> 8, mid & 0xFF))


def _reason_frame(
    packet_type: PacketType,
    mid: int,
    reason: int,
    *,
    flags: int = 0,
) -> bytes:
    return bytes((int(packet_type) | (flags & 0x0F), 3, mid >> 8, mid & 0xFF, reason))


def _decode_ack_v5(
    remaining: bytes,
    packet_name: str,
    allowed: frozenset[int],
) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size < 2:
        raise MalformedPacketError(f"{packet_name} too short")
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError(f"{packet_name} packet identifier must not be 0")
    if size == 2:
        # Success with omitted zero reason code and no property table.
        return mid, 0, None
    reason = remaining[2]
    if reason not in allowed:
        raise MalformedPacketError(f"{packet_name} contains invalid reason code 0x{reason:02x}")
    if size == 3:
        # Explicit reason, no property table — Mosquitto's common PUBACK/PUBREC
        # shape (e.g. 0x10 No matching subscribers).
        return mid, reason, None
    properties, pos = decode_properties(remaining, 3, packet_name)
    require_end(pos, size, packet_name)
    return mid, reason, properties


def decode_puback_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_ack_v5(remaining, PUBACK, _PUBACK_REASONS)


def decode_pubrec_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_ack_v5(remaining, PUBREC, _PUBREC_REASONS)


def decode_pubrel_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_ack_v5(remaining, PUBREL, _PUBREL_REASONS)


def decode_pubcomp_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_ack_v5(remaining, PUBCOMP, _PUBCOMP_REASONS)


def _encode_ack_v5(
    packet_type: PacketType,
    packet_name: str,
    allowed: frozenset[int],
    mid: int,
    reason_code: int,
    properties: Properties | None,
    *,
    flags: int = 0,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError(f"{packet_name} packet identifier must be in 1..65535")
    if reason_code == 0 and not (properties and properties.values):
        return _success_frame(packet_type, mid, flags=flags)
    if reason_code not in allowed:
        raise ProtocolError(f"{packet_name} contains invalid reason code 0x{reason_code:02x}")
    if not (properties and properties.values):
        return _reason_frame(packet_type, mid, reason_code, flags=flags)
    body = bytearray((mid >> 8, mid & 0xFF, reason_code))
    body.extend(encode_properties(properties, packet_name))
    header = bytearray(1)
    header[0] = int(packet_type) | (flags & 0x0F)
    append_vbi(header, len(body))
    header.extend(body)
    return bytes(header)


def encode_puback_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    return _encode_ack_v5(
        PacketType.PUBACK, PUBACK, _PUBACK_REASONS, mid, reason_code, properties
    )


def encode_pubrec_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    return _encode_ack_v5(
        PacketType.PUBREC, PUBREC, _PUBREC_REASONS, mid, reason_code, properties
    )


def encode_pubrel_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    return _encode_ack_v5(
        PacketType.PUBREL,
        PUBREL,
        _PUBREL_REASONS,
        mid,
        reason_code,
        properties,
        flags=0x02,
    )


def encode_pubcomp_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    return _encode_ack_v5(
        PacketType.PUBCOMP, PUBCOMP, _PUBCOMP_REASONS, mid, reason_code, properties
    )
