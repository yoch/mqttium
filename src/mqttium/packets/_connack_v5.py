"""Specialized MQTT 5 CONNACK decode."""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end, require_reason_code
from mqttium.codec.properties import CONNACK, decode_properties
from mqttium.errors import MalformedPacketError
from mqttium.types import Properties

_CONNACK_REASONS = frozenset(
    {
        0x00,
        0x80,
        0x81,
        0x82,
        0x83,
        0x84,
        0x85,
        0x86,
        0x87,
        0x88,
        0x89,
        0x8A,
        0x8C,
        0x90,
        0x95,
        0x97,
        0x99,
        0x9A,
        0x9B,
        0x9C,
        0x9D,
        0x9F,
    }
)


def decode_connack_v5(remaining: bytes) -> tuple[bool, int, Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError("CONNACK too short")
    if remaining[0] not in (0, 1):
        raise MalformedPacketError("CONNACK contains invalid acknowledge flags")
    session_present = bool(remaining[0])
    reason = remaining[1]
    require_reason_code(reason, _CONNACK_REASONS, CONNACK)
    properties, pos = decode_properties(remaining, 2, CONNACK)
    require_end(pos, len(remaining), CONNACK)
    if reason != 0 and session_present:
        raise MalformedPacketError(
            "CONNACK Session Present must be 0 when connection is refused"
        )
    return session_present, reason, properties
