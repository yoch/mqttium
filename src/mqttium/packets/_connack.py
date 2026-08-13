"""Version-specialized MQTT CONNACK decode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end, require_reason_code
from mqttium.codec.properties import CONNACK
from mqttium.errors import MalformedPacketError
from mqttium.types import Properties
from mqttium.codec.properties import decode_properties


_CONNACK_V311_REASONS = frozenset({0, 1, 2, 3, 4, 5})


def decode_connack_v311(remaining: bytes) -> tuple[bool, int, Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError("CONNACK too short")
    if remaining[0] not in (0, 1):
        raise MalformedPacketError("CONNACK contains invalid acknowledge flags")
    session_present = bool(remaining[0])
    reason = remaining[1]
    require_reason_code(reason, _CONNACK_V311_REASONS, CONNACK)
    require_end(2, len(remaining), CONNACK)
    if reason != 0 and session_present:
        raise MalformedPacketError("CONNACK Session Present must be 0 when connection is refused")
    return session_present, reason, None


_CONNACK_V5_REASONS = frozenset(
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
    require_reason_code(reason, _CONNACK_V5_REASONS, CONNACK)
    properties, pos = decode_properties(remaining, 2, CONNACK)
    require_end(pos, len(remaining), CONNACK)
    if reason != 0 and session_present:
        raise MalformedPacketError("CONNACK Session Present must be 0 when connection is refused")
    return session_present, reason, properties
