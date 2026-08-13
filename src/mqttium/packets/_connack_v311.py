"""Specialized MQTT 3.1.1 CONNACK decode."""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end, require_reason_code
from mqttium.codec.properties import CONNACK
from mqttium.errors import MalformedPacketError
from mqttium.types import Properties

_CONNACK_REASONS = frozenset({0, 1, 2, 3, 4, 5})


def decode_connack_v311(remaining: bytes) -> tuple[bool, int, Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError("CONNACK too short")
    if remaining[0] not in (0, 1):
        raise MalformedPacketError("CONNACK contains invalid acknowledge flags")
    session_present = bool(remaining[0])
    reason = remaining[1]
    require_reason_code(reason, _CONNACK_REASONS, CONNACK)
    require_end(2, len(remaining), CONNACK)
    if reason != 0 and session_present:
        raise MalformedPacketError(
            "CONNACK Session Present must be 0 when connection is refused"
        )
    return session_present, reason, None
