"""Specialized MQTT 3.1.1 SUBACK and UNSUBACK decode."""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end, require_nonzero_mid, require_reason_code
from mqttium.codec.primitives import unpack_u16
from mqttium.codec.properties import SUBACK, UNSUBACK
from mqttium.errors import MalformedPacketError
from mqttium.types import Properties

_SUBACK_REASONS = frozenset({0x00, 0x01, 0x02, 0x80})


def decode_suback_v311(
    remaining: bytes,
) -> tuple[int, tuple[int, ...], Properties | None]:
    if len(remaining) < 3:
        raise MalformedPacketError("SUBACK too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, SUBACK)
    reason_codes = tuple(remaining[pos:])
    for reason in reason_codes:
        require_reason_code(reason, _SUBACK_REASONS, SUBACK)
    return mid, reason_codes, None


def decode_unsuback_v311(
    remaining: bytes,
) -> tuple[int, tuple[int, ...], Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError("UNSUBACK too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, UNSUBACK)
    require_end(pos, len(remaining), UNSUBACK)
    return mid, (), None
