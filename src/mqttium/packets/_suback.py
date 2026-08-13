"""Version-specialized MQTT SUBACK and UNSUBACK decode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end, require_nonzero_mid, require_reason_code
from mqttium.codec.primitives import unpack_u16
from mqttium.codec.properties import SUBACK, UNSUBACK
from mqttium.errors import MalformedPacketError
from mqttium.types import Properties
from mqttium.codec.properties import decode_properties


_SUBACK_V311_REASONS = frozenset({0x00, 0x01, 0x02, 0x80})


def decode_suback_v311(
    remaining: bytes,
) -> tuple[int, tuple[int, ...], Properties | None]:
    if len(remaining) < 3:
        raise MalformedPacketError("SUBACK too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, SUBACK)
    reason_codes = tuple(remaining[pos:])
    for reason in reason_codes:
        require_reason_code(reason, _SUBACK_V311_REASONS, SUBACK)
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


_SUBACK_V5_REASONS = frozenset(
    {0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2}
)


_UNSUBACK_REASONS = frozenset({0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91})


def decode_suback_v5(
    remaining: bytes,
) -> tuple[int, tuple[int, ...], Properties | None]:
    if len(remaining) < 3:
        raise MalformedPacketError("SUBACK too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, SUBACK)
    properties, pos = decode_properties(remaining, pos, SUBACK)
    if pos >= len(remaining):
        raise MalformedPacketError("SUBACK missing reason codes")
    reason_codes = tuple(remaining[pos:])
    for reason in reason_codes:
        require_reason_code(reason, _SUBACK_V5_REASONS, SUBACK)
    return mid, reason_codes, properties


def decode_unsuback_v5(
    remaining: bytes,
) -> tuple[int, tuple[int, ...], Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError("UNSUBACK too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, UNSUBACK)
    properties, pos = decode_properties(remaining, pos, UNSUBACK)
    if pos >= len(remaining):
        raise MalformedPacketError("UNSUBACK missing reason codes")
    reason_codes = tuple(remaining[pos:])
    for reason in reason_codes:
        require_reason_code(reason, _UNSUBACK_REASONS, UNSUBACK)
    return mid, reason_codes, properties
