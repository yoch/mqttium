"""Version-specialized MQTT acknowledgement encode/decode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.types import Properties
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


def decode_puback_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size != 2:
        raise MalformedPacketError(
            f"PUBACK has {size - 2} unexpected trailing byte(s)" if size > 2 else "PUBACK too short"
        )
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBACK packet identifier must not be 0")
    return mid, 0, None


def decode_pubrec_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size != 2:
        raise MalformedPacketError(
            f"PUBREC has {size - 2} unexpected trailing byte(s)" if size > 2 else "PUBREC too short"
        )
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBREC packet identifier must not be 0")
    return mid, 0, None


def decode_pubrel_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size != 2:
        raise MalformedPacketError(
            f"PUBREL has {size - 2} unexpected trailing byte(s)" if size > 2 else "PUBREL too short"
        )
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBREL packet identifier must not be 0")
    return mid, 0, None


def decode_pubcomp_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size != 2:
        raise MalformedPacketError(
            f"PUBCOMP has {size - 2} unexpected trailing byte(s)"
            if size > 2
            else "PUBCOMP too short"
        )
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBCOMP packet identifier must not be 0")
    return mid, 0, None


def encode_puback_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBACK packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBACK reason/properties require MQTT 5")
    return bytes((0x40, 2, mid >> 8, mid & 0xFF))


def encode_pubrec_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBREC packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBREC reason/properties require MQTT 5")
    return bytes((0x50, 2, mid >> 8, mid & 0xFF))


def encode_pubrel_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBREL packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBREL reason/properties require MQTT 5")
    return bytes((0x62, 2, mid >> 8, mid & 0xFF))


def encode_pubcomp_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBCOMP packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBCOMP reason/properties require MQTT 5")
    return bytes((0x70, 2, mid >> 8, mid & 0xFF))


_PUBACK_REASONS = frozenset({0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99})


_PUBREC_REASONS = _PUBACK_REASONS


_PUBREL_REASONS = frozenset({0x00, 0x92})


_PUBCOMP_REASONS = _PUBREL_REASONS


def decode_puback_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size < 2:
        raise MalformedPacketError("PUBACK too short")
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBACK packet identifier must not be 0")
    if size == 2:
        return mid, 0, None
    reason = remaining[2]
    if reason not in _PUBACK_REASONS:
        raise MalformedPacketError(f"PUBACK contains invalid reason code 0x{reason:02x}")
    if size == 3:
        return mid, reason, None
    properties, pos = decode_properties(remaining, 3, PUBACK)
    require_end(pos, size, PUBACK)
    return mid, reason, properties


def decode_pubrec_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size < 2:
        raise MalformedPacketError("PUBREC too short")
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBREC packet identifier must not be 0")
    if size == 2:
        return mid, 0, None
    reason = remaining[2]
    if reason not in _PUBREC_REASONS:
        raise MalformedPacketError(f"PUBREC contains invalid reason code 0x{reason:02x}")
    if size == 3:
        return mid, reason, None
    properties, pos = decode_properties(remaining, 3, PUBREC)
    require_end(pos, size, PUBREC)
    return mid, reason, properties


def decode_pubrel_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size < 2:
        raise MalformedPacketError("PUBREL too short")
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBREL packet identifier must not be 0")
    if size == 2:
        return mid, 0, None
    reason = remaining[2]
    if reason not in _PUBREL_REASONS:
        raise MalformedPacketError(f"PUBREL contains invalid reason code 0x{reason:02x}")
    if size == 3:
        return mid, reason, None
    properties, pos = decode_properties(remaining, 3, PUBREL)
    require_end(pos, size, PUBREL)
    return mid, reason, properties


def decode_pubcomp_v5(remaining: bytes) -> tuple[int, int, Properties | None]:
    size = len(remaining)
    if size < 2:
        raise MalformedPacketError("PUBCOMP too short")
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError("PUBCOMP packet identifier must not be 0")
    if size == 2:
        return mid, 0, None
    reason = remaining[2]
    if reason not in _PUBCOMP_REASONS:
        raise MalformedPacketError(f"PUBCOMP contains invalid reason code 0x{reason:02x}")
    if size == 3:
        return mid, reason, None
    properties, pos = decode_properties(remaining, 3, PUBCOMP)
    require_end(pos, size, PUBCOMP)
    return mid, reason, properties


def encode_puback_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBACK packet identifier must be in 1..65535")
    if reason_code == 0 and not (properties and properties.values):
        return bytes((0x40, 2, mid >> 8, mid & 0xFF))
    if reason_code not in _PUBACK_REASONS:
        raise ProtocolError(f"PUBACK contains invalid reason code 0x{reason_code:02x}")
    if not (properties and properties.values):
        return bytes((0x40, 3, mid >> 8, mid & 0xFF, reason_code))
    body = bytearray((mid >> 8, mid & 0xFF, reason_code))
    body.extend(encode_properties(properties, PUBACK))
    header = bytearray((0x40,))
    append_vbi(header, len(body))
    header.extend(body)
    return bytes(header)


def encode_pubrec_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBREC packet identifier must be in 1..65535")
    if reason_code == 0 and not (properties and properties.values):
        return bytes((0x50, 2, mid >> 8, mid & 0xFF))
    if reason_code not in _PUBREC_REASONS:
        raise ProtocolError(f"PUBREC contains invalid reason code 0x{reason_code:02x}")
    if not (properties and properties.values):
        return bytes((0x50, 3, mid >> 8, mid & 0xFF, reason_code))
    body = bytearray((mid >> 8, mid & 0xFF, reason_code))
    body.extend(encode_properties(properties, PUBREC))
    header = bytearray((0x50,))
    append_vbi(header, len(body))
    header.extend(body)
    return bytes(header)


def encode_pubrel_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBREL packet identifier must be in 1..65535")
    if reason_code == 0 and not (properties and properties.values):
        return bytes((0x62, 2, mid >> 8, mid & 0xFF))
    if reason_code not in _PUBREL_REASONS:
        raise ProtocolError(f"PUBREL contains invalid reason code 0x{reason_code:02x}")
    if not (properties and properties.values):
        return bytes((0x62, 3, mid >> 8, mid & 0xFF, reason_code))
    body = bytearray((mid >> 8, mid & 0xFF, reason_code))
    body.extend(encode_properties(properties, PUBREL))
    header = bytearray((0x62,))
    append_vbi(header, len(body))
    header.extend(body)
    return bytes(header)


def encode_pubcomp_v5(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBCOMP packet identifier must be in 1..65535")
    if reason_code == 0 and not (properties and properties.values):
        return bytes((0x70, 2, mid >> 8, mid & 0xFF))
    if reason_code not in _PUBCOMP_REASONS:
        raise ProtocolError(f"PUBCOMP contains invalid reason code 0x{reason_code:02x}")
    if not (properties and properties.values):
        return bytes((0x70, 3, mid >> 8, mid & 0xFF, reason_code))
    body = bytearray((mid >> 8, mid & 0xFF, reason_code))
    body.extend(encode_properties(properties, PUBCOMP))
    header = bytearray((0x70,))
    append_vbi(header, len(body))
    header.extend(body)
    return bytes(header)
