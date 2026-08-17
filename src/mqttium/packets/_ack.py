"""Version-specialized MQTT acknowledgement encode/decode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.

The asymmetry between the decoders and the encoders here is deliberate. The
**decoders** are what the engine binds and calls per acknowledgement, and each
one is a direct, helper-free implementation: that is the property measured in
`docs/reports/ACK-SPECIALIZED-PRIMITIVES.md` and it must not be factored away.
The **encoders** are not on that path at all — the engine writes success
acknowledgements from four-byte literals in the directional sessions, and the
only encoder it can reach is PUBREL, for an orphan PUBREC. Their sole remaining
consumers are the `Pub*Packet` dataclasses, so they share one implementation
per version rather than repeating it four times.
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


# Validation-free success acknowledgements. The directional sessions send these
# per message for an identifier the decoder already validated, so they skip the
# range and reason/property checks their `encode_*` twins below perform. Same
# bytes, no call depth: this is the shape ACK-SPECIALIZED-PRIMITIVES.md bought.
def encode_puback_success(mid: int) -> bytes:
    return bytes((0x40, 2, mid >> 8, mid & 0xFF))


def encode_pubrec_success(mid: int) -> bytes:
    return bytes((0x50, 2, mid >> 8, mid & 0xFF))


def encode_pubrel_success(mid: int) -> bytes:
    return bytes((0x62, 2, mid >> 8, mid & 0xFF))


def encode_pubcomp_success(mid: int) -> bytes:
    return bytes((0x70, 2, mid >> 8, mid & 0xFF))


def _encode_ack_v311(
    first_byte: int,
    packet: str,
    mid: int,
    reason_code: int,
    properties: Properties | None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError(f"{packet} packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError(f"{packet} reason/properties require MQTT 5")
    return bytes((first_byte, 2, mid >> 8, mid & 0xFF))


def encode_puback_v311(
    mid: int, reason_code: int = 0, properties: Properties | None = None
) -> bytes:
    return _encode_ack_v311(0x40, PUBACK, mid, reason_code, properties)


def encode_pubrec_v311(
    mid: int, reason_code: int = 0, properties: Properties | None = None
) -> bytes:
    return _encode_ack_v311(0x50, PUBREC, mid, reason_code, properties)


def encode_pubrel_v311(
    mid: int, reason_code: int = 0, properties: Properties | None = None
) -> bytes:
    return _encode_ack_v311(0x62, PUBREL, mid, reason_code, properties)


def encode_pubcomp_v311(
    mid: int, reason_code: int = 0, properties: Properties | None = None
) -> bytes:
    return _encode_ack_v311(0x70, PUBCOMP, mid, reason_code, properties)


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


def _encode_ack_v5(
    first_byte: int,
    packet: str,
    allowed: frozenset[int],
    mid: int,
    reason_code: int,
    properties: Properties | None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError(f"{packet} packet identifier must be in 1..65535")
    if reason_code == 0 and not (properties and properties.values):
        return bytes((first_byte, 2, mid >> 8, mid & 0xFF))
    if reason_code not in allowed:
        raise ProtocolError(f"{packet} contains invalid reason code 0x{reason_code:02x}")
    if not (properties and properties.values):
        return bytes((first_byte, 3, mid >> 8, mid & 0xFF, reason_code))
    body = bytearray((mid >> 8, mid & 0xFF, reason_code))
    body.extend(encode_properties(properties, packet))
    header = bytearray((first_byte,))
    append_vbi(header, len(body))
    header.extend(body)
    return bytes(header)


def encode_puback_v5(mid: int, reason_code: int = 0, properties: Properties | None = None) -> bytes:
    return _encode_ack_v5(0x40, PUBACK, _PUBACK_REASONS, mid, reason_code, properties)


def encode_pubrec_v5(mid: int, reason_code: int = 0, properties: Properties | None = None) -> bytes:
    return _encode_ack_v5(0x50, PUBREC, _PUBREC_REASONS, mid, reason_code, properties)


def encode_pubrel_v5(mid: int, reason_code: int = 0, properties: Properties | None = None) -> bytes:
    return _encode_ack_v5(0x62, PUBREL, _PUBREL_REASONS, mid, reason_code, properties)


def encode_pubcomp_v5(
    mid: int, reason_code: int = 0, properties: Properties | None = None
) -> bytes:
    return _encode_ack_v5(0x70, PUBCOMP, _PUBCOMP_REASONS, mid, reason_code, properties)
