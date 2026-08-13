"""Specialized MQTT 3.1.1 acknowledgement encode/decode.

One function per packet type. The only legal body is a two-byte packet
identifier; anything else is malformed. Reason codes and properties do not
exist under 3.1.1.
"""

from __future__ import annotations

from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.types import Properties


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
