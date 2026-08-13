"""Specialized MQTT 3.1.1 acknowledgement encode/decode.

One function per packet type. The only legal body is a two-byte packet
identifier; anything else is malformed. Reason codes and properties do not
exist under 3.1.1.
"""

from __future__ import annotations

from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.types import Properties


def _success_frame(packet_type: PacketType, mid: int, *, flags: int = 0) -> bytes:
    return bytes((int(packet_type) | (flags & 0x0F), 2, mid >> 8, mid & 0xFF))


def _decode_mid(remaining: bytes, packet_name: str) -> int:
    size = len(remaining)
    if size != 2:
        raise MalformedPacketError(
            f"{packet_name} has {size - 2} unexpected trailing byte(s)"
            if size > 2
            else f"{packet_name} too short"
        )
    mid = (remaining[0] << 8) | remaining[1]
    if mid == 0:
        raise MalformedPacketError(f"{packet_name} packet identifier must not be 0")
    return mid


def decode_puback_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_mid(remaining, "PUBACK"), 0, None


def decode_pubrec_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_mid(remaining, "PUBREC"), 0, None


def decode_pubrel_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_mid(remaining, "PUBREL"), 0, None


def decode_pubcomp_v311(remaining: bytes) -> tuple[int, int, Properties | None]:
    return _decode_mid(remaining, "PUBCOMP"), 0, None


def encode_puback_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBACK packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBACK reason/properties require MQTT 5")
    return _success_frame(PacketType.PUBACK, mid)


def encode_pubrec_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBREC packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBREC reason/properties require MQTT 5")
    return _success_frame(PacketType.PUBREC, mid)


def encode_pubrel_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBREL packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBREL reason/properties require MQTT 5")
    return _success_frame(PacketType.PUBREL, mid, flags=0x02)


def encode_pubcomp_v311(
    mid: int,
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if not 1 <= mid <= 65535:
        raise ProtocolError("PUBCOMP packet identifier must be in 1..65535")
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("PUBCOMP reason/properties require MQTT 5")
    return _success_frame(PacketType.PUBCOMP, mid)
