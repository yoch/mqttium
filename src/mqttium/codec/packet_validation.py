"""Fixed-header and typed-packet validation helpers."""

from __future__ import annotations

from collections.abc import Collection

from mqttium.codec.buffer import RawPacket
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError

# Packets with a fixed flags nibble (PUBLISH is special-cased).
_FIXED_FLAGS: dict[PacketType, int] = {
    PacketType.CONNECT: 0x0,
    PacketType.CONNACK: 0x0,
    PacketType.PUBACK: 0x0,
    PacketType.PUBREC: 0x0,
    PacketType.PUBREL: 0x2,
    PacketType.PUBCOMP: 0x0,
    PacketType.SUBSCRIBE: 0x2,
    PacketType.SUBACK: 0x0,
    PacketType.UNSUBSCRIBE: 0x2,
    PacketType.UNSUBACK: 0x0,
    PacketType.PINGREQ: 0x0,
    PacketType.PINGRESP: 0x0,
    PacketType.DISCONNECT: 0x0,
    PacketType.AUTH: 0x0,
}


def validate_raw_packet(raw: RawPacket) -> None:
    """Raise MalformedPacketError if fixed-header flags are illegal."""
    if raw.packet_type is PacketType.PUBLISH:
        qos = (raw.flags >> 1) & 0x03
        if qos == 3:
            raise MalformedPacketError("PUBLISH with QoS 3")
        return
    expected = _FIXED_FLAGS.get(raw.packet_type)
    if expected is None:
        raise MalformedPacketError(f"Unknown packet type {raw.packet_type!r}")
    if raw.flags != expected:
        raise MalformedPacketError(
            f"{raw.packet_type.name} flags 0x{raw.flags:x} != 0x{expected:x}"
        )


def require_nonzero_mid(mid: int, what: str) -> None:
    if mid == 0:
        raise MalformedPacketError(f"{what} packet identifier must not be 0")


def require_end(pos: int, length: int, what: str) -> None:
    """Require a typed decoder to consume the complete MQTT packet body."""
    if pos != length:
        raise MalformedPacketError(f"{what} has {length - pos} unexpected trailing byte(s)")


def require_reason_code(reason: int, allowed: Collection[int], what: str) -> None:
    """Reject reason codes that are not defined for the packet type."""
    if reason not in allowed:
        raise MalformedPacketError(f"{what} contains invalid reason code 0x{reason:02x}")
