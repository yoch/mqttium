"""Framing and validation helpers shared by every packet module."""

from __future__ import annotations

from mqttium.codec.vbi import append_vbi
from mqttium.enums import PacketType
from mqttium.errors import ProtocolError
from mqttium.types import Properties


def encode_frame(packet_type: PacketType, flags: int, remaining: bytes | bytearray) -> bytes:
    if flags & 0xF0:
        raise ValueError("flags must fit in low nibble")
    header = bytearray(1)
    header[0] = int(packet_type) | (flags & 0x0F)
    append_vbi(header, len(remaining))
    header.extend(remaining)
    return bytes(header)


def _require_outbound_mid(mid: int, what: str) -> None:
    if not 1 <= mid <= 65535:
        raise ProtocolError(f"{what} packet identifier must be in 1..65535")


def _require_outbound_reason(reason: int, allowed: frozenset[int], what: str) -> None:
    if reason not in allowed:
        raise ProtocolError(f"{what} contains invalid reason code 0x{reason:02x}")


def validate_payload_format(payload: bytes, properties: Properties | None) -> None:
    """Enforce the sender-side MQTT 5 Payload Format Indicator contract."""
    if properties is None or properties.get("payload_format_indicator") != 1:
        return
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProtocolError(
            "Payload Format Indicator 1 requires a well-formed UTF-8 payload"
        ) from exc
