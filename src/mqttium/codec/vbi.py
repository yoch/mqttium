"""MQTT Variable Byte Integer encode/decode."""

from __future__ import annotations

from mqttium.errors import MalformedPacketError

_MAX_VBI = 268_435_455

# Precomputed single-byte VBIs (remaining length / prop length < 128).
_VBI_ONE = tuple(bytes((i,)) for i in range(128))


def encode_vbi(value: int) -> bytes:
    if value < 128:
        if value < 0:
            raise ValueError(f"VBI out of range: {value}")
        return _VBI_ONE[value]
    if value > _MAX_VBI:
        raise ValueError(f"VBI out of range: {value}")
    out = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value > 0:
            digit |= 0x80
        out.append(digit)
        if value == 0:
            break
    return bytes(out)


def append_vbi(buf: bytearray, value: int) -> None:
    """Encode a VBI directly into *buf* (avoids intermediate bytes)."""
    if not 0 <= value <= _MAX_VBI:
        raise ValueError(f"VBI out of range: {value}")
    while True:
        digit = value % 128
        value //= 128
        if value > 0:
            digit |= 0x80
        buf.append(digit)
        if value == 0:
            break


def decode_vbi(buffer: bytes | bytearray | memoryview, offset: int = 0) -> tuple[int, int]:
    """Decode a canonical MQTT VBI starting at *offset*.

    Returns ``(value, new_offset)``. MQTT requires the shortest possible
    representation, so encodings such as ``80 00`` are malformed even though
    they numerically represent zero.
    """
    length = len(buffer)
    if offset >= length:
        raise MalformedPacketError("Incomplete Variable Byte Integer")
    first = buffer[offset]
    if first < 128:
        return first, offset + 1

    multiplier = 128
    value = first & 0x7F
    pos = offset + 1
    encoded_bytes = 1
    while True:
        if pos >= length:
            raise MalformedPacketError("Incomplete Variable Byte Integer")
        encoded_bytes += 1
        if encoded_bytes > 4:
            raise MalformedPacketError("Malformed Variable Byte Integer (too long)")
        byte = buffer[pos]
        pos += 1
        value += (byte & 0x7F) * multiplier
        if byte & 0x80 == 0:
            break
        multiplier *= 128
        if multiplier > 128 * 128 * 128:
            raise MalformedPacketError("Malformed Variable Byte Integer")
    if value > _MAX_VBI:
        raise MalformedPacketError("Variable Byte Integer exceeds maximum")
    if encoded_bytes != vbi_len(value):
        raise MalformedPacketError("Non-canonical Variable Byte Integer")
    return value, pos


def vbi_len(value: int) -> int:
    if not 0 <= value <= _MAX_VBI:
        raise ValueError(f"VBI out of range: {value}")
    if value < 128:
        return 1
    if value < 16_384:
        return 2
    if value < 2_097_152:
        return 3
    return 4
