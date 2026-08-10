"""Binary primitives used by MQTT codecs."""

from __future__ import annotations

import struct
from typing import Final

from mqttium.errors import MalformedPacketError, ProtocolError

_U16: Final = struct.Struct("!H")
_U32: Final = struct.Struct("!I")


def pack_u16(value: int) -> bytes:
    return _U16.pack(value)


def append_u16(buf: bytearray, value: int) -> None:
    buf += _U16.pack(value)


def unpack_u16(buffer: bytes | bytearray | memoryview, offset: int = 0) -> tuple[int, int]:
    if offset + 2 > len(buffer):
        raise MalformedPacketError("Incomplete uint16")
    return _U16.unpack_from(buffer, offset)[0], offset + 2


def pack_u32(value: int) -> bytes:
    return _U32.pack(value)


def unpack_u32(buffer: bytes | bytearray | memoryview, offset: int = 0) -> tuple[int, int]:
    if offset + 4 > len(buffer):
        raise MalformedPacketError("Incomplete uint32")
    return _U32.unpack_from(buffer, offset)[0], offset + 4


def validate_utf8(value: str) -> None:
    """Apply the MQTT UTF-8 rules without building the encoded form.

    For a caller that only wants the rejection -- topic validation, which runs
    before the encoder that will produce the bytes anyway -- ``encode_utf8``
    encodes a string whose result is then discarded. A code point never encodes
    to fewer than one byte, so an ASCII string's character count is already its
    byte count and needs no encoding at all. Anything else is encoded exactly
    once, for the length bound, and that same encoding answers the surrogate
    rule.
    """
    if not isinstance(value, str):
        raise ProtocolError(f"MQTT UTF-8 value must be str, got {type(value).__name__}")
    _validate_mqtt_utf8(value, error_type=ProtocolError)
    if value.isascii():
        if len(value) > 65535:
            raise ProtocolError("UTF-8 string too long for MQTT")
        return
    try:
        data = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ProtocolError("[MQTT-1.5.4-1] Surrogate in UTF-8 data") from exc
    if len(data) > 65535:
        raise ProtocolError("UTF-8 string too long for MQTT")


def encode_utf8(value: str) -> bytes:
    """Validate and encode one MQTT UTF-8 string without its length prefix."""
    if not isinstance(value, str):
        raise ProtocolError(f"MQTT UTF-8 value must be str, got {type(value).__name__}")
    try:
        data = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        # Surrogates are the only code points UTF-8 refuses, so this branch
        # already answers [MQTT-1.5.4-1] and _validate_mqtt_utf8 need not.
        raise ProtocolError("[MQTT-1.5.4-1] Surrogate in UTF-8 data") from exc
    _validate_mqtt_utf8(value, error_type=ProtocolError)
    if len(data) > 65535:
        raise ProtocolError("UTF-8 string too long for MQTT")
    return data


def pack_utf8(value: str) -> bytes:
    data = encode_utf8(value)
    return _U16.pack(len(data)) + data


def append_utf8(buf: bytearray, value: str) -> None:
    """Append MQTT UTF-8 string encoding into *buf* without intermediate concat."""
    data = encode_utf8(value)
    buf += _U16.pack(len(data))
    buf += data


def unpack_utf8(buffer: bytes | bytearray | memoryview, offset: int = 0) -> tuple[str, int]:
    length, pos = unpack_u16(buffer, offset)
    end = pos + length
    if end > len(buffer):
        raise MalformedPacketError("Incomplete UTF-8 string")
    try:
        # bytes/bytearray: decode the slice directly (one owned copy for bytes).
        # memoryview: tobytes() then decode.
        if isinstance(buffer, (bytes, bytearray)):
            text = buffer[pos:end].decode("utf-8")
        else:
            text = memoryview(buffer)[pos:end].tobytes().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedPacketError("Invalid UTF-8 data") from exc
    _validate_mqtt_utf8(text)
    return text, end


def pack_binary(value: bytes) -> bytes:
    if len(value) > 65535:
        raise ValueError("Binary data too long for MQTT")
    return _U16.pack(len(value)) + value


def unpack_binary(buffer: bytes | bytearray | memoryview, offset: int = 0) -> tuple[bytes, int]:
    length, pos = unpack_u16(buffer, offset)
    end = pos + length
    if end > len(buffer):
        raise MalformedPacketError("Incomplete binary data")
    return bytes(buffer[pos:end]), end


def _validate_mqtt_utf8(
    text: str,
    *,
    error_type: type[MalformedPacketError] | type[ProtocolError] = MalformedPacketError,
) -> None:
    # Two substring searches in C, rather than an interpreted loop running
    # three comparisons per code point.
    #
    # The third rule -- no surrogates -- is deliberately absent. Surrogates are
    # the only code points strict UTF-8 refuses, so every caller already
    # answers that rule through the conversion it performs anyway: encode_utf8
    # and validate_utf8 encode, and unpack_utf8 decodes, which cannot produce
    # one. Testing it here as well made a non-ASCII publication encode its
    # topic four times where twice is enough.
    if "\x00" in text:
        raise error_type("[MQTT-1.5.4-2] Null in UTF-8 data")
    if "﻿" in text:
        raise error_type("[MQTT-1.5.4-3] U+FEFF in UTF-8 data")
