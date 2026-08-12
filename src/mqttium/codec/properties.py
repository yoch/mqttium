"""MQTT 5 property encode/decode with per-packet validation.

Property table follows IMPLEMENTATION-GUIDE.md §2.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from mqttium.codec.primitives import (
    pack_binary,
    pack_u16,
    pack_u32,
    pack_utf8,
    unpack_binary,
    unpack_u16,
    unpack_u32,
    unpack_utf8,
)
from mqttium.codec.vbi import decode_vbi, encode_vbi
from mqttium.errors import MQTTError, MalformedPacketError, ProtocolError
from mqttium.types import Properties


class PropType(Enum):
    BYTE = "B"
    U16 = "2I"
    U32 = "4I"
    VBI = "VBI"
    STRING = "S"
    STRING_PAIR = "SP"
    BINARY = "BIN"


# Packet / context names used in the allowed set.
CONNECT = "CONNECT"
CONNACK = "CONNACK"
PUBLISH = "PUBLISH"
PUBACK = "PUBACK"
PUBREC = "PUBREC"
PUBREL = "PUBREL"
PUBCOMP = "PUBCOMP"
SUBSCRIBE = "SUBSCRIBE"
SUBACK = "SUBACK"
UNSUBSCRIBE = "UNSUBSCRIBE"
UNSUBACK = "UNSUBACK"
DISCONNECT = "DISCONNECT"
AUTH = "AUTH"
WILL = "WILL"

ALL_PACKETS = frozenset(
    {
        CONNECT,
        CONNACK,
        PUBLISH,
        PUBACK,
        PUBREC,
        PUBREL,
        PUBCOMP,
        SUBSCRIBE,
        SUBACK,
        UNSUBSCRIBE,
        UNSUBACK,
        DISCONNECT,
        AUTH,
        WILL,
    }
)


@dataclass(frozen=True, slots=True)
class PropertySpec:
    id: int
    name: str
    type: PropType
    packets: frozenset[str]
    multiple: bool = False
    # Extra constraint: value must not be zero.
    nonzero: bool = False
    # Extra constraint: BYTE value must be 0 or 1.
    zero_one: bool = False


_SPECS: tuple[PropertySpec, ...] = (
    PropertySpec(
        0x01, "payload_format_indicator", PropType.BYTE, frozenset({PUBLISH, WILL}), zero_one=True
    ),
    PropertySpec(0x02, "message_expiry_interval", PropType.U32, frozenset({PUBLISH, WILL})),
    PropertySpec(0x03, "content_type", PropType.STRING, frozenset({PUBLISH, WILL})),
    PropertySpec(0x08, "response_topic", PropType.STRING, frozenset({PUBLISH, WILL})),
    PropertySpec(0x09, "correlation_data", PropType.BINARY, frozenset({PUBLISH, WILL})),
    PropertySpec(
        0x0B,
        "subscription_identifier",
        PropType.VBI,
        frozenset({PUBLISH, SUBSCRIBE}),
        multiple=True,  # multiple only on PUBLISH; SUBSCRIBE checked at encode/decode
        nonzero=True,
    ),
    PropertySpec(
        0x11,
        "session_expiry_interval",
        PropType.U32,
        frozenset({CONNECT, CONNACK, DISCONNECT}),
    ),
    PropertySpec(0x12, "assigned_client_identifier", PropType.STRING, frozenset({CONNACK})),
    PropertySpec(0x13, "server_keep_alive", PropType.U16, frozenset({CONNACK})),
    PropertySpec(
        0x15,
        "authentication_method",
        PropType.STRING,
        frozenset({CONNECT, CONNACK, AUTH}),
    ),
    PropertySpec(
        0x16,
        "authentication_data",
        PropType.BINARY,
        frozenset({CONNECT, CONNACK, AUTH}),
    ),
    PropertySpec(
        0x17, "request_problem_information", PropType.BYTE, frozenset({CONNECT}), zero_one=True
    ),
    PropertySpec(0x18, "will_delay_interval", PropType.U32, frozenset({WILL})),
    PropertySpec(
        0x19, "request_response_information", PropType.BYTE, frozenset({CONNECT}), zero_one=True
    ),
    PropertySpec(0x1A, "response_information", PropType.STRING, frozenset({CONNACK})),
    PropertySpec(0x1C, "server_reference", PropType.STRING, frozenset({CONNACK, DISCONNECT})),
    PropertySpec(
        0x1F,
        "reason_string",
        PropType.STRING,
        frozenset(
            {
                CONNACK,
                PUBACK,
                PUBREC,
                PUBREL,
                PUBCOMP,
                SUBACK,
                UNSUBACK,
                DISCONNECT,
                AUTH,
            }
        ),
    ),
    PropertySpec(
        0x21,
        "receive_maximum",
        PropType.U16,
        frozenset({CONNECT, CONNACK}),
        nonzero=True,
    ),
    PropertySpec(0x22, "topic_alias_maximum", PropType.U16, frozenset({CONNECT, CONNACK})),
    PropertySpec(0x23, "topic_alias", PropType.U16, frozenset({PUBLISH}), nonzero=True),
    PropertySpec(0x24, "maximum_qos", PropType.BYTE, frozenset({CONNACK}), zero_one=True),
    PropertySpec(0x25, "retain_available", PropType.BYTE, frozenset({CONNACK}), zero_one=True),
    PropertySpec(0x26, "user_property", PropType.STRING_PAIR, ALL_PACKETS, multiple=True),
    PropertySpec(
        0x27,
        "maximum_packet_size",
        PropType.U32,
        frozenset({CONNECT, CONNACK}),
        nonzero=True,
    ),
    PropertySpec(
        0x28,
        "wildcard_subscription_available",
        PropType.BYTE,
        frozenset({CONNACK}),
        zero_one=True,
    ),
    PropertySpec(
        0x29,
        "subscription_identifier_available",
        PropType.BYTE,
        frozenset({CONNACK}),
        zero_one=True,
    ),
    PropertySpec(
        0x2A,
        "shared_subscription_available",
        PropType.BYTE,
        frozenset({CONNACK}),
        zero_one=True,
    ),
)

BY_ID: dict[int, PropertySpec] = {s.id: s for s in _SPECS}
BY_NAME: dict[str, PropertySpec] = {s.name: s for s in _SPECS}


def _encode_value(ptype: PropType, value: Any) -> bytes:  # noqa: C901
    if ptype is PropType.BYTE:
        if not isinstance(value, int) or not 0 <= value <= 255:
            raise ProtocolError(f"Invalid byte property value: {value!r}")
        return bytes((value,))
    if ptype is PropType.U16:
        if not isinstance(value, int) or not 0 <= value <= 65535:
            raise ProtocolError(f"Invalid uint16 property value: {value!r}")
        return pack_u16(value)
    if ptype is PropType.U32:
        if not isinstance(value, int) or not 0 <= value <= 0xFFFFFFFF:
            raise ProtocolError(f"Invalid uint32 property value: {value!r}")
        return pack_u32(value)
    if ptype is PropType.VBI:
        if not isinstance(value, int) or not 0 <= value <= 268_435_455:
            raise ProtocolError(f"Invalid VBI property value: {value!r}")
        return encode_vbi(value)
    if ptype is PropType.STRING:
        if not isinstance(value, str):
            raise ProtocolError(f"Invalid string property value: {value!r}")
        return pack_utf8(value)
    if ptype is PropType.STRING_PAIR:
        if not (isinstance(value, tuple) and len(value) == 2):
            raise ProtocolError(f"Invalid string-pair property value: {value!r}")
        return pack_utf8(value[0]) + pack_utf8(value[1])
    if ptype is PropType.BINARY:
        if not isinstance(value, (bytes, bytearray)):
            raise ProtocolError(f"Invalid binary property value: {value!r}")
        try:
            return pack_binary(bytes(value))
        except ValueError as exc:
            raise ProtocolError(f"Invalid binary property value: {exc}") from exc
    raise ProtocolError(f"Unknown property type {ptype}")


def _decode_value(ptype: PropType, buf: bytes | bytearray, offset: int) -> tuple[Any, int]:
    if ptype is PropType.BYTE:
        if offset >= len(buf):
            raise MalformedPacketError("Incomplete byte property")
        return buf[offset], offset + 1
    if ptype is PropType.U16:
        return unpack_u16(buf, offset)
    if ptype is PropType.U32:
        return unpack_u32(buf, offset)
    if ptype is PropType.VBI:
        return decode_vbi(buf, offset)
    if ptype is PropType.STRING:
        return unpack_utf8(buf, offset)
    if ptype is PropType.STRING_PAIR:
        k, pos = unpack_utf8(buf, offset)
        v, pos = unpack_utf8(buf, pos)
        return (k, v), pos
    if ptype is PropType.BINARY:
        return unpack_binary(buf, offset)
    raise MalformedPacketError(f"Unknown property type {ptype}")


def _validate_response_topic(value: Any, error_type: type[MQTTError]) -> None:
    if not isinstance(value, str):
        return
    if not value:
        raise error_type("Property 'response_topic' must not be empty")
    if "+" in value or "#" in value:
        raise error_type("Property 'response_topic' must not contain wildcards")


def _encode_properties_uncached(props: Properties, packet: str) -> bytes:
    body = bytearray()
    for name, value in props.values.items():
        spec = BY_NAME.get(name)
        if spec is None:
            raise ProtocolError(f"Unknown property name {name!r}")
        if packet not in spec.packets:
            raise ProtocolError(f"Property {name!r} not allowed on {packet}")

        values: list[Any]
        if spec.multiple:
            if name == "subscription_identifier" and packet == SUBSCRIBE:
                # SUBSCRIBE: single value only (guide §2).
                if isinstance(value, list):
                    if len(value) != 1:
                        raise ProtocolError("SUBSCRIBE allows one subscription_identifier")
                    values = value
                else:
                    values = [value]
            elif isinstance(value, list):
                values = value
            else:
                values = [value]
        else:
            if isinstance(value, list):
                raise ProtocolError(f"Property {name!r} is not repeatable")
            values = [value]

        for item in values:
            if spec.nonzero and item == 0:
                raise ProtocolError(f"Property {name!r} must not be zero")
            if spec.zero_one and item not in (0, 1):
                raise ProtocolError(f"Property {name!r} must be 0 or 1")
            if name == "response_topic":
                _validate_response_topic(item, ProtocolError)
            body.append(spec.id)
            body.extend(_encode_value(spec.type, item))

    return encode_vbi(len(body)) + bytes(body)


def encode_properties(props: Properties | None, packet: str) -> bytes:
    """Encode a property bag for *packet* (including WILL context).

    Empty / None → single ``0x00`` length byte (fast path). Non-empty results
    are cached by packet context and a structural value signature, so repeated
    sizing/encoding reuses bytes even when callers mutate ``props.values``
    directly between calls.
    """
    if not props or not props.values:
        return b"\x00"

    signature = props._signature()
    cached = props._encoded.get(packet)
    if cached is not None and cached[0] == signature:
        return cached[1]

    encoded = _encode_properties_uncached(props, packet)
    props._encoded[packet] = (signature, encoded)
    return encoded


def decode_properties(
    buf: bytes | bytearray,
    offset: int,
    packet: str,
) -> tuple[Properties, int]:
    """Decode properties starting at *offset*.

    Returns ``(Properties, new_offset)``.
    """
    if offset >= len(buf):
        raise MalformedPacketError("Missing properties length")
    # Fast path: empty property length (single 0x00) — common for PUBLISH/ACK.
    if buf[offset] == 0:
        return Properties(), offset + 1
    props_len, pos = decode_vbi(buf, offset)
    end = pos + props_len
    if end > len(buf):
        raise MalformedPacketError("Properties length exceeds remaining data")

    result = Properties()
    seen: set[str] = set()
    while pos < end:
        prop_id = buf[pos]
        pos += 1
        spec = BY_ID.get(prop_id)
        if spec is None:
            raise MalformedPacketError(f"Unknown property id 0x{prop_id:02x}")
        if packet not in spec.packets:
            raise MalformedPacketError(f"Property {spec.name} not allowed on {packet}")
        value, pos = _decode_value(spec.type, buf, pos)
        if spec.nonzero and value == 0:
            raise MalformedPacketError(f"Property {spec.name} must not be zero")
        if spec.zero_one and value not in (0, 1):
            raise MalformedPacketError(f"Property {spec.name} must be 0 or 1")
        if spec.name == "response_topic":
            _validate_response_topic(value, MalformedPacketError)

        if spec.multiple:
            if spec.name == "subscription_identifier" and packet == SUBSCRIBE:
                if spec.name in seen:
                    raise MalformedPacketError("Duplicate subscription_identifier on SUBSCRIBE")
                result.set(spec.name, value)
                seen.add(spec.name)
            else:
                items = result.values.setdefault(spec.name, [])
                items.append(value)
                seen.add(spec.name)
        else:
            if spec.name in seen:
                raise MalformedPacketError(f"Duplicate property {spec.name}")
            result.set(spec.name, value)
            seen.add(spec.name)

    if pos != end:
        raise MalformedPacketError("Properties length does not match consumed bytes")
    return result, end
