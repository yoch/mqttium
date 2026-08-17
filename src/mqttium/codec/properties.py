"""MQTT 5 property encode/decode with per-packet validation.

Property table follows IMPLEMENTATION-GUIDE.md §2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from collections.abc import Callable
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
    # Extra constraint: the value is a topic name, so it must reject wildcards.
    # A declared flag rather than a `name == "response_topic"` string compare
    # per property, matching how nonzero/zero_one are already expressed.
    topic_value: bool = False
    # Extra constraint: repeatable in general, single-valued on SUBSCRIBE.
    subscribe_singleton: bool = False
    # The value codec for `type`, attached once at import (see below). Held on
    # the spec rather than looked up in a dict keyed by PropType: PropType is an
    # Enum and Enum.__hash__ is a Python-level call, so an enum-keyed lookup
    # costs two extra calls per property -- measurably worse than the if-chain
    # it would replace.
    encode: Callable[[Any], bytes] = field(init=False, repr=False, compare=False)
    decode: Callable[[bytes | bytearray, int], tuple[Any, int]] = field(
        init=False, repr=False, compare=False
    )


_SPECS: tuple[PropertySpec, ...] = (
    PropertySpec(
        0x01, "payload_format_indicator", PropType.BYTE, frozenset({PUBLISH, WILL}), zero_one=True
    ),
    PropertySpec(0x02, "message_expiry_interval", PropType.U32, frozenset({PUBLISH, WILL})),
    PropertySpec(0x03, "content_type", PropType.STRING, frozenset({PUBLISH, WILL})),
    PropertySpec(
        0x08, "response_topic", PropType.STRING, frozenset({PUBLISH, WILL}), topic_value=True
    ),
    PropertySpec(0x09, "correlation_data", PropType.BINARY, frozenset({PUBLISH, WILL})),
    PropertySpec(
        0x0B,
        "subscription_identifier",
        PropType.VBI,
        frozenset({PUBLISH, SUBSCRIBE}),
        multiple=True,  # multiple only on PUBLISH; SUBSCRIBE checked at encode/decode
        nonzero=True,
        subscribe_singleton=True,
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


def _encode_byte(value: Any) -> bytes:
    if not isinstance(value, int) or not 0 <= value <= 255:
        raise ProtocolError(f"Invalid byte property value: {value!r}")
    return bytes((value,))


def _encode_u16(value: Any) -> bytes:
    if not isinstance(value, int) or not 0 <= value <= 65535:
        raise ProtocolError(f"Invalid uint16 property value: {value!r}")
    return pack_u16(value)


def _encode_u32(value: Any) -> bytes:
    if not isinstance(value, int) or not 0 <= value <= 0xFFFFFFFF:
        raise ProtocolError(f"Invalid uint32 property value: {value!r}")
    return pack_u32(value)


def _encode_vbi_value(value: Any) -> bytes:
    if not isinstance(value, int) or not 0 <= value <= 268_435_455:
        raise ProtocolError(f"Invalid VBI property value: {value!r}")
    return encode_vbi(value)


def _encode_string(value: Any) -> bytes:
    if not isinstance(value, str):
        raise ProtocolError(f"Invalid string property value: {value!r}")
    return pack_utf8(value)


def _encode_string_pair(value: Any) -> bytes:
    if not (isinstance(value, tuple) and len(value) == 2):
        raise ProtocolError(f"Invalid string-pair property value: {value!r}")
    return pack_utf8(value[0]) + pack_utf8(value[1])


def _encode_binary(value: Any) -> bytes:
    if not isinstance(value, (bytes, bytearray)):
        raise ProtocolError(f"Invalid binary property value: {value!r}")
    try:
        return pack_binary(bytes(value))
    except ValueError as exc:
        raise ProtocolError(f"Invalid binary property value: {exc}") from exc


def _decode_byte(buf: bytes | bytearray, offset: int) -> tuple[Any, int]:
    if offset >= len(buf):
        raise MalformedPacketError("Incomplete byte property")
    return buf[offset], offset + 1


def _decode_string_pair(buf: bytes | bytearray, offset: int) -> tuple[Any, int]:
    key, pos = unpack_utf8(buf, offset)
    value, pos = unpack_utf8(buf, pos)
    return (key, value), pos


# Replaces two seven-branch `is` chains re-walked per property. Five of the
# seven decoders are now the primitive itself, so those lose a frame as well.
_ENCODERS: dict[PropType, Callable[[Any], bytes]] = {
    PropType.BYTE: _encode_byte,
    PropType.U16: _encode_u16,
    PropType.U32: _encode_u32,
    PropType.VBI: _encode_vbi_value,
    PropType.STRING: _encode_string,
    PropType.STRING_PAIR: _encode_string_pair,
    PropType.BINARY: _encode_binary,
}

_DECODERS: dict[PropType, Callable[[bytes | bytearray, int], tuple[Any, int]]] = {
    PropType.BYTE: _decode_byte,
    PropType.U16: unpack_u16,
    PropType.U32: unpack_u32,
    PropType.VBI: decode_vbi,
    PropType.STRING: unpack_utf8,
    PropType.STRING_PAIR: _decode_string_pair,
    PropType.BINARY: unpack_binary,
}

# A missing entry is an import-time failure, not a runtime branch.
assert set(_ENCODERS) == set(PropType) == set(_DECODERS)

for _spec in _SPECS:
    object.__setattr__(_spec, "encode", _ENCODERS[_spec.type])
    object.__setattr__(_spec, "decode", _DECODERS[_spec.type])


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
            if spec.subscribe_singleton and packet == SUBSCRIBE:
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
            if spec.topic_value:
                _validate_response_topic(item, ProtocolError)
            body.append(spec.id)
            body.extend(spec.encode(item))

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
    cache = props._encoded
    if cache is None:
        cache = props._encoded = {}
    else:
        cached = cache.get(packet)
        if cached is not None and cached[0] == signature:
            return cached[1]

    encoded = _encode_properties_uncached(props, packet)
    cache[packet] = (signature, encoded)
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
    # `result.values` is the seen-set: every branch below that records a name
    # also inserts it there, so a separate set would track the same keys.
    seen = result.values
    while pos < end:
        prop_id = buf[pos]
        pos += 1
        spec = BY_ID.get(prop_id)
        if spec is None:
            raise MalformedPacketError(f"Unknown property id 0x{prop_id:02x}")
        if packet not in spec.packets:
            raise MalformedPacketError(f"Property {spec.name} not allowed on {packet}")
        value, pos = spec.decode(buf, pos)
        if spec.nonzero and value == 0:
            raise MalformedPacketError(f"Property {spec.name} must not be zero")
        if spec.zero_one and value not in (0, 1):
            raise MalformedPacketError(f"Property {spec.name} must be 0 or 1")
        if spec.topic_value:
            _validate_response_topic(value, MalformedPacketError)

        if spec.multiple:
            if spec.subscribe_singleton and packet == SUBSCRIBE:
                if spec.name in seen:
                    raise MalformedPacketError("Duplicate subscription_identifier on SUBSCRIBE")
                result.set(spec.name, value)
            else:
                items = seen.setdefault(spec.name, [])
                items.append(value)
        else:
            if spec.name in seen:
                raise MalformedPacketError(f"Duplicate property {spec.name}")
            result.set(spec.name, value)

    if pos != end:
        raise MalformedPacketError("Properties length does not match consumed bytes")
    return result, end
