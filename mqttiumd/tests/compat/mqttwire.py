"""Minimal MQTT 3.1.1 and MQTT 5.0 packet encoder and decoder for the bench.

Independent of the broker's codec and of the `mqttium` client: every byte
layout below is written from the OASIS specifications (MQTT 3.1.1 with
Errata 01 and MQTT 5.0). The encoder does not validate: it writes what it
is asked to write, so that a scenario can send a packet a conforming
client never would. The decoder is strict and raises `WireError`.

A decoded packet is a plain dict that `json.dumps(..., sort_keys=True)`
renders in a stable way: no bytes, no tuples, no set.
"""
from __future__ import annotations

import hashlib

CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP = 1, 2, 3, 4, 5, 6, 7
SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT, AUTH = 8, 9, 10, 11, 12, 13, 14, 15

TYPE_NAMES = {
    CONNECT: "CONNECT", CONNACK: "CONNACK", PUBLISH: "PUBLISH", PUBACK: "PUBACK",
    PUBREC: "PUBREC", PUBREL: "PUBREL", PUBCOMP: "PUBCOMP", SUBSCRIBE: "SUBSCRIBE",
    SUBACK: "SUBACK", UNSUBSCRIBE: "UNSUBSCRIBE", UNSUBACK: "UNSUBACK",
    PINGREQ: "PINGREQ", PINGRESP: "PINGRESP", DISCONNECT: "DISCONNECT", AUTH: "AUTH",
}

# Fixed header flags required by both specifications (PUBLISH has none).
REQUIRED_FLAGS = {PUBREL: 2, SUBSCRIBE: 2, UNSUBSCRIBE: 2}

# MQTT 5.0 section 2.2.2.2: identifier -> (name, data type).
PROPERTIES = {
    0x01: ("payload_format_indicator", "byte"),
    0x02: ("message_expiry_interval", "u32"),
    0x03: ("content_type", "utf8"),
    0x08: ("response_topic", "utf8"),
    0x09: ("correlation_data", "binary"),
    0x0B: ("subscription_identifier", "varint"),
    0x11: ("session_expiry_interval", "u32"),
    0x12: ("assigned_client_identifier", "utf8"),
    0x13: ("server_keep_alive", "u16"),
    0x15: ("authentication_method", "utf8"),
    0x16: ("authentication_data", "binary"),
    0x17: ("request_problem_information", "byte"),
    0x18: ("will_delay_interval", "u32"),
    0x19: ("request_response_information", "byte"),
    0x1A: ("response_information", "utf8"),
    0x1C: ("server_reference", "utf8"),
    0x1F: ("reason_string", "utf8"),
    0x21: ("receive_maximum", "u16"),
    0x22: ("topic_alias_maximum", "u16"),
    0x23: ("topic_alias", "u16"),
    0x24: ("maximum_qos", "byte"),
    0x25: ("retain_available", "byte"),
    0x26: ("user_property", "pair"),
    0x27: ("maximum_packet_size", "u32"),
    0x28: ("wildcard_subscription_available", "byte"),
    0x29: ("subscription_identifier_available", "byte"),
    0x2A: ("shared_subscription_available", "byte"),
}
PROPERTY_IDS = {name: (identifier, kind) for identifier, (name, kind) in PROPERTIES.items()}
REPEATABLE = {"user_property", "subscription_identifier"}

# Payloads longer than this are rendered as a length and a digest.
PAYLOAD_INLINE_LIMIT = 96


class WireError(ValueError):
    """Bytes that are not a well-formed MQTT packet of the given level."""


# --------------------------------------------------------------------------
# Primitive encoders
# --------------------------------------------------------------------------

def as_bytes(value) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


def varint(value: int) -> bytes:
    """Variable Byte Integer (MQTT 5.0 section 1.5.5; 3.1.1 section 2.2.3)."""
    if not 0 <= value <= 268_435_455:
        raise ValueError("variable byte integer out of range")
    out = bytearray()
    while True:
        value, digit = divmod(value, 128)
        out.append(digit | (0x80 if value else 0))
        if not value:
            return bytes(out)


def u16(value: int) -> bytes:
    return value.to_bytes(2, "big")


def u32(value: int) -> bytes:
    return value.to_bytes(4, "big")


def utf8(value) -> bytes:
    """Length-prefixed string; bytes are passed through unchecked."""
    data = as_bytes(value)
    return u16(len(data)) + data


binary = utf8


def properties(items=None) -> bytes:
    """Property block with its length prefix.

    `items` is a dict or a list of `(name, value)` pairs. A list keeps its
    order and may repeat a name. A name may also be an integer identifier
    with a value already encoded, to write an unknown property.
    """
    if items is None:
        items = []
    if isinstance(items, dict):
        items = list(items.items())
    body = bytearray()
    for name, value in items:
        if isinstance(name, int):
            body += varint(name) + as_bytes(value)
            continue
        identifier, kind = PROPERTY_IDS[name]
        body += varint(identifier)
        if kind == "byte":
            body.append(int(value))
        elif kind == "u16":
            body += u16(value)
        elif kind == "u32":
            body += u32(value)
        elif kind == "varint":
            body += varint(value)
        elif kind in ("utf8", "binary"):
            body += utf8(value)
        else:
            key, text = value
            body += utf8(key) + utf8(text)
    return varint(len(body)) + bytes(body)


def packet(packet_type: int, flags: int, body: bytes = b"") -> bytes:
    """Fixed header with a canonical Remaining Length, then `body`."""
    return bytes([(packet_type << 4) | (flags & 0x0F)]) + varint(len(body)) + body


def raw(first_byte: int, remaining_length: bytes, body: bytes = b"") -> bytes:
    """A packet whose Remaining Length field is given as raw bytes."""
    return bytes([first_byte]) + remaining_length + body


# --------------------------------------------------------------------------
# Packet encoders
# --------------------------------------------------------------------------

def connect(client_id="", *, level=4, clean=True, keepalive=60, username=None,
            password=None, will=None, props=None, protocol_name="MQTT",
            connect_flags=None, reserved_flag=False, level_byte=None) -> bytes:
    """CONNECT. `will` is a dict: topic, payload, qos, retain, props.

    `connect_flags` overrides the computed flags byte without changing the
    payload, and `level_byte` the protocol level written, without changing
    the layout chosen by `level`: both build contradictory packets.
    """
    flags = 0x02 if clean else 0
    payload = bytearray(utf8(client_id))
    if will is not None:
        flags |= 0x04 | (will.get("qos", 0) << 3) | (0x20 if will.get("retain") else 0)
        if level >= 5:
            payload += properties(will.get("props"))
        payload += utf8(will["topic"]) + binary(will.get("payload", b""))
    if username is not None:
        flags |= 0x80
        payload += utf8(username)
    if password is not None:
        flags |= 0x40
        payload += binary(password)
    if reserved_flag:
        flags |= 0x01
    if connect_flags is not None:
        flags = connect_flags
    head = utf8(protocol_name) + bytes([level if level_byte is None else level_byte, flags]) + u16(keepalive)
    if level >= 5:
        head += properties(props)
    return packet(CONNECT, 0, head + bytes(payload))


def connack(session_present=False, reason=0, *, level=4, props=None) -> bytes:
    body = bytes([1 if session_present else 0, reason])
    if level >= 5:
        body += properties(props)
    return packet(CONNACK, 0, body)


def publish(topic, payload=b"", *, qos=0, retain=False, dup=False, packet_id=None,
            level=4, props=None, flags=None) -> bytes:
    """PUBLISH. `packet_id` is written whenever it is given, even at QoS 0."""
    if flags is None:
        flags = (0x08 if dup else 0) | (qos << 1) | (0x01 if retain else 0)
    body = bytearray(utf8(topic))
    if packet_id is not None:
        body += u16(packet_id)
    if level >= 5:
        body += properties(props)
    return packet(PUBLISH, flags, bytes(body) + as_bytes(payload))


def _ack(packet_type, flags, packet_id, level, reason, props) -> bytes:
    body = u16(packet_id)
    if level >= 5 and (reason is not None or props is not None):
        body += bytes([reason or 0])
        if props is not None:
            body += properties(props)
    return packet(packet_type, flags, body)


def puback(packet_id, *, level=4, reason=None, props=None) -> bytes:
    return _ack(PUBACK, 0, packet_id, level, reason, props)


def pubrec(packet_id, *, level=4, reason=None, props=None) -> bytes:
    return _ack(PUBREC, 0, packet_id, level, reason, props)


def pubrel(packet_id, *, level=4, reason=None, props=None, flags=2) -> bytes:
    return _ack(PUBREL, flags, packet_id, level, reason, props)


def pubcomp(packet_id, *, level=4, reason=None, props=None) -> bytes:
    return _ack(PUBCOMP, 0, packet_id, level, reason, props)


def sub_options(qos=0, *, no_local=False, rap=False, rh=0, reserved=0) -> int:
    """Subscription Options byte (MQTT 5.0 section 3.8.3.1)."""
    return qos | (0x04 if no_local else 0) | (0x08 if rap else 0) | (rh << 4) | (reserved << 6)


def subscribe(packet_id, filters, *, level=4, props=None, flags=2) -> bytes:
    """SUBSCRIBE. `filters` is a list of `(filter, options byte)`."""
    body = bytearray(u16(packet_id))
    if level >= 5:
        body += properties(props)
    for topic_filter, options in filters:
        body += utf8(topic_filter) + bytes([options])
    return packet(SUBSCRIBE, flags, bytes(body))


def suback(packet_id, codes, *, level=4, props=None) -> bytes:
    body = u16(packet_id)
    if level >= 5:
        body += properties(props)
    return packet(SUBACK, 0, body + bytes(codes))


def unsubscribe(packet_id, filters, *, level=4, props=None, flags=2) -> bytes:
    body = bytearray(u16(packet_id))
    if level >= 5:
        body += properties(props)
    for topic_filter in filters:
        body += utf8(topic_filter)
    return packet(UNSUBSCRIBE, flags, bytes(body))


def unsuback(packet_id, codes=(), *, level=4, props=None) -> bytes:
    body = u16(packet_id)
    if level >= 5:
        body += properties(props) + bytes(codes)
    return packet(UNSUBACK, 0, body)


def pingreq() -> bytes:
    return packet(PINGREQ, 0)


def pingresp() -> bytes:
    return packet(PINGRESP, 0)


def disconnect(*, level=4, reason=None, props=None) -> bytes:
    body = b""
    if level >= 5 and (reason is not None or props is not None):
        body = bytes([reason or 0])
        if props is not None:
            body += properties(props)
    return packet(DISCONNECT, 0, body)


def auth(reason=None, *, props=None) -> bytes:
    body = b""
    if reason is not None or props is not None:
        body = bytes([reason or 0]) + properties(props)
    return packet(AUTH, 0, body)


# --------------------------------------------------------------------------
# Decoder
# --------------------------------------------------------------------------

class Reader:
    def __init__(self, data: bytes):
        self.data = data
        self.at = 0

    def take(self, count: int) -> bytes:
        if count < 0 or self.at + count > len(self.data):
            raise WireError("truncated packet")
        chunk = self.data[self.at:self.at + count]
        self.at += count
        return chunk

    def byte(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return int.from_bytes(self.take(2), "big")

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "big")

    def varint(self) -> int:
        value = 0
        for shift in range(4):
            digit = self.byte()
            value |= (digit & 0x7F) << (7 * shift)
            if not digit & 0x80:
                if shift and not digit:
                    raise WireError("non-minimal variable byte integer")
                return value
        raise WireError("variable byte integer longer than four bytes")

    def binary(self) -> bytes:
        return self.take(self.u16())

    def utf8(self) -> str:
        data = self.binary()
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise WireError("string is not UTF-8") from error

    def rest(self) -> bytes:
        return self.take(len(self.data) - self.at)

    def done(self) -> bool:
        return self.at == len(self.data)


def read_properties(reader: Reader) -> dict:
    """Decode a property block to `{name: value}`.

    User properties become a list of `[key, value]` in wire order and
    subscription identifiers a list of integers. Another property present
    twice is a malformed packet.
    """
    block = Reader(reader.take(reader.varint()))
    out: dict = {}
    while not block.done():
        identifier = block.varint()
        if identifier not in PROPERTIES:
            raise WireError(f"unknown property {identifier}")
        name, kind = PROPERTIES[identifier]
        if kind == "byte":
            value = block.byte()
        elif kind == "u16":
            value = block.u16()
        elif kind == "u32":
            value = block.u32()
        elif kind == "varint":
            value = block.varint()
        elif kind == "utf8":
            value = block.utf8()
        elif kind == "binary":
            value = block.binary().hex()
        else:
            value = [block.utf8(), block.utf8()]
        if name in REPEATABLE:
            out.setdefault(name, []).append(value)
        elif name in out:
            raise WireError(f"property {name} present twice")
        else:
            out[name] = value
    return out


def render_payload(data: bytes):
    """A JSON-safe, stable rendering of an application payload."""
    if len(data) > PAYLOAD_INLINE_LIMIT:
        return {"length": len(data), "sha256": hashlib.sha256(data).hexdigest()[:16]}
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return {"hex": data.hex()}
    if all(character.isprintable() for character in text):
        return text
    return {"hex": data.hex()}


def split_packets(buffer: bytes):
    """Split complete packets off the front of `buffer`.

    Returns `(packets, rest)`. Raises `WireError` when the Remaining Length
    cannot be a Variable Byte Integer.
    """
    packets = []
    at = 0
    while True:
        if len(buffer) - at < 2:
            break
        length, shift, cursor = 0, 0, at + 1
        complete = False
        while cursor < len(buffer):
            digit = buffer[cursor]
            cursor += 1
            length |= (digit & 0x7F) << shift
            if not digit & 0x80:
                complete = True
                break
            shift += 7
            if shift > 21:
                raise WireError("remaining length longer than four bytes")
        if not complete or len(buffer) - cursor < length:
            break
        packets.append(bytes(buffer[at:cursor + length]))
        at = cursor + length
    return packets, bytes(buffer[at:])


def _ack_fields(reader: Reader, level: int, out: dict) -> None:
    out["packet_id"] = reader.u16()
    if level >= 5 and not reader.done():
        out["reason"] = reader.byte()
        if not reader.done():
            out["properties"] = read_properties(reader)
    elif level >= 5:
        out["reason"] = 0


def decode(data: bytes, level: int = 4) -> dict:
    """Decode one complete packet sent by either side at protocol `level`."""
    packets, rest = split_packets(data)
    if len(packets) != 1 or rest:
        raise WireError("not exactly one packet")
    first = data[0]
    packet_type, flags = first >> 4, first & 0x0F
    if packet_type not in TYPE_NAMES or (packet_type == AUTH and level < 5):
        raise WireError(f"unknown packet type {packet_type}")
    reader = Reader(data)
    reader.byte()
    reader.varint()
    out: dict = {"type": TYPE_NAMES[packet_type]}
    if packet_type != PUBLISH and flags != REQUIRED_FLAGS.get(packet_type, 0):
        raise WireError(f"{out['type']} with fixed header flags {flags}")

    if packet_type == CONNECT:
        out["protocol_name"] = reader.utf8()
        out["level"] = level = reader.byte()
        connect_flags = reader.byte()
        out["clean"] = bool(connect_flags & 0x02)
        out["keepalive"] = reader.u16()
        if connect_flags & 0x01:
            out["reserved_flag"] = True
        if level >= 5:
            out["properties"] = read_properties(reader)
        out["client_id"] = reader.utf8()
        if connect_flags & 0x04:
            will = {"qos": (connect_flags >> 3) & 3, "retain": bool(connect_flags & 0x20)}
            if level >= 5:
                will["properties"] = read_properties(reader)
            will["topic"] = reader.utf8()
            will["payload"] = render_payload(reader.binary())
            out["will"] = will
        elif connect_flags & 0x38:
            out["will_flags_without_will"] = connect_flags & 0x38
        if connect_flags & 0x80:
            out["username"] = reader.utf8()
        if connect_flags & 0x40:
            out["password"] = render_payload(reader.binary())
    elif packet_type == CONNACK:
        acknowledge = reader.byte()
        if acknowledge > 1:
            raise WireError("CONNACK acknowledge flags reserved bits set")
        out["session_present"] = bool(acknowledge)
        out["reason"] = reader.byte()
        if level >= 5:
            out["properties"] = read_properties(reader)
    elif packet_type == PUBLISH:
        qos = (flags >> 1) & 3
        if qos == 3:
            raise WireError("PUBLISH with QoS 3")
        out.update(dup=bool(flags & 0x08), qos=qos, retain=bool(flags & 0x01))
        out["topic"] = reader.utf8()
        if qos:
            out["packet_id"] = reader.u16()
        if level >= 5:
            out["properties"] = read_properties(reader)
        out["payload"] = render_payload(reader.rest())
    elif packet_type in (PUBACK, PUBREC, PUBREL, PUBCOMP):
        _ack_fields(reader, level, out)
    elif packet_type == SUBSCRIBE:
        out["packet_id"] = reader.u16()
        if level >= 5:
            out["properties"] = read_properties(reader)
        filters = []
        while not reader.done():
            topic_filter = reader.utf8()
            options = reader.byte()
            entry = {"filter": topic_filter, "qos": options & 3}
            if options & 0xFC:
                entry.update(no_local=bool(options & 4), rap=bool(options & 8), rh=(options >> 4) & 3)
                if options & 0xC0:
                    entry["reserved"] = options >> 6
            filters.append(entry)
        out["filters"] = filters
    elif packet_type == SUBACK:
        out["packet_id"] = reader.u16()
        if level >= 5:
            out["properties"] = read_properties(reader)
        out["codes"] = list(reader.rest())
    elif packet_type == UNSUBSCRIBE:
        out["packet_id"] = reader.u16()
        if level >= 5:
            out["properties"] = read_properties(reader)
        filters = []
        while not reader.done():
            filters.append(reader.utf8())
        out["filters"] = filters
    elif packet_type == UNSUBACK:
        out["packet_id"] = reader.u16()
        if level >= 5:
            out["properties"] = read_properties(reader)
            out["codes"] = list(reader.rest())
    elif packet_type == DISCONNECT:
        if level >= 5:
            out["reason"] = 0 if reader.done() else reader.byte()
            if not reader.done():
                out["properties"] = read_properties(reader)
    elif packet_type == AUTH:
        out["reason"] = 0 if reader.done() else reader.byte()
        if not reader.done():
            out["properties"] = read_properties(reader)
    if not reader.done():
        raise WireError(f"{out['type']} with trailing bytes")
    if "properties" in out and not out["properties"]:
        del out["properties"]
    return out
