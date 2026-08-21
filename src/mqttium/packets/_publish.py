"""Version-specialized MQTT PUBLISH encode/decode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from mqttium.codec.buffer import RawPacket
from mqttium.codec.packet_validation import require_nonzero_mid
from mqttium.codec.primitives import encode_utf8, unpack_u16, unpack_utf8
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
from mqttium.codec.vbi import MAX_VBI, append_vbi
from mqttium.enums import PacketType, QoS
from mqttium.errors import MalformedPacketError, PacketTooLargeError, ProtocolError
from mqttium.packets._common import _require_outbound_mid
from mqttium.topics import validate_received_publish_topic
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem
from mqttium.types import Message, Properties


def decode_qos0_message_v311(raw: RawPacket) -> Message:
    if raw.flags & 0x08:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    topic, payload_pos = unpack_utf8(raw.remaining)
    if not topic:
        raise MalformedPacketError("PUBLISH topic must not be empty")
    validate_received_publish_topic(topic, utf8_validated=True)
    return Message(
        topic=topic,
        payload=raw.remaining[payload_pos:],
        qos=QoS.AT_MOST_ONCE,
        retain=bool(raw.flags & 0x01),
        dup=False,
        mid=None,
        properties=None,
    )


def decode_qos0_message_v5(raw: RawPacket) -> tuple[Message, int]:
    """Decode MQTT 5 QoS 0 directly and return its property-table wire size."""
    if raw.flags & 0x08:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    topic, pos = unpack_utf8(raw.remaining)
    properties_pos = pos
    properties, pos = decode_properties(raw.remaining, pos, PUBLISH)
    property_wire_size = pos - properties_pos
    validate_received_publish_topic(topic, utf8_validated=True)
    return (
        Message(
            topic=topic,
            payload=raw.remaining[pos:],
            qos=QoS.AT_MOST_ONCE,
            retain=bool(raw.flags & 0x01),
            dup=False,
            mid=None,
            properties=properties,
        ),
        property_wire_size,
    )


def decode_qos0_message_v311_borrowed(
    buf: bytearray, body_start: int, body_end: int, flags: int
) -> Message:
    """Decode QoS 0 from the decoder buffer, copying only owned application fields."""
    if flags & 0x08:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    if body_start + 2 > body_end:
        raise MalformedPacketError("Incomplete uint16")
    topic_len = (buf[body_start] << 8) | buf[body_start + 1]
    topic_start = body_start + 2
    payload_pos = topic_start + topic_len
    if payload_pos > body_end:
        raise MalformedPacketError("Incomplete UTF-8 string")
    try:
        topic = buf[topic_start:payload_pos].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedPacketError("Invalid UTF-8 data") from exc
    if "\x00" in topic:
        raise MalformedPacketError("[MQTT-1.5.4-2] Null in UTF-8 data")
    if not topic:
        raise MalformedPacketError("PUBLISH topic must not be empty")
    validate_received_publish_topic(topic, utf8_validated=True)
    return Message(
        topic,
        bytes(buf[payload_pos:body_end]),
        QoS.AT_MOST_ONCE,
        bool(flags & 0x01),
        False,
        None,
        None,
    )


def _decode_bounded_vbi(buf: bytearray, offset: int, end: int) -> tuple[int, int]:
    if offset >= end:
        raise MalformedPacketError("Incomplete Variable Byte Integer")
    value = 0
    multiplier = 1
    pos = offset
    count = 0
    while True:
        if pos >= end:
            raise MalformedPacketError("Incomplete Variable Byte Integer")
        byte = buf[pos]
        pos += 1
        count += 1
        if count > 4:
            raise MalformedPacketError("Malformed Variable Byte Integer (too long)")
        value += (byte & 0x7F) * multiplier
        if byte & 0x80 == 0:
            break
        multiplier *= 128
    if count != (1 if value < 128 else 2 if value < 16_384 else 3 if value < 2_097_152 else 4):
        raise MalformedPacketError("Non-canonical Variable Byte Integer")
    return value, pos


def decode_qos0_message_v5_borrowed(
    buf: bytearray, body_start: int, body_end: int, flags: int
) -> tuple[Message, int]:
    """Decode MQTT 5 QoS 0 without materialising an owned RawPacket body."""
    if flags & 0x08:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    if body_start + 2 > body_end:
        raise MalformedPacketError("Incomplete uint16")
    topic_len = (buf[body_start] << 8) | buf[body_start + 1]
    topic_start = body_start + 2
    pos = topic_start + topic_len
    if pos > body_end:
        raise MalformedPacketError("Incomplete UTF-8 string")
    try:
        topic = buf[topic_start:pos].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedPacketError("Invalid UTF-8 data") from exc
    if "\x00" in topic:
        raise MalformedPacketError("[MQTT-1.5.4-2] Null in UTF-8 data")
    validate_received_publish_topic(topic, utf8_validated=True)
    if pos >= body_end:
        raise MalformedPacketError("Missing properties length")

    properties_pos = pos
    if buf[pos] == 0:
        properties = Properties()
        pos += 1
    else:
        props_len, props_data_pos = _decode_bounded_vbi(buf, pos, body_end)
        props_end = props_data_pos + props_len
        if props_end > body_end:
            raise MalformedPacketError("Properties length exceeds remaining data")
        # Property tables are typically small. For these sizes, copying the
        # bytearray slice and converting it to bytes is measurably cheaper than
        # creating a transient memoryview, while still avoiding a payload copy.
        prefix = bytes(buf[body_start:props_end])
        relative_pos = properties_pos - body_start
        properties, relative_end = decode_properties(prefix, relative_pos, PUBLISH)
        pos = body_start + relative_end
    property_wire_size = pos - properties_pos
    return (
        Message(
            topic,
            bytes(buf[pos:body_end]),
            QoS.AT_MOST_ONCE,
            bool(flags & 0x01),
            False,
            None,
            properties,
        ),
        property_wire_size,
    )


def decode_qos12_fields_v311(raw: RawPacket) -> tuple[str, bytes, int, bool, bool]:
    """Decode MQTT 3.1.1 QoS 1/2 PUBLISH fields (identical wire layout)."""
    topic, pos = unpack_utf8(raw.remaining)
    if not topic:
        raise MalformedPacketError("PUBLISH topic must not be empty")
    validate_received_publish_topic(topic, utf8_validated=True)
    mid, pos = unpack_u16(raw.remaining, pos)
    require_nonzero_mid(mid, "PUBLISH")
    return topic, raw.remaining[pos:], mid, bool(raw.flags & 0x01), bool(raw.flags & 0x08)


def encode_publish_item_v311(
    topic: str,
    payload: bytes,
    *,
    qos: QoS | int,
    retain: bool,
    dup: bool,
    mid: int | None,
    properties: Properties | None = None,
    _topic_bytes: bytes | None = None,
    _property_bytes: bytes | None = None,
) -> WriteItem:
    if type(qos) is QoS:
        level = qos
    else:
        try:
            level = QoS(qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid PUBLISH QoS {qos!r}") from exc
    if level:
        if mid is None:
            raise ProtocolError("QoS > 0 PUBLISH requires a packet identifier")
        _require_outbound_mid(mid, "PUBLISH")
    else:
        if mid is not None:
            raise ProtocolError("QoS 0 PUBLISH must not carry a packet identifier")
        if dup:
            raise ProtocolError("QoS 0 PUBLISH must not set DUP")

    flags = (int(level) << 1) | (0x01 if retain else 0) | (0x08 if dup else 0)
    topic_bytes = encode_utf8(topic) if _topic_bytes is None else _topic_bytes
    topic_size = len(topic_bytes)
    payload_size = len(payload)
    remaining_length = 2 + topic_size + (2 if level else 0) + payload_size
    if remaining_length > MAX_VBI:
        raise PacketTooLargeError(
            f"PUBLISH remaining length {remaining_length} exceeds MQTT wire maximum {MAX_VBI}"
        )

    header = bytearray()
    header.append(int(PacketType.PUBLISH) | flags)
    append_vbi(header, remaining_length)
    header.append((topic_size >> 8) & 0xFF)
    header.append(topic_size & 0xFF)
    header.extend(topic_bytes)
    if level:
        assert mid is not None
        header.append((mid >> 8) & 0xFF)
        header.append(mid & 0xFF)

    if payload_size >= SEGMENT_THRESHOLD:
        return bytes(header), bytes(payload)
    header.extend(payload)
    return bytes(header)


_EMPTY_PROPS = b"\x00"


def decode_publish_fields_v5(
    raw: RawPacket,
    qos: QoS,
) -> tuple[str, bytes, int | None, bool, bool, Properties, int]:
    """Decode MQTT 5 PUBLISH fields and its property-table wire size."""
    dup = bool(raw.flags & 0x08)
    if qos is QoS.AT_MOST_ONCE and dup:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    topic, pos = unpack_utf8(raw.remaining)
    mid: int | None = None
    if qos:
        mid, pos = unpack_u16(raw.remaining, pos)
        require_nonzero_mid(mid, "PUBLISH")
    properties_pos = pos
    properties, pos = decode_properties(raw.remaining, pos, PUBLISH)
    property_wire_size = pos - properties_pos
    # Preserve #128: a non-empty Topic Name must be validated before it can
    # establish or replace a connection-scoped Topic Alias.
    validate_received_publish_topic(topic, utf8_validated=True)
    return (
        topic,
        raw.remaining[pos:],
        mid,
        bool(raw.flags & 0x01),
        dup,
        properties,
        property_wire_size,
    )


def encode_publish_item_v5(
    topic: str,
    payload: bytes,
    *,
    qos: QoS | int,
    retain: bool,
    dup: bool,
    mid: int | None,
    properties: Properties | None,
    _topic_bytes: bytes | None = None,
    _property_bytes: bytes | None = None,
) -> WriteItem:
    if type(qos) is QoS:
        level = qos
    else:
        try:
            level = QoS(qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid PUBLISH QoS {qos!r}") from exc
    if level:
        if mid is None:
            raise ProtocolError("QoS > 0 PUBLISH requires a packet identifier")
        _require_outbound_mid(mid, PUBLISH)
    else:
        if mid is not None:
            raise ProtocolError("QoS 0 PUBLISH must not carry a packet identifier")
        if dup:
            raise ProtocolError("QoS 0 PUBLISH must not set DUP")

    flags = (int(level) << 1) | (0x01 if retain else 0) | (0x08 if dup else 0)
    topic_bytes = encode_utf8(topic) if _topic_bytes is None else _topic_bytes
    topic_size = len(topic_bytes)
    if _property_bytes is not None:
        props = _property_bytes
    elif not properties or not properties.values:
        props = _EMPTY_PROPS
    else:
        props = encode_properties(properties, PUBLISH)
    payload_size = len(payload)
    remaining_length = 2 + topic_size + (2 if level else 0) + len(props) + payload_size
    if remaining_length > MAX_VBI:
        raise PacketTooLargeError(
            f"PUBLISH remaining length {remaining_length} exceeds MQTT wire maximum {MAX_VBI}"
        )

    header = bytearray()
    header.append(int(PacketType.PUBLISH) | flags)
    append_vbi(header, remaining_length)
    header.append((topic_size >> 8) & 0xFF)
    header.append(topic_size & 0xFF)
    header.extend(topic_bytes)
    if level:
        assert mid is not None
        header.append((mid >> 8) & 0xFF)
        header.append(mid & 0xFF)
    header.extend(props)

    if payload_size >= SEGMENT_THRESHOLD:
        return bytes(header), bytes(payload)
    header.extend(payload)
    return bytes(header)
