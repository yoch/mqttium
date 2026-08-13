"""Version-specialized MQTT PUBLISH encode/decode primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from mqttium.codec.buffer import RawPacket
from mqttium.codec.packet_validation import require_nonzero_mid
from mqttium.codec.primitives import encode_utf8, unpack_u16, unpack_utf8
from mqttium.codec.vbi import MAX_VBI, append_vbi
from mqttium.enums import PacketType, QoS
from mqttium.errors import MalformedPacketError, PacketTooLargeError, ProtocolError
from mqttium.packets._common import _require_outbound_mid
from mqttium.topics import validate_received_publish_topic
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem
from mqttium.types import Message, Properties
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties


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
) -> WriteItem:
    if type(qos) is QoS:
        level = qos
    else:
        try:
            level = QoS(qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid PUBLISH QoS {qos!r}") from exc
    if level and mid is None:
        raise ProtocolError("QoS > 0 PUBLISH requires a packet identifier")
    if level == QoS.AT_MOST_ONCE and mid is not None:
        raise ProtocolError("QoS 0 PUBLISH must not carry a packet identifier")
    if level == QoS.AT_MOST_ONCE and dup:
        raise ProtocolError("QoS 0 PUBLISH must not set DUP")
    if mid is not None:
        _require_outbound_mid(mid, "PUBLISH")

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
) -> tuple[str, bytes, int | None, bool, bool, Properties]:
    """Decode MQTT 5 PUBLISH fields without constructing a PublishPacket."""
    dup = bool(raw.flags & 0x08)
    if qos is QoS.AT_MOST_ONCE and dup:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    topic, pos = unpack_utf8(raw.remaining)
    mid: int | None = None
    if qos:
        mid, pos = unpack_u16(raw.remaining, pos)
        require_nonzero_mid(mid, "PUBLISH")
    properties, pos = decode_properties(raw.remaining, pos, PUBLISH)
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
) -> WriteItem:
    if type(qos) is QoS:
        level = qos
    else:
        try:
            level = QoS(qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid PUBLISH QoS {qos!r}") from exc
    if level and mid is None:
        raise ProtocolError("QoS > 0 PUBLISH requires a packet identifier")
    if level == QoS.AT_MOST_ONCE and mid is not None:
        raise ProtocolError("QoS 0 PUBLISH must not carry a packet identifier")
    if level == QoS.AT_MOST_ONCE and dup:
        raise ProtocolError("QoS 0 PUBLISH must not set DUP")
    if mid is not None:
        _require_outbound_mid(mid, PUBLISH)

    flags = (int(level) << 1) | (0x01 if retain else 0) | (0x08 if dup else 0)
    topic_bytes = encode_utf8(topic) if _topic_bytes is None else _topic_bytes
    topic_size = len(topic_bytes)
    if not properties or not properties.values:
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
