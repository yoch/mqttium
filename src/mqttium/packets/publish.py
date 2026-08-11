"""PUBLISH encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.packet_validation import require_nonzero_mid
from mqttium.codec.primitives import encode_utf8, unpack_utf8, unpack_u16
from mqttium.codec.properties import PUBLISH, decode_properties
from mqttium.codec.vbi import append_vbi
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets._common import _props_or_empty, _require_outbound_mid
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem
from mqttium.types import Properties


def encode_publish_item(
    topic: str,
    payload: bytes,
    *,
    qos: QoS | int,
    retain: bool,
    dup: bool,
    mid: int | None,
    properties: Properties | None,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    property_wire: bytes | None = None,
    topic_validated: bool = False,
) -> WriteItem:
    """Validate and encode one outbound PUBLISH exactly once.

    ``property_wire`` reuses a table already produced by admission sizing.
    ``topic_validated`` skips a second MQTT-UTF-8 pass when the caller has
    already run ``validate_publish_topic``.
    """

    # Every internal caller has already validated and converted, so the common
    # case is an exact QoS member. Re-running the enum call on it costs about
    # 0.32 us -- roughly 4% of a QoS 0 publication -- to return its argument.
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
    if topic_validated:
        topic_bytes = topic.encode("ascii") if topic.isascii() else topic.encode("utf-8")
        if len(topic_bytes) > 65535:
            raise ProtocolError("UTF-8 string too long for MQTT")
    else:
        topic_bytes = encode_utf8(topic)
    topic_size = len(topic_bytes)
    if property_wire is not None:
        props = property_wire
    else:
        props = _props_or_empty(properties, PUBLISH, protocol)
    payload_size = len(payload)
    remaining_length = 2 + topic_size + (2 if level else 0) + len(props) + payload_size

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


@dataclass(slots=True, frozen=True)
class PublishPacket:
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    dup: bool
    mid: int | None = None
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        item = self.encode_write_item(protocol)
        if isinstance(item, bytes):
            return item
        return item[0] + item[1]

    def encode_write_item(
        self,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> WriteItem:
        return encode_publish_item(
            self.topic,
            self.payload,
            qos=self.qos,
            retain=self.retain,
            dup=self.dup,
            mid=self.mid,
            properties=self.properties,
            protocol=protocol,
        )

    @classmethod
    def decode(
        cls,
        flags: int,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PublishPacket:
        dup = bool(flags & 0x08)
        qos_raw = (flags >> 1) & 0x03
        if qos_raw == 3:
            raise MalformedPacketError("Invalid PUBLISH QoS 3")
        qos = QoS(qos_raw)
        if qos == QoS.AT_MOST_ONCE and dup:
            raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
        retain = bool(flags & 0x01)

        topic, pos = unpack_utf8(remaining, 0)
        mid: int | None = None
        if qos:
            mid, pos = unpack_u16(remaining, pos)
            require_nonzero_mid(mid, PUBLISH)
        properties: Properties | None = None
        if protocol == MQTTProtocolVersion.MQTTv5:
            properties, pos = decode_properties(remaining, pos, PUBLISH)
        # The remainder is the PUBLISH payload and may be empty.
        payload = remaining[pos:]
        return cls(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            dup=dup,
            mid=mid,
            properties=properties,
        )
