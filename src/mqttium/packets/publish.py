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
        try:
            qos = QoS(self.qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid PUBLISH QoS {self.qos!r}") from exc
        if qos and self.mid is None:
            raise ProtocolError("QoS > 0 PUBLISH requires a packet identifier")
        if qos == QoS.AT_MOST_ONCE and self.mid is not None:
            raise ProtocolError("QoS 0 PUBLISH must not carry a packet identifier")
        if qos == QoS.AT_MOST_ONCE and self.dup:
            raise ProtocolError("QoS 0 PUBLISH must not set DUP")
        if self.mid is not None:
            _require_outbound_mid(self.mid, PUBLISH)

        flags = 0
        if self.retain:
            flags |= 0x01
        flags |= int(qos) << 1
        if self.dup:
            flags |= 0x08

        topic_bytes = encode_utf8(self.topic)
        topic_len = len(topic_bytes)

        props = _props_or_empty(self.properties, PUBLISH, protocol)
        payload = self.payload
        payload_len = len(payload)
        mid_len = 2 if qos else 0
        remaining_length = 2 + topic_len + mid_len + len(props) + payload_len

        # Append-built buffer beats pre-sized index writes for typical small
        # PUBLISH frames (measured ~1.8× on QoS0 microbench).
        out = bytearray()
        out.append(int(PacketType.PUBLISH) | (flags & 0x0F))
        append_vbi(out, remaining_length)
        out.append((topic_len >> 8) & 0xFF)
        out.append(topic_len & 0xFF)
        out.extend(topic_bytes)
        if qos:
            assert self.mid is not None
            mid = self.mid
            out.append((mid >> 8) & 0xFF)
            out.append(mid & 0xFF)
        if props:
            out.extend(props)

        if payload_len >= SEGMENT_THRESHOLD and isinstance(payload, (bytes, bytearray)):
            return (bytes(out), bytes(payload))
        if payload_len:
            out.extend(payload)
        return bytes(out)

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
