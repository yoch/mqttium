"""PUBLISH encode/decode.

Specialized v3.1.1 / v5.0 primitives own the common encode and inbound field
decode paths. ``PublishPacket`` remains the Provisional typed view and the
MQTT 3.1 fallback decoder.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.packet_validation import require_nonzero_mid
from mqttium.codec.primitives import unpack_utf8, unpack_u16
from mqttium.codec.properties import PUBLISH, decode_properties
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets._publish_v311 import encode_publish_item_v311
from mqttium.packets._publish_v5 import encode_publish_item_v5
from mqttium.topics import validate_received_publish_topic
from mqttium.transport.writes import WriteItem
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
    _topic_bytes: bytes | None = None,
) -> WriteItem:
    """Validate and encode one outbound PUBLISH exactly once."""
    if protocol is MQTTProtocolVersion.MQTTv5:
        return encode_publish_item_v5(
            topic,
            payload,
            qos=qos,
            retain=retain,
            dup=dup,
            mid=mid,
            properties=properties,
            _topic_bytes=_topic_bytes,
        )
    return encode_publish_item_v311(
        topic,
        payload,
        qos=qos,
        retain=retain,
        dup=dup,
        mid=mid,
        properties=properties,
        _topic_bytes=_topic_bytes,
    )


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
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_publish_item_v5(
                self.topic,
                self.payload,
                qos=self.qos,
                retain=self.retain,
                dup=self.dup,
                mid=self.mid,
                properties=self.properties,
            )
        return encode_publish_item_v311(
            self.topic,
            self.payload,
            qos=self.qos,
            retain=self.retain,
            dup=self.dup,
            mid=self.mid,
            properties=self.properties,
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
        # Validate before the inbound session can mutate alias/state. MQTT 5
        # alone may defer an empty Topic Name for Topic Alias resolution.
        validate_received_publish_topic(topic, utf8_validated=True)
        if protocol != MQTTProtocolVersion.MQTTv5 and not topic:
            raise MalformedPacketError("PUBLISH topic must not be empty")
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
