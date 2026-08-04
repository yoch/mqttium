"""SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.packet_validation import (
    require_end,
    require_nonzero_mid,
    require_reason_code,
)
from mqttium.codec.primitives import pack_utf8, pack_u16, unpack_u16
from mqttium.codec.properties import (
    SUBACK,
    SUBSCRIBE,
    UNSUBACK,
    UNSUBSCRIBE,
    decode_properties,
)
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets._common import _props_or_empty, _require_outbound_mid, encode_frame
from mqttium.types import Properties

_SUBACK_V311_REASONS = frozenset({0x00, 0x01, 0x02, 0x80})
_SUBACK_V5_REASONS = frozenset(
    {0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2}
)
_UNSUBACK_V5_REASONS = frozenset({0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91})


@dataclass(slots=True, frozen=True)
class SubscribeOptions:
    qos: QoS = QoS.AT_MOST_ONCE
    no_local: bool = False
    retain_as_published: bool = False
    retain_handling: int = 0  # 0, 1, or 2

    def encode_byte(self, protocol: MQTTProtocolVersion) -> int:
        try:
            qos = QoS(self.qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid subscribe QoS {self.qos!r}") from exc
        if protocol != MQTTProtocolVersion.MQTTv5:
            if self.no_local or self.retain_as_published or self.retain_handling:
                raise ProtocolError("SubscribeOptions v5 flags require MQTT 5")
            return int(qos)
        if self.retain_handling not in (0, 1, 2):
            raise ProtocolError("retain_handling must be 0, 1, or 2")
        return (
            int(qos)
            | (int(self.no_local) << 2)
            | (int(self.retain_as_published) << 3)
            | ((self.retain_handling & 0x03) << 4)
        )


@dataclass(slots=True, frozen=True)
class Subscription:
    topic: str
    options: SubscribeOptions = SubscribeOptions()


@dataclass(slots=True, frozen=True)
class SubscribePacket:
    mid: int
    subscriptions: tuple[Subscription, ...]
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if not self.subscriptions:
            raise ProtocolError("SUBSCRIBE requires at least one filter")
        _require_outbound_mid(self.mid, SUBSCRIBE)
        body = bytearray()
        body.extend(pack_u16(self.mid))
        body.extend(_props_or_empty(self.properties, SUBSCRIBE, protocol))
        for sub in self.subscriptions:
            body.extend(pack_utf8(sub.topic))
            body.append(sub.options.encode_byte(protocol))
        return encode_frame(PacketType.SUBSCRIBE, 0x02, body)


@dataclass(slots=True, frozen=True)
class SubAckPacket:
    mid: int
    reason_codes: tuple[int, ...]
    properties: Properties | None = None

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> SubAckPacket:
        if len(remaining) < 3:
            raise MalformedPacketError("SUBACK too short")
        mid, pos = unpack_u16(remaining, 0)
        require_nonzero_mid(mid, SUBACK)
        properties: Properties | None = None
        if protocol == MQTTProtocolVersion.MQTTv5:
            properties, pos = decode_properties(remaining, pos, SUBACK)
            allowed = _SUBACK_V5_REASONS
        else:
            allowed = _SUBACK_V311_REASONS
        if pos >= len(remaining):
            raise MalformedPacketError("SUBACK missing reason codes")
        reason_codes = tuple(remaining[pos:])
        for reason in reason_codes:
            require_reason_code(reason, allowed, SUBACK)
        return cls(mid=mid, reason_codes=reason_codes, properties=properties)


@dataclass(slots=True, frozen=True)
class UnsubscribePacket:
    mid: int
    topics: tuple[str, ...]
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if not self.topics:
            raise ProtocolError("UNSUBSCRIBE requires at least one filter")
        _require_outbound_mid(self.mid, UNSUBSCRIBE)
        body = bytearray()
        body.extend(pack_u16(self.mid))
        body.extend(_props_or_empty(self.properties, UNSUBSCRIBE, protocol))
        for topic in self.topics:
            body.extend(pack_utf8(topic))
        return encode_frame(PacketType.UNSUBSCRIBE, 0x02, body)


@dataclass(slots=True, frozen=True)
class UnsubAckPacket:
    mid: int
    reason_codes: tuple[int, ...] = ()
    properties: Properties | None = None

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> UnsubAckPacket:
        if len(remaining) < 2:
            raise MalformedPacketError("UNSUBACK too short")
        mid, pos = unpack_u16(remaining, 0)
        require_nonzero_mid(mid, UNSUBACK)
        if protocol == MQTTProtocolVersion.MQTTv5:
            properties, pos = decode_properties(remaining, pos, UNSUBACK)
            if pos >= len(remaining):
                raise MalformedPacketError("UNSUBACK missing reason codes")
            reason_codes = tuple(remaining[pos:])
            for reason in reason_codes:
                require_reason_code(reason, _UNSUBACK_V5_REASONS, UNSUBACK)
            return cls(mid=mid, reason_codes=reason_codes, properties=properties)
        require_end(pos, len(remaining), UNSUBACK)
        return cls(mid=mid)
