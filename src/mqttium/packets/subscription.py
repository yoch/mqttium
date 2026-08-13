"""SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import ProtocolError
from mqttium.packets._suback import (
    decode_suback_v311,
    decode_suback_v5,
    decode_unsuback_v311,
    decode_unsuback_v5,
)
from mqttium.packets._subscribe import (
    encode_subscribe_v311,
    encode_subscribe_v5,
    encode_unsubscribe_v311,
    encode_unsubscribe_v5,
)
from mqttium.types import Properties


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
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_subscribe_v5(self.mid, self.subscriptions, self.properties)
        return encode_subscribe_v311(self.mid, self.subscriptions, self.properties)


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
        if protocol is MQTTProtocolVersion.MQTTv5:
            mid, reason_codes, properties = decode_suback_v5(remaining)
        else:
            mid, reason_codes, properties = decode_suback_v311(remaining)
        return cls(mid=mid, reason_codes=reason_codes, properties=properties)


@dataclass(slots=True, frozen=True)
class UnsubscribePacket:
    mid: int
    topics: tuple[str, ...]
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_unsubscribe_v5(self.mid, self.topics, self.properties)
        return encode_unsubscribe_v311(self.mid, self.topics, self.properties)


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
        if protocol is MQTTProtocolVersion.MQTTv5:
            mid, reason_codes, properties = decode_unsuback_v5(remaining)
        else:
            mid, reason_codes, properties = decode_unsuback_v311(remaining)
        return cls(mid=mid, reason_codes=reason_codes, properties=properties)
