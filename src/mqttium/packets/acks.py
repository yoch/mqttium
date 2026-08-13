"""PUBACK / PUBREC / PUBREL / PUBCOMP encode/decode.

Specialized v3.1.1 / v5.0 primitives own the wire parse and frame build.
The packet dataclasses are thin factories over those primitives for tests,
fuzzing and Provisional ``mqttium.packets`` consumers.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion
from mqttium.packets._ack_v311 import (
    decode_puback_v311,
    decode_pubcomp_v311,
    decode_pubrec_v311,
    decode_pubrel_v311,
    encode_puback_v311,
    encode_pubcomp_v311,
    encode_pubrec_v311,
    encode_pubrel_v311,
)
from mqttium.packets._ack_v5 import (
    decode_puback_v5,
    decode_pubcomp_v5,
    decode_pubrec_v5,
    decode_pubrel_v5,
    encode_puback_v5,
    encode_pubcomp_v5,
    encode_pubrec_v5,
    encode_pubrel_v5,
)
from mqttium.types import Properties


@dataclass(slots=True, frozen=True)
class PubAckPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_puback_v5(self.mid, self.reason_code, self.properties)
        return encode_puback_v311(self.mid, self.reason_code, self.properties)

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubAckPacket:
        if protocol is MQTTProtocolVersion.MQTTv5:
            mid, reason, props = decode_puback_v5(remaining)
        else:
            mid, reason, props = decode_puback_v311(remaining)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRecPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_pubrec_v5(self.mid, self.reason_code, self.properties)
        return encode_pubrec_v311(self.mid, self.reason_code, self.properties)

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRecPacket:
        if protocol is MQTTProtocolVersion.MQTTv5:
            mid, reason, props = decode_pubrec_v5(remaining)
        else:
            mid, reason, props = decode_pubrec_v311(remaining)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRelPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_pubrel_v5(self.mid, self.reason_code, self.properties)
        return encode_pubrel_v311(self.mid, self.reason_code, self.properties)

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRelPacket:
        if protocol is MQTTProtocolVersion.MQTTv5:
            mid, reason, props = decode_pubrel_v5(remaining)
        else:
            mid, reason, props = decode_pubrel_v311(remaining)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubCompPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if protocol is MQTTProtocolVersion.MQTTv5:
            return encode_pubcomp_v5(self.mid, self.reason_code, self.properties)
        return encode_pubcomp_v311(self.mid, self.reason_code, self.properties)

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubCompPacket:
        if protocol is MQTTProtocolVersion.MQTTv5:
            mid, reason, props = decode_pubcomp_v5(remaining)
        else:
            mid, reason, props = decode_pubcomp_v311(remaining)
        return cls(mid=mid, reason_code=reason, properties=props)
