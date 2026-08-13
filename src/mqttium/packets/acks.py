"""PUBACK / PUBREC / PUBREL / PUBCOMP encode/decode.

Specialized v3.1.1 / v5.0 primitives own the wire parse and frame build.
The packet dataclasses are thin factories over those primitives for tests,
fuzzing and Provisional ``mqttium.packets`` consumers.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion, PacketType
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


def encode_success_ack(packet_type: PacketType, mid: int, *, flags: int = 0) -> bytes:
    """Fixed four-byte success/no-properties acknowledgement.

    MQTT 5 omits a zero reason code when nothing follows it, so the frame is
    identical in both protocol versions. Reason code 0 is in every ack's allowed
    set. This is the common case on both QoS legs.
    """
    return bytes((int(packet_type) | (flags & 0x0F), 2, mid >> 8, mid & 0xFF))


def _decode_ack(
    remaining: bytes,
    protocol: MQTTProtocolVersion,
    decode_v311,
    decode_v5,
):
    if protocol is MQTTProtocolVersion.MQTTv5:
        return decode_v5(remaining)
    return decode_v311(remaining)


def _encode_ack(
    protocol: MQTTProtocolVersion,
    encode_v311,
    encode_v5,
    mid: int,
    reason_code: int,
    properties: Properties | None,
) -> bytes:
    if protocol is MQTTProtocolVersion.MQTTv5:
        return encode_v5(mid, reason_code, properties)
    return encode_v311(mid, reason_code, properties)


@dataclass(slots=True, frozen=True)
class PubAckPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack(
            protocol,
            encode_puback_v311,
            encode_puback_v5,
            self.mid,
            self.reason_code,
            self.properties,
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubAckPacket:
        mid, reason, props = _decode_ack(
            remaining, protocol, decode_puback_v311, decode_puback_v5
        )
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRecPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack(
            protocol,
            encode_pubrec_v311,
            encode_pubrec_v5,
            self.mid,
            self.reason_code,
            self.properties,
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRecPacket:
        mid, reason, props = _decode_ack(
            remaining, protocol, decode_pubrec_v311, decode_pubrec_v5
        )
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRelPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack(
            protocol,
            encode_pubrel_v311,
            encode_pubrel_v5,
            self.mid,
            self.reason_code,
            self.properties,
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRelPacket:
        mid, reason, props = _decode_ack(
            remaining, protocol, decode_pubrel_v311, decode_pubrel_v5
        )
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubCompPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack(
            protocol,
            encode_pubcomp_v311,
            encode_pubcomp_v5,
            self.mid,
            self.reason_code,
            self.properties,
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubCompPacket:
        mid, reason, props = _decode_ack(
            remaining, protocol, decode_pubcomp_v311, decode_pubcomp_v5
        )
        return cls(mid=mid, reason_code=reason, properties=props)
