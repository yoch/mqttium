"""PUBACK / PUBREC / PUBREL / PUBCOMP encode/decode.

All four share a body shape — packet identifier, optional reason code, optional
properties — so a single decode/encode pair drives every class.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.packet_validation import (
    require_end,
    require_nonzero_mid,
    require_reason_code,
)
from mqttium.codec.primitives import pack_u16, unpack_u16
from mqttium.codec.properties import (
    PUBACK,
    PUBCOMP,
    PUBREC,
    PUBREL,
    decode_properties,
    encode_properties,
)
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets._common import (
    _require_outbound_mid,
    _require_outbound_reason,
    encode_frame,
)
from mqttium.types import Properties

_PUBACK_REASONS = frozenset({0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99})
_PUBREC_REASONS = _PUBACK_REASONS
_PUBREL_REASONS = frozenset({0x00, 0x92})
_PUBCOMP_REASONS = _PUBREL_REASONS
_ACK_REASONS = {
    PUBACK: _PUBACK_REASONS,
    PUBREC: _PUBREC_REASONS,
    PUBREL: _PUBREL_REASONS,
    PUBCOMP: _PUBCOMP_REASONS,
}


def _decode_ack_with_reason(
    remaining: bytes,
    protocol: MQTTProtocolVersion,
    packet_name: str,
) -> tuple[int, int, Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError(f"{packet_name} too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, packet_name)
    reason = 0
    properties: Properties | None = None
    if protocol == MQTTProtocolVersion.MQTTv5:
        if pos < len(remaining):
            reason = remaining[pos]
            pos += 1
            if pos < len(remaining):
                properties, pos = decode_properties(remaining, pos, packet_name)
            else:
                properties = Properties()
        require_reason_code(reason, _ACK_REASONS[packet_name], packet_name)
        require_end(pos, len(remaining), packet_name)
    else:
        require_end(pos, len(remaining), packet_name)
    return mid, reason, properties


def _encode_ack_with_reason(
    packet_type: PacketType,
    flags: int,
    mid: int,
    reason_code: int,
    properties: Properties | None,
    packet_name: str,
    protocol: MQTTProtocolVersion,
) -> bytes:
    _require_outbound_mid(mid, packet_name)
    body = bytearray(pack_u16(mid))
    if protocol == MQTTProtocolVersion.MQTTv5:
        _require_outbound_reason(reason_code, _ACK_REASONS[packet_name], packet_name)
        if reason_code != 0 or (properties and properties.values):
            body.append(reason_code)
            body.extend(encode_properties(properties, packet_name))
    elif reason_code != 0 or (properties and properties.values):
        raise ProtocolError(f"{packet_name} reason/properties require MQTT 5")
    return encode_frame(packet_type, flags, body)


@dataclass(slots=True, frozen=True)
class PubAckPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBACK, 0, self.mid, self.reason_code, self.properties, PUBACK, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubAckPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBACK)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRecPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBREC, 0, self.mid, self.reason_code, self.properties, PUBREC, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRecPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBREC)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRelPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBREL, 0x02, self.mid, self.reason_code, self.properties, PUBREL, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRelPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBREL)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubCompPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBCOMP, 0, self.mid, self.reason_code, self.properties, PUBCOMP, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubCompPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBCOMP)
        return cls(mid=mid, reason_code=reason, properties=props)
