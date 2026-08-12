"""CONNECT / CONNACK encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.packet_validation import require_end, require_reason_code
from mqttium.codec.primitives import pack_utf8, pack_u16
from mqttium.codec.properties import CONNACK, CONNECT, WILL, decode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets._common import _props_or_empty, encode_frame
from mqttium.types import Properties

_CONNACK_V311_REASONS = frozenset({0, 1, 2, 3, 4, 5})
_CONNACK_V5_REASONS = frozenset(
    {
        0x00,
        0x80,
        0x81,
        0x82,
        0x83,
        0x84,
        0x85,
        0x86,
        0x87,
        0x88,
        0x89,
        0x8A,
        0x8C,
        0x90,
        0x95,
        0x97,
        0x99,
        0x9A,
        0x9B,
        0x9C,
        0x9D,
        0x9F,
    }
)


@dataclass(slots=True, frozen=True)
class ConnectPacket:
    client_id: str
    clean_start: bool = True
    keepalive: int = 60
    username: str | None = None
    password: bytes | None = None
    will_topic: str | None = None
    will_payload: bytes = b""
    will_qos: QoS = QoS.AT_MOST_ONCE
    will_retain: bool = False
    will_properties: Properties | None = None
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311
    properties: Properties | None = None

    def encode(self) -> bytes:  # noqa: C901
        if self.protocol == MQTTProtocolVersion.MQTTv31:
            protocol_name = b"MQIsdp"
            protocol_level = 3
        elif self.protocol == MQTTProtocolVersion.MQTTv311:
            protocol_name = b"MQTT"
            protocol_level = 4
        elif self.protocol == MQTTProtocolVersion.MQTTv5:
            protocol_name = b"MQTT"
            protocol_level = 5
        else:
            raise ProtocolError(f"Unsupported protocol {self.protocol}")
        if not 0 <= self.keepalive <= 65535:
            raise ProtocolError("keepalive must be in 0..65535")
        try:
            will_qos = QoS(self.will_qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid Will QoS {self.will_qos!r}") from exc
        if self.will_topic is None and (will_qos or self.will_retain):
            raise ProtocolError("Will QoS/retain require a Will topic")
        if self.password is not None and len(self.password) > 65535:
            raise ProtocolError("Password exceeds MQTT binary-data limit")
        if len(self.will_payload) > 65535:
            raise ProtocolError("Will payload exceeds MQTT binary-data limit")
        if (
            self.protocol == MQTTProtocolVersion.MQTTv5
            and self.properties is not None
            and self.properties.get("authentication_data") is not None
            and self.properties.get("authentication_method") is None
        ):
            raise ProtocolError("CONNECT authentication_data requires authentication_method")

        flags = 0
        if self.clean_start:
            flags |= 0x02
        if self.will_topic is not None:
            flags |= 0x04
            flags |= int(will_qos) << 3
            if self.will_retain:
                flags |= 0x20
        if self.password is not None:
            flags |= 0x40
        if self.username is not None:
            flags |= 0x80

        body = bytearray()
        body.extend(pack_u16(len(protocol_name)))
        body.extend(protocol_name)
        body.append(protocol_level)
        body.append(flags)
        body.extend(pack_u16(self.keepalive))
        body.extend(_props_or_empty(self.properties, CONNECT, self.protocol))
        body.extend(pack_utf8(self.client_id))
        if self.will_topic is not None:
            body.extend(_props_or_empty(self.will_properties, WILL, self.protocol))
            body.extend(pack_utf8(self.will_topic))
            body.extend(pack_u16(len(self.will_payload)))
            body.extend(self.will_payload)
        if self.username is not None:
            body.extend(pack_utf8(self.username))
        if self.password is not None:
            body.extend(pack_u16(len(self.password)))
            body.extend(self.password)
        return encode_frame(PacketType.CONNECT, 0, body)


@dataclass(slots=True, frozen=True)
class ConnAckPacket:
    session_present: bool
    reason_code: int
    properties: Properties | None = None

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> ConnAckPacket:
        if len(remaining) < 2:
            raise MalformedPacketError("CONNACK too short")
        if remaining[0] not in (0, 1):
            raise MalformedPacketError("CONNACK contains invalid acknowledge flags")
        session_present = bool(remaining[0])
        reason_code = remaining[1]
        properties: Properties | None = None
        if protocol == MQTTProtocolVersion.MQTTv5:
            require_reason_code(reason_code, _CONNACK_V5_REASONS, CONNACK)
            properties, pos = decode_properties(remaining, 2, CONNACK)
            require_end(pos, len(remaining), CONNACK)
        else:
            require_reason_code(reason_code, _CONNACK_V311_REASONS, CONNACK)
            require_end(2, len(remaining), CONNACK)
        if reason_code != 0 and session_present:
            raise MalformedPacketError(
                "CONNACK Session Present must be 0 when connection is refused"
            )
        return cls(
            session_present=session_present,
            reason_code=reason_code,
            properties=properties,
        )
