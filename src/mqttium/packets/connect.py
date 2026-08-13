"""CONNECT / CONNACK encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.primitives import pack_utf8, pack_u16
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets._connack import decode_connack_v311, decode_connack_v5
from mqttium.packets._connect import encode_connect_v311, encode_connect_v5
from mqttium.packets._common import encode_frame
from mqttium.types import Properties


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

    def encode(self) -> bytes:  # noqa: C901 - includes the legacy MQTT 3.1 encoder
        if self.protocol is MQTTProtocolVersion.MQTTv311:
            return encode_connect_v311(
                self.client_id,
                self.clean_start,
                self.keepalive,
                self.username,
                self.password,
                self.will_topic,
                self.will_payload,
                self.will_qos,
                self.will_retain,
                self.will_properties,
                self.properties,
            )
        if self.protocol is MQTTProtocolVersion.MQTTv5:
            return encode_connect_v5(
                self.client_id,
                self.clean_start,
                self.keepalive,
                self.username,
                self.password,
                self.will_topic,
                self.will_payload,
                self.will_qos,
                self.will_retain,
                self.will_properties,
                self.properties,
            )
        if self.protocol is not MQTTProtocolVersion.MQTTv31:
            raise ProtocolError(f"Unsupported protocol {self.protocol}")

        protocol_name = b"MQIsdp"
        protocol_level = 3
        if not 0 <= self.keepalive <= 65535:
            raise ProtocolError("keepalive must be in 0..65535")
        try:
            will_qos = QoS(self.will_qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid Will QoS {self.will_qos!r}") from exc
        if self.will_topic is None:
            if will_qos or self.will_retain:
                raise ProtocolError("Will QoS/retain require a Will topic")
            if self.will_payload or (
                self.will_properties is not None and self.will_properties.values
            ):
                raise ProtocolError("Will payload/properties require a Will topic")
        if self.password is not None and len(self.password) > 65535:
            raise ProtocolError("Password exceeds MQTT binary-data limit")
        if len(self.will_payload) > 65535:
            raise ProtocolError("Will payload exceeds MQTT binary-data limit")
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
        body.extend(pack_utf8(self.client_id))
        if self.will_topic is not None:
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
        if protocol is MQTTProtocolVersion.MQTTv5:
            session_present, reason_code, properties = decode_connack_v5(remaining)
        else:
            session_present, reason_code, properties = decode_connack_v311(remaining)
        return cls(
            session_present=session_present,
            reason_code=reason_code,
            properties=properties,
        )
