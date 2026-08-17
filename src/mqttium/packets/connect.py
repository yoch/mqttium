"""CONNECT / CONNACK encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import ProtocolError
from mqttium.packets._connack import decode_connack_v311, decode_connack_v5
from mqttium.packets._connect import (
    encode_connect_v31,
    encode_connect_v311,
    encode_connect_v5,
)
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

    def encode(self) -> bytes:
        if self.protocol is MQTTProtocolVersion.MQTTv311:
            encoder = encode_connect_v311
        elif self.protocol is MQTTProtocolVersion.MQTTv5:
            encoder = encode_connect_v5
        elif self.protocol is MQTTProtocolVersion.MQTTv31:
            encoder = encode_connect_v31
        else:
            raise ProtocolError(f"Unsupported protocol {self.protocol}")
        return encoder(
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
