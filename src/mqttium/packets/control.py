"""PINGREQ / PINGRESP / DISCONNECT / AUTH encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.properties import AUTH, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets._control_v311 import (
    decode_disconnect_v311,
    encode_disconnect_v311,
    encode_pingreq as encode_pingreq_v311,
    encode_pingresp as encode_pingresp_v311,
)
from mqttium.packets._control_v5 import (
    decode_auth_v5,
    decode_disconnect_v5,
    encode_disconnect_v5,
)
from mqttium.packets._common import _require_outbound_reason, encode_frame
from mqttium.types import Properties

_AUTH_REASONS = frozenset({0x00, 0x18, 0x19})


@dataclass(slots=True, frozen=True)
class DisconnectPacket:
    reason_code: int = 0
    properties: Properties | None = None

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> DisconnectPacket:
        if protocol is MQTTProtocolVersion.MQTTv5:
            reason, properties = decode_disconnect_v5(remaining)
        else:
            reason, properties = decode_disconnect_v311(remaining)
        return cls(reason_code=reason, properties=properties)


@dataclass(slots=True, frozen=True)
class AuthPacket:
    """MQTT 5 AUTH (enhanced authentication exchange)."""

    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5) -> bytes:
        if protocol != MQTTProtocolVersion.MQTTv5:
            raise ProtocolError("AUTH requires MQTT 5")
        _require_outbound_reason(self.reason_code, _AUTH_REASONS, AUTH)
        body = bytearray()
        body.append(self.reason_code)
        body.extend(encode_properties(self.properties, AUTH))
        return encode_frame(PacketType.AUTH, 0, body)

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5,
    ) -> AuthPacket:
        if protocol != MQTTProtocolVersion.MQTTv5:
            raise ProtocolError("AUTH requires MQTT 5")
        reason, properties = decode_auth_v5(remaining)
        return cls(reason_code=reason, properties=properties)


def encode_pingreq() -> bytes:
    return encode_pingreq_v311()


def encode_pingresp() -> bytes:
    return encode_pingresp_v311()


def encode_disconnect(
    reason_code: int = 0,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    properties: Properties | None = None,
) -> bytes:
    if protocol is MQTTProtocolVersion.MQTTv5:
        return encode_disconnect_v5(reason_code, properties)
    return encode_disconnect_v311(reason_code, properties)
