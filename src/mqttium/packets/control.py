"""PINGREQ / PINGRESP / DISCONNECT / AUTH encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import ProtocolError
from mqttium.packets._control import (
    decode_disconnect_v311,
    encode_disconnect_v311,
    encode_pingreq as encode_pingreq_v311,
    encode_pingresp as encode_pingresp_v311,
)
from mqttium.packets._control import (
    decode_auth_v5,
    decode_disconnect_v5,
    encode_auth_v5,
    encode_disconnect_v5,
)
from mqttium.types import Properties


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
        return encode_auth_v5(self.reason_code, self.properties)

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


# PINGREQ/PINGRESP are version-invariant; alias rather than wrap.
encode_pingreq = encode_pingreq_v311
encode_pingresp = encode_pingresp_v311


def encode_disconnect(
    reason_code: int = 0,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    properties: Properties | None = None,
) -> bytes:
    if protocol is MQTTProtocolVersion.MQTTv5:
        return encode_disconnect_v5(reason_code, properties)
    return encode_disconnect_v311(reason_code, properties)
