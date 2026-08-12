"""PINGREQ / PINGRESP / DISCONNECT / AUTH encode/decode."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.packet_validation import require_end, require_reason_code
from mqttium.codec.properties import AUTH, DISCONNECT, decode_properties, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets._common import _require_outbound_reason, encode_frame
from mqttium.types import Properties

_DISCONNECT_V5_REASONS = frozenset(
    {
        0x00,
        0x04,
        0x80,
        0x81,
        0x82,
        0x83,
        0x87,
        0x89,
        0x8B,
        0x8C,
        0x8D,
        0x8E,
        0x8F,
        0x90,
        0x93,
        0x94,
        0x95,
        0x96,
        0x97,
        0x98,
        0x99,
        0x9A,
        0x9B,
        0x9C,
        0x9D,
        0x9E,
        0x9F,
        0xA0,
        0xA1,
        0xA2,
    }
)
# MQTT 5 Table 3-10 is directional. The decoder accepts every DISCONNECT
# reason a Server may legally send; the Client encoder is restricted to values
# whose "Sent by" column includes Client.
_CLIENT_DISCONNECT_V5_REASONS = frozenset(
    {
        0x00,
        0x04,
        0x80,
        0x81,
        0x82,
        0x83,
        0x90,
        0x93,
        0x94,
        0x95,
        0x96,
        0x97,
        0x98,
        0x99,
    }
)
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
        if protocol != MQTTProtocolVersion.MQTTv5:
            require_end(0, len(remaining), DISCONNECT)
            return cls(reason_code=0, properties=None)
        if not remaining:
            return cls(reason_code=0, properties=Properties())
        reason = remaining[0]
        require_reason_code(reason, _DISCONNECT_V5_REASONS, DISCONNECT)
        properties: Properties | None
        if len(remaining) > 1:
            properties, pos = decode_properties(remaining, 1, DISCONNECT)
            require_end(pos, len(remaining), DISCONNECT)
        else:
            properties = Properties()
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
        if not remaining:
            return cls(reason_code=0, properties=Properties())
        reason = remaining[0]
        require_reason_code(reason, _AUTH_REASONS, AUTH)
        if len(remaining) > 1:
            properties, pos = decode_properties(remaining, 1, AUTH)
            require_end(pos, len(remaining), AUTH)
        else:
            properties = Properties()
        return cls(reason_code=reason, properties=properties)


def encode_pingreq() -> bytes:
    return encode_frame(PacketType.PINGREQ, 0, b"")


def encode_pingresp() -> bytes:
    return encode_frame(PacketType.PINGRESP, 0, b"")


def encode_disconnect(
    reason_code: int = 0,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    properties: Properties | None = None,
) -> bytes:
    if protocol == MQTTProtocolVersion.MQTTv5:
        _require_outbound_reason(reason_code, _CLIENT_DISCONNECT_V5_REASONS, DISCONNECT)
        if properties is not None and properties.get("server_reference") is not None:
            raise ProtocolError(
                "DISCONNECT server_reference is Server-only and cannot be sent by a Client"
            )
        if reason_code == 0 and not (properties and properties.values):
            return encode_frame(PacketType.DISCONNECT, 0, b"")
        body = bytearray()
        body.append(reason_code)
        body.extend(encode_properties(properties, DISCONNECT))
        return encode_frame(PacketType.DISCONNECT, 0, body)
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("DISCONNECT reason/properties require MQTT 5")
    return encode_frame(PacketType.DISCONNECT, 0, b"")
