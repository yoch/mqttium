"""Version-specialized MQTT control packet primitives.

Functions remain deliberately specialized by protocol version; grouping them
by packet family keeps navigation simple without adding hot-path dispatch.
"""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end
from mqttium.codec.properties import DISCONNECT
from mqttium.errors import ProtocolError
from mqttium.types import Properties
from mqttium.codec.packet_validation import require_reason_code
from mqttium.codec.properties import AUTH, decode_properties, encode_properties
from mqttium.enums import PacketType
from mqttium.packets._common import _require_outbound_reason, encode_frame


_PINGREQ = b"\xc0\x00"


_PINGRESP = b"\xd0\x00"


_DISCONNECT = b"\xe0\x00"


def decode_disconnect_v311(remaining: bytes) -> tuple[int, Properties | None]:
    require_end(0, len(remaining), DISCONNECT)
    return 0, None


def encode_pingreq() -> bytes:
    return _PINGREQ


def encode_pingresp() -> bytes:
    return _PINGRESP


def encode_disconnect_v311(
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("DISCONNECT reason/properties require MQTT 5")
    return _DISCONNECT


_DISCONNECT_REASONS = frozenset(
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


_CLIENT_DISCONNECT_REASONS = frozenset(
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

# MQTT 5 Table 3-10: 0x04 (Disconnect with Will Message) is Client-only.
_SERVER_DISCONNECT_REASONS = _DISCONNECT_REASONS - {0x04}


_AUTH_REASONS = frozenset({0x00, 0x18, 0x19})


def _decode_control_v5(
    remaining: bytes,
    packet_name: str,
    allowed: frozenset[int],
) -> tuple[int, Properties | None]:
    if not remaining:
        return 0, Properties()
    reason = remaining[0]
    require_reason_code(reason, allowed, packet_name)
    if len(remaining) == 1:
        return reason, Properties()
    properties, pos = decode_properties(remaining, 1, packet_name)
    require_end(pos, len(remaining), packet_name)
    return reason, properties


def decode_disconnect_v5(remaining: bytes) -> tuple[int, Properties | None]:
    return _decode_control_v5(remaining, DISCONNECT, _SERVER_DISCONNECT_REASONS)


def decode_auth_v5(remaining: bytes) -> tuple[int, Properties | None]:
    return _decode_control_v5(remaining, AUTH, _AUTH_REASONS)


def encode_disconnect_v5(
    reason_code: int = 0,
    properties: Properties | None = None,
) -> bytes:
    _require_outbound_reason(reason_code, _CLIENT_DISCONNECT_REASONS, DISCONNECT)
    if properties is not None and properties.get("server_reference") is not None:
        raise ProtocolError(
            "DISCONNECT server_reference is Server-only and cannot be sent by a Client"
        )
    if reason_code == 0 and not (properties and properties.values):
        return _DISCONNECT
    body = bytearray((reason_code,))
    body.extend(encode_properties(properties, DISCONNECT))
    return encode_frame(PacketType.DISCONNECT, 0, body)
