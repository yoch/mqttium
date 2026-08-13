"""Specialized MQTT 3.1.1 control packet primitives."""

from __future__ import annotations

from mqttium.codec.packet_validation import require_end
from mqttium.codec.properties import DISCONNECT
from mqttium.errors import ProtocolError
from mqttium.types import Properties

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
