"""Specialized MQTT 3.1.1 CONNECT encode."""

from __future__ import annotations

from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets._common import encode_frame
from mqttium.types import Properties


def encode_connect_v311(
    client_id: str,
    clean_start: bool = True,
    keepalive: int = 60,
    username: str | None = None,
    password: bytes | None = None,
    will_topic: str | None = None,
    will_payload: bytes = b"",
    will_qos: QoS = QoS.AT_MOST_ONCE,
    will_retain: bool = False,
    will_properties: Properties | None = None,
    properties: Properties | None = None,
) -> bytes:
    if not 0 <= keepalive <= 65535:
        raise ProtocolError("keepalive must be in 0..65535")
    try:
        encoded_will_qos = QoS(will_qos)
    except ValueError as exc:
        raise ProtocolError(f"Invalid Will QoS {will_qos!r}") from exc
    if will_topic is None and (encoded_will_qos or will_retain):
        raise ProtocolError("Will QoS/retain require a Will topic")
    if password is not None and len(password) > 65535:
        raise ProtocolError("Password exceeds MQTT binary-data limit")
    if len(will_payload) > 65535:
        raise ProtocolError("Will payload exceeds MQTT binary-data limit")

    flags = 0
    if clean_start:
        flags |= 0x02
    if will_topic is not None:
        flags |= 0x04 | (int(encoded_will_qos) << 3)
        if will_retain:
            flags |= 0x20
    if password is not None:
        flags |= 0x40
    if username is not None:
        flags |= 0x80

    body = bytearray(b"\x00\x04MQTT\x04")
    body.append(flags)
    body.extend(pack_u16(keepalive))
    body.extend(pack_utf8(client_id))
    if will_topic is not None:
        body.extend(pack_utf8(will_topic))
        body.extend(pack_u16(len(will_payload)))
        body.extend(will_payload)
    if username is not None:
        body.extend(pack_utf8(username))
    if password is not None:
        body.extend(pack_u16(len(password)))
        body.extend(password)
    return encode_frame(PacketType.CONNECT, 0, body)
