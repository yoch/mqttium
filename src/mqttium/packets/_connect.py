"""Version-specialized MQTT CONNECT encode primitives.

Unlike the PUBLISH and acknowledgement primitives, CONNECT is encoded once per
connection and nothing binds a version-specific encoder: `ConnectPacket.encode`
dispatches on `protocol` either way. The three wire layouts differ only in the
protocol name/level prefix and in whether property tables are present, so they
share one implementation instead of repeating the same validation and flag
assembly three times.
"""

from __future__ import annotations

from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets._common import encode_frame
from mqttium.types import Properties
from mqttium.codec.properties import CONNECT, WILL, encode_properties

# Protocol Name + Protocol Level, the only fixed bytes that differ by version.
_PREFIX_V31 = b"\x00\x06MQIsdp\x03"
_PREFIX_V311 = b"\x00\x04MQTT\x04"
_PREFIX_V5 = b"\x00\x04MQTT\x05"


def _validate_connect(
    v5: bool,
    keepalive: int,
    password: bytes | None,
    will_topic: str | None,
    will_payload: bytes,
    will_qos: QoS,
    will_retain: bool,
    will_properties: Properties | None,
    properties: Properties | None,
) -> QoS:
    """Check the wire contract before any byte is assembled, and resolve Will QoS."""
    if not 0 <= keepalive <= 65535:
        raise ProtocolError("keepalive must be in 0..65535")
    if not v5:
        if properties is not None and properties.values:
            raise ProtocolError("CONNECT properties require MQTT 5")
        if will_properties is not None and will_properties.values:
            raise ProtocolError("Will properties require MQTT 5")
    try:
        encoded_will_qos = QoS(will_qos)
    except ValueError as exc:
        raise ProtocolError(f"Invalid Will QoS {will_qos!r}") from exc
    if will_topic is None:
        if encoded_will_qos or will_retain:
            raise ProtocolError("Will QoS/retain require a Will topic")
        if will_payload or (will_properties is not None and will_properties.values):
            raise ProtocolError("Will payload/properties require a Will topic")
    if password is not None and len(password) > 65535:
        raise ProtocolError("Password exceeds MQTT binary-data limit")
    if len(will_payload) > 65535:
        raise ProtocolError("Will payload exceeds MQTT binary-data limit")
    if (
        v5
        and properties is not None
        and properties.get("authentication_data") is not None
        and properties.get("authentication_method") is None
    ):
        raise ProtocolError("CONNECT authentication_data requires authentication_method")
    return encoded_will_qos


def _encode_connect(
    prefix: bytes,
    v5: bool,
    client_id: str,
    clean_start: bool,
    keepalive: int,
    username: str | None,
    password: bytes | None,
    will_topic: str | None,
    will_payload: bytes,
    will_qos: QoS,
    will_retain: bool,
    will_properties: Properties | None,
    properties: Properties | None,
) -> bytes:
    encoded_will_qos = _validate_connect(
        v5,
        keepalive,
        password,
        will_topic,
        will_payload,
        will_qos,
        will_retain,
        will_properties,
        properties,
    )

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

    body = bytearray(prefix)
    body.append(flags)
    body.extend(pack_u16(keepalive))
    if v5:
        body.extend(encode_properties(properties, CONNECT))
    body.extend(pack_utf8(client_id))
    if will_topic is not None:
        if v5:
            body.extend(encode_properties(will_properties, WILL))
        body.extend(pack_utf8(will_topic))
        body.extend(pack_u16(len(will_payload)))
        body.extend(will_payload)
    if username is not None:
        body.extend(pack_utf8(username))
    if password is not None:
        body.extend(pack_u16(len(password)))
        body.extend(password)
    return encode_frame(PacketType.CONNECT, 0, body)


def encode_connect_v31(
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
    return _encode_connect(
        _PREFIX_V31,
        False,
        client_id,
        clean_start,
        keepalive,
        username,
        password,
        will_topic,
        will_payload,
        will_qos,
        will_retain,
        will_properties,
        properties,
    )


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
    return _encode_connect(
        _PREFIX_V311,
        False,
        client_id,
        clean_start,
        keepalive,
        username,
        password,
        will_topic,
        will_payload,
        will_qos,
        will_retain,
        will_properties,
        properties,
    )


def encode_connect_v5(
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
    return _encode_connect(
        _PREFIX_V5,
        True,
        client_id,
        clean_start,
        keepalive,
        username,
        password,
        will_topic,
        will_payload,
        will_qos,
        will_retain,
        will_properties,
        properties,
    )
