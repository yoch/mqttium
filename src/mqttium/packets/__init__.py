"""MQTT packet framing helpers and typed packet views."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.codec.primitives import encode_utf8, pack_utf8, pack_u16, unpack_utf8, unpack_u16
from mqttium.codec.properties import (
    AUTH,
    CONNACK,
    CONNECT,
    DISCONNECT,
    PUBACK,
    PUBCOMP,
    PUBLISH,
    PUBREC,
    PUBREL,
    SUBACK,
    SUBSCRIBE,
    UNSUBACK,
    UNSUBSCRIBE,
    WILL,
    decode_properties,
    encode_properties,
)
from mqttium.codec.vbi import append_vbi
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.protocol.validate import require_end, require_nonzero_mid, require_reason_code
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem
from mqttium.types import Properties


_EMPTY_PROPS_V5 = b"\x00"

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
_PUBACK_REASONS = frozenset({0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99})
_PUBREC_REASONS = _PUBACK_REASONS
_PUBREL_REASONS = frozenset({0x00, 0x92})
_PUBCOMP_REASONS = _PUBREL_REASONS
_SUBACK_V311_REASONS = frozenset({0x00, 0x01, 0x02, 0x80})
_SUBACK_V5_REASONS = frozenset(
    {0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2}
)
_UNSUBACK_V5_REASONS = frozenset({0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91})
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
_AUTH_REASONS = frozenset({0x00, 0x18, 0x19})
_ACK_REASONS = {
    PUBACK: _PUBACK_REASONS,
    PUBREC: _PUBREC_REASONS,
    PUBREL: _PUBREL_REASONS,
    PUBCOMP: _PUBCOMP_REASONS,
}


def encode_frame(packet_type: PacketType, flags: int, remaining: bytes | bytearray) -> bytes:
    if flags & 0xF0:
        raise ValueError("flags must fit in low nibble")
    header = bytearray(1)
    header[0] = int(packet_type) | (flags & 0x0F)
    append_vbi(header, len(remaining))
    header.extend(remaining)
    return bytes(header)


def _props_or_empty(
    props: Properties | None,
    packet: str,
    protocol: MQTTProtocolVersion,
) -> bytes:
    if protocol != MQTTProtocolVersion.MQTTv5:
        return b""
    if not props or not props.values:
        return _EMPTY_PROPS_V5
    return encode_properties(props, packet)


def _require_outbound_mid(mid: int, what: str) -> None:
    if not 1 <= mid <= 65535:
        raise ProtocolError(f"{what} packet identifier must be in 1..65535")


def _require_outbound_reason(reason: int, allowed: frozenset[int], what: str) -> None:
    if reason not in allowed:
        raise ProtocolError(f"{what} contains invalid reason code 0x{reason:02x}")


@dataclass(slots=True, frozen=True)
class SubscribeOptions:
    qos: QoS = QoS.AT_MOST_ONCE
    no_local: bool = False
    retain_as_published: bool = False
    retain_handling: int = 0  # 0, 1, or 2

    def encode_byte(self, protocol: MQTTProtocolVersion) -> int:
        try:
            qos = QoS(self.qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid subscribe QoS {self.qos!r}") from exc
        if protocol != MQTTProtocolVersion.MQTTv5:
            if self.no_local or self.retain_as_published or self.retain_handling:
                raise ProtocolError("SubscribeOptions v5 flags require MQTT 5")
            return int(qos)
        if self.retain_handling not in (0, 1, 2):
            raise ProtocolError("retain_handling must be 0, 1, or 2")
        return (
            int(qos)
            | (int(self.no_local) << 2)
            | (int(self.retain_as_published) << 3)
            | ((self.retain_handling & 0x03) << 4)
        )


@dataclass(slots=True, frozen=True)
class PublishPacket:
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    dup: bool
    mid: int | None = None
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        item = self.encode_write_item(protocol)
        if isinstance(item, bytes):
            return item
        return item[0] + item[1]

    def encode_write_item(
        self,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> WriteItem:
        try:
            qos = QoS(self.qos)
        except ValueError as exc:
            raise ProtocolError(f"Invalid PUBLISH QoS {self.qos!r}") from exc
        if qos and self.mid is None:
            raise ProtocolError("QoS > 0 PUBLISH requires a packet identifier")
        if qos == QoS.AT_MOST_ONCE and self.mid is not None:
            raise ProtocolError("QoS 0 PUBLISH must not carry a packet identifier")
        if qos == QoS.AT_MOST_ONCE and self.dup:
            raise ProtocolError("QoS 0 PUBLISH must not set DUP")
        if self.mid is not None:
            _require_outbound_mid(self.mid, PUBLISH)

        flags = 0
        if self.retain:
            flags |= 0x01
        flags |= int(qos) << 1
        if self.dup:
            flags |= 0x08

        topic_bytes = encode_utf8(self.topic)
        topic_len = len(topic_bytes)

        props = _props_or_empty(self.properties, PUBLISH, protocol)
        payload = self.payload
        payload_len = len(payload)
        mid_len = 2 if qos else 0
        remaining_length = 2 + topic_len + mid_len + len(props) + payload_len

        # Append-built buffer beats pre-sized index writes for typical small
        # PUBLISH frames (measured ~1.8× on QoS0 microbench).
        out = bytearray()
        out.append(int(PacketType.PUBLISH) | (flags & 0x0F))
        append_vbi(out, remaining_length)
        out.append((topic_len >> 8) & 0xFF)
        out.append(topic_len & 0xFF)
        out.extend(topic_bytes)
        if qos:
            assert self.mid is not None
            mid = self.mid
            out.append((mid >> 8) & 0xFF)
            out.append(mid & 0xFF)
        if props:
            out.extend(props)

        if payload_len >= SEGMENT_THRESHOLD and isinstance(payload, (bytes, bytearray)):
            return (bytes(out), bytes(payload))
        if payload_len:
            out.extend(payload)
        return bytes(out)

    @classmethod
    def decode(
        cls,
        flags: int,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PublishPacket:
        dup = bool(flags & 0x08)
        qos_raw = (flags >> 1) & 0x03
        if qos_raw == 3:
            raise MalformedPacketError("Invalid PUBLISH QoS 3")
        qos = QoS(qos_raw)
        if qos == QoS.AT_MOST_ONCE and dup:
            raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
        retain = bool(flags & 0x01)

        topic, pos = unpack_utf8(remaining, 0)
        mid: int | None = None
        if qos:
            mid, pos = unpack_u16(remaining, pos)
            require_nonzero_mid(mid, PUBLISH)
        properties: Properties | None = None
        if protocol == MQTTProtocolVersion.MQTTv5:
            properties, pos = decode_properties(remaining, pos, PUBLISH)
        # The remainder is the PUBLISH payload and may be empty.
        payload = remaining[pos:]
        return cls(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            dup=dup,
            mid=mid,
            properties=properties,
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

    def encode(self) -> bytes:
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


@dataclass(slots=True, frozen=True)
class Subscription:
    topic: str
    options: SubscribeOptions = SubscribeOptions()


@dataclass(slots=True, frozen=True)
class SubscribePacket:
    mid: int
    subscriptions: tuple[Subscription, ...]
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if not self.subscriptions:
            raise ProtocolError("SUBSCRIBE requires at least one filter")
        _require_outbound_mid(self.mid, SUBSCRIBE)
        body = bytearray()
        body.extend(pack_u16(self.mid))
        body.extend(_props_or_empty(self.properties, SUBSCRIBE, protocol))
        for sub in self.subscriptions:
            body.extend(pack_utf8(sub.topic))
            body.append(sub.options.encode_byte(protocol))
        return encode_frame(PacketType.SUBSCRIBE, 0x02, body)


@dataclass(slots=True, frozen=True)
class SubAckPacket:
    mid: int
    reason_codes: tuple[int, ...]
    properties: Properties | None = None

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> SubAckPacket:
        if len(remaining) < 3:
            raise MalformedPacketError("SUBACK too short")
        mid, pos = unpack_u16(remaining, 0)
        require_nonzero_mid(mid, SUBACK)
        properties: Properties | None = None
        if protocol == MQTTProtocolVersion.MQTTv5:
            properties, pos = decode_properties(remaining, pos, SUBACK)
            allowed = _SUBACK_V5_REASONS
        else:
            allowed = _SUBACK_V311_REASONS
        if pos >= len(remaining):
            raise MalformedPacketError("SUBACK missing reason codes")
        reason_codes = tuple(remaining[pos:])
        for reason in reason_codes:
            require_reason_code(reason, allowed, SUBACK)
        return cls(mid=mid, reason_codes=reason_codes, properties=properties)


@dataclass(slots=True, frozen=True)
class UnsubscribePacket:
    mid: int
    topics: tuple[str, ...]
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        if not self.topics:
            raise ProtocolError("UNSUBSCRIBE requires at least one filter")
        _require_outbound_mid(self.mid, UNSUBSCRIBE)
        body = bytearray()
        body.extend(pack_u16(self.mid))
        body.extend(_props_or_empty(self.properties, UNSUBSCRIBE, protocol))
        for topic in self.topics:
            body.extend(pack_utf8(topic))
        return encode_frame(PacketType.UNSUBSCRIBE, 0x02, body)


@dataclass(slots=True, frozen=True)
class UnsubAckPacket:
    mid: int
    reason_codes: tuple[int, ...] = ()
    properties: Properties | None = None

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> UnsubAckPacket:
        if len(remaining) < 2:
            raise MalformedPacketError("UNSUBACK too short")
        mid, pos = unpack_u16(remaining, 0)
        require_nonzero_mid(mid, UNSUBACK)
        if protocol == MQTTProtocolVersion.MQTTv5:
            properties, pos = decode_properties(remaining, pos, UNSUBACK)
            if pos >= len(remaining):
                raise MalformedPacketError("UNSUBACK missing reason codes")
            reason_codes = tuple(remaining[pos:])
            for reason in reason_codes:
                require_reason_code(reason, _UNSUBACK_V5_REASONS, UNSUBACK)
            return cls(mid=mid, reason_codes=reason_codes, properties=properties)
        require_end(pos, len(remaining), UNSUBACK)
        return cls(mid=mid)


def _decode_ack_with_reason(
    remaining: bytes,
    protocol: MQTTProtocolVersion,
    packet_name: str,
) -> tuple[int, int, Properties | None]:
    if len(remaining) < 2:
        raise MalformedPacketError(f"{packet_name} too short")
    mid, pos = unpack_u16(remaining, 0)
    require_nonzero_mid(mid, packet_name)
    reason = 0
    properties: Properties | None = None
    if protocol == MQTTProtocolVersion.MQTTv5:
        if pos < len(remaining):
            reason = remaining[pos]
            pos += 1
            if pos < len(remaining):
                properties, pos = decode_properties(remaining, pos, packet_name)
            else:
                properties = Properties()
        require_reason_code(reason, _ACK_REASONS[packet_name], packet_name)
        require_end(pos, len(remaining), packet_name)
    else:
        require_end(pos, len(remaining), packet_name)
    return mid, reason, properties


def _encode_ack_with_reason(
    packet_type: PacketType,
    flags: int,
    mid: int,
    reason_code: int,
    properties: Properties | None,
    packet_name: str,
    protocol: MQTTProtocolVersion,
) -> bytes:
    _require_outbound_mid(mid, packet_name)
    body = bytearray(pack_u16(mid))
    if protocol == MQTTProtocolVersion.MQTTv5:
        _require_outbound_reason(reason_code, _ACK_REASONS[packet_name], packet_name)
        if reason_code != 0 or (properties and properties.values):
            body.append(reason_code)
            body.extend(encode_properties(properties, packet_name))
    elif reason_code != 0 or (properties and properties.values):
        raise ProtocolError(f"{packet_name} reason/properties require MQTT 5")
    return encode_frame(packet_type, flags, body)


@dataclass(slots=True, frozen=True)
class PubAckPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBACK, 0, self.mid, self.reason_code, self.properties, PUBACK, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubAckPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBACK)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRecPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBREC, 0, self.mid, self.reason_code, self.properties, PUBREC, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRecPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBREC)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubRelPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBREL, 0x02, self.mid, self.reason_code, self.properties, PUBREL, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubRelPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBREL)
        return cls(mid=mid, reason_code=reason, properties=props)


@dataclass(slots=True, frozen=True)
class PubCompPacket:
    mid: int
    reason_code: int = 0
    properties: Properties | None = None

    def encode(self, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311) -> bytes:
        return _encode_ack_with_reason(
            PacketType.PUBCOMP, 0, self.mid, self.reason_code, self.properties, PUBCOMP, protocol
        )

    @classmethod
    def decode(
        cls,
        remaining: bytes,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> PubCompPacket:
        mid, reason, props = _decode_ack_with_reason(remaining, protocol, PUBCOMP)
        return cls(mid=mid, reason_code=reason, properties=props)


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
        _require_outbound_reason(reason_code, _DISCONNECT_V5_REASONS, DISCONNECT)
        if reason_code == 0 and not (properties and properties.values):
            return encode_frame(PacketType.DISCONNECT, 0, b"")
        body = bytearray()
        body.append(reason_code)
        body.extend(encode_properties(properties, DISCONNECT))
        return encode_frame(PacketType.DISCONNECT, 0, body)
    if reason_code != 0 or (properties and properties.values):
        raise ProtocolError("DISCONNECT reason/properties require MQTT 5")
    return encode_frame(PacketType.DISCONNECT, 0, b"")
