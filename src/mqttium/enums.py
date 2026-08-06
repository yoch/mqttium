"""Public enums for mqttium."""

from __future__ import annotations

from enum import IntEnum

from mqttium.errors import MalformedPacketError


class MQTTProtocolVersion(IntEnum):
    MQTTv31 = 3
    MQTTv311 = 4
    MQTTv5 = 5


class PacketType(IntEnum):
    CONNECT = 0x10
    CONNACK = 0x20
    PUBLISH = 0x30
    PUBACK = 0x40
    PUBREC = 0x50
    PUBREL = 0x60
    PUBCOMP = 0x70
    SUBSCRIBE = 0x80
    SUBACK = 0x90
    UNSUBSCRIBE = 0xA0
    UNSUBACK = 0xB0
    PINGREQ = 0xC0
    PINGRESP = 0xD0
    DISCONNECT = 0xE0
    AUTH = 0xF0

    @classmethod
    def from_byte(cls, byte: int) -> PacketType:
        try:
            return cls._value2member_map_[byte & 0xF0]
        except KeyError as exc:
            raise MalformedPacketError(f"Unknown MQTT packet type byte 0x{byte:02x}") from exc


class QoS(IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class OutboundQoSState(IntEnum):
    """Outbound QoS message lifecycle."""

    QUEUED = 1
    WAIT_PUBACK = 2
    WAIT_PUBREC = 3
    WAIT_PUBCOMP = 4
    DONE = 5


class InboundQoSState(IntEnum):
    """Inbound QoS lifecycle (QoS 1 manual ACK + QoS 2)."""

    WAIT_PUBREL = 1
    DONE = 2
    WAIT_PUBACK = 3  # QoS 1 + manual_ack: awaiting client.ack()
    WAIT_USER_ACK = 4  # QoS 2 + manual_ack: PUBREL seen, awaiting client.ack()


class ConnectionState(IntEnum):
    NEW = 1
    CONNECTING = 2
    CONNECTED = 3
    DISCONNECTING = 4
    DISCONNECTED = 5
    RECONNECTING = 6
