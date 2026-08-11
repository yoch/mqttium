"""Shared typed models used across layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS


@dataclass(slots=True)
class Properties:
    """Minimal MQTT 5 property bag.

    Values that may repeat (user properties, subscription identifiers) are lists.
    Singletons are stored directly; packet encoders and decoders validate which
    properties are legal for each MQTT packet type.
    """

    values: dict[str, Any] = field(default_factory=dict)
    # Packet-name → last encoded table. Cleared on mutation through set/add.
    # compare=False so equality stays value-based; not part of the public surface.
    _encoded: dict[str, bytes] = field(default_factory=dict, repr=False, compare=False)

    def get(self, name: str, default: Any = None) -> Any:
        return self.values.get(name, default)

    def set(self, name: str, value: Any) -> None:
        self.values[name] = value
        self._encoded.clear()

    def add_user_property(self, key: str, value: str) -> None:
        items = self.values.setdefault("user_property", [])
        items.append((key, value))
        self._encoded.clear()

    def __bool__(self) -> bool:
        return bool(self.values)


@dataclass(slots=True, frozen=True)
class Message:
    topic: str
    payload: bytes
    qos: QoS = QoS.AT_MOST_ONCE
    retain: bool = False
    dup: bool = False
    mid: int | None = None
    properties: Properties | None = None


@dataclass(slots=True)
class OutboundMessage:
    mid: int
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    state: OutboundQoSState
    dup: bool = False
    properties: Properties | None = None
    encoded_publish: bytes | tuple[bytes, bytes] | None = None
    encoded_pubrel: bytes | None = None
    logical_size: int = 0
    # Ephemeral admit→launch cache of the MQTT 5 property table. Not persisted.
    property_wire: bytes | None = None


@dataclass(slots=True, frozen=True)
class OutboundMessageSummary:
    """Payload-free durable outbound metadata used for lazy replay queues."""

    mid: int
    topic: str
    payload_size: int
    qos: QoS
    retain: bool
    state: OutboundQoSState
    dup: bool = False
    properties: Properties | None = None
    logical_size: int = 0

    @classmethod
    def from_message(cls, message: OutboundMessage) -> OutboundMessageSummary:
        return cls(
            mid=message.mid,
            topic=message.topic,
            payload_size=len(message.payload),
            qos=message.qos,
            retain=message.retain,
            state=message.state,
            dup=message.dup,
            properties=message.properties,
            logical_size=message.logical_size,
        )


@dataclass(slots=True, frozen=True)
class OutboundRecordMeta:
    """Everything a durable outbound transition has to return.

    Deliberately payload-free: a PUBACK for a multi-megabyte publication must
    settle its record without the store ever reading the BLOB back.
    """

    mid: int
    state: OutboundQoSState
    logical_size: int


@dataclass(slots=True, frozen=True)
class InboundRecordMeta:
    """Payload-free inbound record metadata (see :class:`OutboundRecordMeta`)."""

    mid: int
    state: InboundQoSState
    user_acked: bool
    logical_size: int = 0


@dataclass(slots=True)
class InboundMessage:
    mid: int
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    state: InboundQoSState
    delivered: bool = False
    properties: Properties | None = None
    user_acked: bool = False  # manual_ack: app called ack() before PUBREL
    logical_size: int = 0
