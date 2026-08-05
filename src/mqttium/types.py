"""Shared typed models used across layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS


@dataclass(slots=True)
class Properties:
    """Minimal MQTT 5 property bag.

    Values that may repeat (user properties, subscription identifiers) are lists.
    Singletons are stored directly. Full validation by packet type lands in phase 1.
    """

    values: dict[str, Any] = field(default_factory=dict)

    def get(self, name: str, default: Any = None) -> Any:
        return self.values.get(name, default)

    def set(self, name: str, value: Any) -> None:
        self.values[name] = value

    def add_user_property(self, key: str, value: str) -> None:
        items = self.values.setdefault("user_property", [])
        items.append((key, value))

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
    # Internal delivery accounting. Public Message instances remain frozen;
    # AsyncClient mutates only these private slots with object.__setattr__.
    _delivery_logical_bytes: int = field(default=0, init=False, repr=False, compare=False)
    _delivery_references: int = field(default=0, init=False, repr=False, compare=False)


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
