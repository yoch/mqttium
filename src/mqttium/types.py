"""Shared typed models used across layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS


def _freeze_property_signature(value: Any) -> Any:
    """Snapshot mutable property values before using them as a cache signature."""
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, list):
        return tuple(_freeze_property_signature(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_property_signature(item) for item in value)
    return value


@dataclass(slots=True)
class Properties:
    """Minimal MQTT 5 property bag.

    Values that may repeat (user properties, subscription identifiers) are lists.
    Singletons are stored directly; packet encoders and decoders validate which
    properties are legal for each MQTT packet type.
    """

    values: dict[str, Any] = field(default_factory=dict)
    # packet context -> (structural signature, encoded property table).
    # The signature snapshots mutable values, so direct/in-place mutations remain safe.
    # Created on first encode: every decoded inbound packet builds a Properties
    # and never encodes it, so allocating the cache eagerly would double the
    # dict allocations on the ingress path.
    _encoded: dict[str, tuple[tuple[tuple[str, Any], ...], bytes]] | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def _signature(self) -> tuple[tuple[str, Any], ...]:
        return tuple(
            (name, _freeze_property_signature(value)) for name, value in self.values.items()
        )

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
