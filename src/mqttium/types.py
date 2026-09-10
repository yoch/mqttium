"""Shared typed models used across layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from collections.abc import Mapping
from types import MappingProxyType

from mqttium.errors import ProtocolError

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS


def _owned_payload(payload: bytes | str) -> bytes:
    if isinstance(payload, str):
        return payload.encode("utf-8")
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, (bytearray, memoryview)):
        return bytes(payload)
    raise TypeError("payload must be bytes or str")


def _freeze_property_value(value: Any) -> Any:
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_property_value(item) for item in value)
    if value is None or isinstance(value, (str, bytes, int, float)):
        return value
    raise ProtocolError(f"Unsupported property value type: {type(value).__name__}")


@dataclass(slots=True, frozen=True)
class Properties:
    """Owned, immutable MQTT 5 properties.

    Construct from a mapping. Repeated values become tuples; user properties
    are ordered tuples of string pairs. Packet codecs validate context and
    value constraints before admitting an operation.
    """

    values: Mapping[str, Any] = field(default_factory=dict)
    _encoded: dict[str, bytes] | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        values = {}
        for name, value in self.values.items():
            if not isinstance(name, str):
                raise ProtocolError("Property names must be strings")
            frozen = _freeze_property_value(value)
            if name == "user_property" and isinstance(frozen, tuple):
                if len(frozen) == 2 and all(isinstance(item, str) for item in frozen):
                    frozen = (frozen,)
            values[name] = frozen
        object.__setattr__(self, "values", MappingProxyType(values))

    def get(self, name: str, default: Any = None) -> Any:
        return self.values.get(name, default)

    def __bool__(self) -> bool:
        return bool(self.values)


@dataclass(slots=True, frozen=True)
class Message:
    """Immutable application message delivered by :class:`AsyncClient`.

    Attributes:
        topic: Decoded MQTT topic name.
        payload: Owned payload bytes.
        qos: Delivery QoS.
        retain: Whether the broker marked the delivery as retained.
        dup: Whether the MQTT PUBLISH carried the DUP flag.
        mid: Packet identifier for QoS 1/2, otherwise ``None``.
        properties: MQTT 5 PUBLISH properties, otherwise ``None``.
    """

    topic: str
    payload: bytes
    qos: QoS = QoS.AT_MOST_ONCE
    retain: bool = False
    dup: bool = False
    mid: int | None = None
    properties: Properties | None = None
    _ack_token: object | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _owned_payload(self.payload))


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
    delivered: bool = False
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
