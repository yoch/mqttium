"""Values the protocol engine hands back to its runtime adapter.

An effect is the engine's only output channel: it never touches a socket, a
timer or a callback itself. AsyncClient turns these into writes, futures,
receipts and messages.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from mqttium.enums import QoS
from mqttium.types import Properties


class EffectKind(Enum):
    SEND = auto()
    MESSAGE = auto()
    CONNACK = auto()
    PUBLISH_COMPLETE = auto()
    PUBLISH_FAILED = auto()
    SUBACK = auto()
    UNSUBACK = auto()
    DISCONNECTED = auto()
    PROTOCOL_ERROR = auto()
    PINGRESP = auto()
    AUTH = auto()


@dataclass(slots=True)
class EngineEffect:
    kind: EffectKind
    data: Any = None


@dataclass(slots=True)
class PublishHandle:
    mid: int | None
    qos: QoS


@dataclass(slots=True)
class PublishFailure:
    mid: int
    reason: BaseException


@dataclass(slots=True)
class DisconnectInfo:
    """Broker or local disconnect signal carried on EffectKind.DISCONNECTED."""

    reason_code: int = 0
    properties: Properties | None = None
    from_broker: bool = False
