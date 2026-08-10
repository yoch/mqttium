"""MQTTium — a reliable, async-native MQTT client for Python."""

from __future__ import annotations

from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import (
    FlowControlError,
    MQTTError,
    MQTTTimeoutError,
    MalformedPacketError,
    MessageDeliveryError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    PublishBatchError,
    SessionDiscardedError,
)

__all__ = [
    "ConnectionState",
    "FlowControlError",
    "MQTTError",
    "MQTTProtocolVersion",
    "MQTTTimeoutError",
    "MalformedPacketError",
    "MessageDeliveryError",
    "NotConnectedError",
    "PacketTooLargeError",
    "PacketType",
    "ProtocolError",
    "PublishBatchError",
    "QoS",
    "SessionDiscardedError",
    "__version__",
]

__version__ = "0.2.0b4"
