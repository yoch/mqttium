"""MQTTium — a reliable, async-native MQTT client for Python."""

from __future__ import annotations

from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import (
    BrokerDisconnectError,
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
    "BrokerDisconnectError",
    "ConnectionState",
    "FlowControlError",
    "MQTTError",
    "MQTTProtocolVersion",
    "MQTTTimeoutError",
    "MalformedPacketError",
    "MessageDeliveryError",
    "NotConnectedError",
    "PacketTooLargeError",
    "ProtocolError",
    "PublishBatchError",
    "QoS",
    "SessionDiscardedError",
    "__version__",
]

__version__ = "1.0.0rc13"
