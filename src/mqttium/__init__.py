"""MQTTium — a reliable, async-native MQTT client for Python."""

from __future__ import annotations

from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import (
    BrokerDisconnectError,
    ConnectError,
    ConnectRefusedError,
    FlowControlError,
    MQTTError,
    MQTTTimeoutError,
    MalformedPacketError,
    MandatoryResponseTooLargeError,
    MessageDeliveryError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    PublishBatchError,
    PublishRejectedError,
    SessionDiscardedError,
    SessionReplayError,
    SubscribeError,
)

__all__ = [
    "BrokerDisconnectError",
    "ConnectError",
    "ConnectRefusedError",
    "ConnectionState",
    "FlowControlError",
    "MQTTError",
    "MQTTProtocolVersion",
    "MQTTTimeoutError",
    "MalformedPacketError",
    "MandatoryResponseTooLargeError",
    "MessageDeliveryError",
    "NotConnectedError",
    "PacketTooLargeError",
    "ProtocolError",
    "PublishBatchError",
    "PublishRejectedError",
    "QoS",
    "SessionDiscardedError",
    "SessionReplayError",
    "SubscribeError",
    "__version__",
]

__version__ = "1.0.0"
