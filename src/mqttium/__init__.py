"""MQTTium — a reliable, async-native MQTT client for Python."""

from __future__ import annotations

from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError, MalformedPacketError, ProtocolError

__all__ = [
    "MQTTError",
    "MQTTProtocolVersion",
    "MalformedPacketError",
    "PacketType",
    "ProtocolError",
    "QoS",
    "__version__",
]

__version__ = "0.1.0a1"
