"""mqttium — async-native MQTT client (reliable async-native MQTT client).

Designed from scratch using lessons from Eclipse Paho MQTT Python performance
work and the gmqtt asyncio architecture, without inheriting either codebase's
protocol-engine debt.
"""

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
