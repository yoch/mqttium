"""Paho VERSION2 compatibility.

See ``docs/paho-compatibility.md`` for the detailed policy.
"""

from mqttium.compat.paho import (
    CallbackAPIVersion,
    Client,
    ConnectFlags,
    DisconnectFlags,
    MQTTMessage,
    MQTTMessageInfo,
)

__all__ = [
    "CallbackAPIVersion",
    "Client",
    "ConnectFlags",
    "DisconnectFlags",
    "MQTTMessage",
    "MQTTMessageInfo",
]
