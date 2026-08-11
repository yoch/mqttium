"""Paho VERSION2 compatibility.

See ``docs/COMPAT.md`` for the detailed policy.
"""

from mqttium.compat.paho import (
    CallbackAPIVersion,
    Client,
    DisconnectFlags,
    MQTTMessage,
    MQTTMessageInfo,
)

__all__ = [
    "CallbackAPIVersion",
    "Client",
    "DisconnectFlags",
    "MQTTMessage",
    "MQTTMessageInfo",
]
