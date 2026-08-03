"""Compatibilité Paho — phase 3 (VERSION2).

Politique détaillée : ``docs/COMPAT.md``.
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
