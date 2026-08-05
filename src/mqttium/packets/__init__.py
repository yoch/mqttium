"""MQTT packet framing helpers and typed packet views.

One module per packet family; this package only re-exports them. Nothing here
imports ``mqttium.protocol``: packet validation lives in
:mod:`mqttium.codec.packet_validation`, which keeps the codec layer free of any
dependency on the state machine that consumes it.
"""

from __future__ import annotations

from mqttium.packets._common import encode_frame
from mqttium.packets.acks import (
    PubAckPacket,
    PubCompPacket,
    PubRecPacket,
    PubRelPacket,
)
from mqttium.packets.connect import ConnAckPacket, ConnectPacket
from mqttium.packets.control import (
    AuthPacket,
    DisconnectPacket,
    encode_disconnect,
    encode_pingreq,
    encode_pingresp,
)
from mqttium.packets.publish import PublishPacket
from mqttium.packets.subscription import (
    SubAckPacket,
    SubscribeOptions,
    SubscribePacket,
    Subscription,
    UnsubAckPacket,
    UnsubscribePacket,
)

__all__ = [
    "AuthPacket",
    "ConnAckPacket",
    "ConnectPacket",
    "DisconnectPacket",
    "PubAckPacket",
    "PubCompPacket",
    "PubRecPacket",
    "PubRelPacket",
    "PublishPacket",
    "SubAckPacket",
    "SubscribeOptions",
    "SubscribePacket",
    "Subscription",
    "UnsubAckPacket",
    "UnsubscribePacket",
    "encode_disconnect",
    "encode_frame",
    "encode_pingreq",
    "encode_pingresp",
]
