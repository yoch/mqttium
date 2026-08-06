"""Canonical import paths and stability-tier boundaries."""

from __future__ import annotations

import mqttium
import mqttium.api as api
from mqttium.api.async_client import AsyncClient, MessageDelivery, PublishBackpressure
from mqttium.api.models import (
    PublishBatchReceipt,
    PublishMessage,
    PublishReceipt,
    SubscribeResult,
    UnsubscribeResult,
)
from mqttium.api.stats import ClientStats
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
from mqttium.packets import AuthPacket, ConnAckPacket, SubscribeOptions
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Message, Properties


def test_root_exports_operational_errors_and_connection_state() -> None:
    expected = {
        "ConnectionState": mqttium.ConnectionState,
        "FlowControlError": FlowControlError,
        "MQTTError": MQTTError,
        "MQTTTimeoutError": MQTTTimeoutError,
        "MalformedPacketError": MalformedPacketError,
        "MessageDeliveryError": MessageDeliveryError,
        "NotConnectedError": NotConnectedError,
        "PacketTooLargeError": PacketTooLargeError,
        "ProtocolError": ProtocolError,
        "PublishBatchError": PublishBatchError,
        "SessionDiscardedError": SessionDiscardedError,
    }

    for name, value in expected.items():
        assert getattr(mqttium, name) is value
        assert name in mqttium.__all__


def test_api_exports_every_type_used_by_supported_signatures() -> None:
    expected = {
        "AsyncClient": AsyncClient,
        "AuthPacket": AuthPacket,
        "ClientStats": ClientStats,
        "ConnAckPacket": ConnAckPacket,
        "Message": Message,
        "MessageDelivery": MessageDelivery,
        "NegotiatedSettings": NegotiatedSettings,
        "Properties": Properties,
        "PublishBackpressure": PublishBackpressure,
        "PublishBatchReceipt": PublishBatchReceipt,
        "PublishMessage": PublishMessage,
        "PublishReceipt": PublishReceipt,
        "ReconnectPolicy": ReconnectPolicy,
        "SubscribeOptions": SubscribeOptions,
        "SubscribeResult": SubscribeResult,
        "UnsubscribeResult": UnsubscribeResult,
    }

    assert set(api.__all__) == set(expected)
    for name, value in expected.items():
        assert getattr(api, name) is value


def test_internal_pumps_are_not_promoted_to_supported_entry_points() -> None:
    for name in ("EffectPump", "WritePump", "InboundSession", "OutboundSession"):
        assert name not in mqttium.__all__
        assert name not in api.__all__
        assert not hasattr(api, name)
