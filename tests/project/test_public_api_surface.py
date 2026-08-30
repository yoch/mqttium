"""Project contract for canonical imports and stability-tier boundaries."""

from __future__ import annotations

import inspect
from pathlib import Path
from types import ModuleType

import pytest

import mqttium
import mqttium.api as api
import mqttium.helpers as helpers
import mqttium.protocol as protocol
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
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import AuthPacket, ConnAckPacket, SubscribeOptions
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Message, Properties


STABLE_ROOT_EXPORTS = {
    "ConnectionState": ConnectionState,
    "FlowControlError": FlowControlError,
    "MQTTError": MQTTError,
    "MQTTProtocolVersion": MQTTProtocolVersion,
    "MQTTTimeoutError": MQTTTimeoutError,
    "MalformedPacketError": MalformedPacketError,
    "MessageDeliveryError": MessageDeliveryError,
    "NotConnectedError": NotConnectedError,
    "PacketTooLargeError": PacketTooLargeError,
    "ProtocolError": ProtocolError,
    "PublishBatchError": PublishBatchError,
    "QoS": QoS,
    "SessionDiscardedError": SessionDiscardedError,
}

STABLE_API_EXPORTS = {
    "AsyncClient": AsyncClient,
    "AuthPacket": AuthPacket,
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


def test_root_exports_operational_errors_and_connection_state() -> None:
    assert set(mqttium.__all__) == {*STABLE_ROOT_EXPORTS, "PacketType", "__version__"}
    for name, value in STABLE_ROOT_EXPORTS.items():
        assert getattr(mqttium, name) is value
    assert isinstance(mqttium.__version__, str)


def test_api_exports_every_type_used_by_supported_signatures() -> None:
    expected = {
        **STABLE_API_EXPORTS,
        "ClientStats": ClientStats,
    }

    assert set(api.__all__) == set(expected)
    for name, value in expected.items():
        assert getattr(api, name) is value


def test_helpers_export_exact_stable_surface() -> None:
    assert helpers.__all__ == ["publish", "subscribe"]
    assert isinstance(helpers.publish, ModuleType)
    assert isinstance(helpers.subscribe, ModuleType)
    assert callable(helpers.publish.single)
    assert callable(helpers.publish.multiple)
    assert callable(helpers.subscribe.simple)
    assert callable(helpers.subscribe.callback)


def test_explicitly_retained_alpha_packet_type_import() -> None:
    assert mqttium.PacketType is PacketType


def test_async_client_constructor_keywords_and_defaults() -> None:
    expected_defaults = {
        "client_id": "",
        "protocol": MQTTProtocolVersion.MQTTv311,
        "clean_start": True,
        "keepalive": 60,
        "username": None,
        "password": None,
        "local_receive_maximum": 100,
        "max_outbound_inflight": None,
        "max_pending_outbound_messages": 10_000,
        "max_pending_outbound_bytes": 64 * 1024 * 1024,
        "max_pending_inbound_bytes": 64 * 1024 * 1024,
        "publish_backpressure": "wait",
        "connect_properties": None,
        "will": None,
        "will_properties": None,
        "maximum_packet_size": None,
        "topic_alias_maximum": 0,
        "reconnect": None,
        "ping_timeout": None,
        "ack_timeout": 30.0,
        "max_outbound_bytes": 1 * 1024 * 1024,
        "max_outbound_messages": 10_000,
        "max_ingress_batch_bytes": 1 * 1024 * 1024,
        "max_pending_messages": 65_536,
        "max_pending_callbacks": 1_024,
        "max_pending_delivery_bytes": 64 * 1024 * 1024,
        "delivery_timeout": 1.0,
        "callback_shutdown_timeout": 5.0,
        "message_delivery": "auto",
        "manual_ack": False,
        "store": None,
        "auth_handler": None,
        "auth_timeout": 10.0,
    }
    parameters = inspect.signature(AsyncClient).parameters

    assert tuple(parameters) == tuple(expected_defaults)
    assert parameters["client_id"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in parameters.items()
        if name != "client_id"
    )
    assert {name: parameter.default for name, parameter in parameters.items()} == expected_defaults


def test_async_client_stable_method_parameter_contract() -> None:
    expected = {
        "connect": ("self", "host", "port", "ssl", "timeout"),
        "connect_unix": ("self", "path", "timeout"),
        "connect_ws": ("self", "url", "ssl", "extra_headers", "timeout"),
        "disconnect": ("self", "reason_code"),
        "publish": ("self", "topic", "payload", "qos", "retain", "properties", "nowait"),
        "publish_nowait": ("self", "topic", "payload", "qos", "retain", "properties"),
        "publish_many": (
            "self",
            "messages",
            "chunk_size",
            "nowait",
            "max_failure_details",
            "failure_sink",
        ),
        "subscribe": ("self", "topics", "qos", "properties", "timeout"),
        "unsubscribe": ("self", "topics", "timeout"),
        "messages": ("self",),
        "ack": ("self", "message"),
        "auth": ("self", "reason_code", "properties"),
        "set_auth_handler": ("self", "handler"),
        "message_callback_add": ("self", "topic_filter", "callback"),
        "message_callback_remove": ("self", "topic_filter"),
        "stats": ("self",),
    }

    for name, parameter_names in expected.items():
        assert tuple(inspect.signature(getattr(AsyncClient, name)).parameters) == parameter_names


def test_internal_pumps_are_not_promoted_to_supported_entry_points() -> None:
    for name in ("EffectPump", "WritePump", "InboundSession", "OutboundSession"):
        assert name not in mqttium.__all__
        assert name not in api.__all__
        assert not hasattr(api, name)


def test_protocol_lazy_exports_are_complete_and_discoverable() -> None:
    assert set(protocol.__all__) <= set(dir(protocol))
    for name in protocol.__all__:
        value = getattr(protocol, name)
        assert value.__module__.startswith("mqttium.protocol")

    with pytest.raises(AttributeError, match="has no attribute"):
        protocol.__getattr__("NotAProtocolExport")


def test_protocol_and_dispatch_do_not_import_compat() -> None:
    """Native layers stay independent of the Paho façade and API routing."""
    root = Path(__file__).resolve().parents[2] / "src" / "mqttium"
    offenders: list[str] = []
    for directory in ("protocol", "dispatch"):
        for path in (root / directory).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            relative = str(path.relative_to(root))
            if "mqttium.compat" in text:
                offenders.append(relative)
            if "mqttium.api" in text:
                offenders.append(relative)
    engine = (root / "protocol" / "engine.py").read_text(encoding="utf-8")
    if "mqttium.dispatch" in engine:
        offenders.append("protocol/engine.py")
    assert not offenders
