"""Project contract for canonical imports and stability-tier boundaries."""

from __future__ import annotations

import dataclasses
import inspect
from pathlib import Path

import pytest

import mqttium
import mqttium.api as api
import mqttium.protocol as protocol
from mqttium.api.async_client import AsyncClient, MessageDelivery
from mqttium.api.models import (
    PublishBatchReceipt,
    PublishMessage,
    PublishReceipt,
    SubscribeResult,
    UnsubscribeResult,
)
from mqttium.api.stats import (
    ClientStats,
    DecoderStats,
    DeliveryStats,
    InboundStats,
    OutboundStats,
    ReceiptStats,
    TransportStats,
    WriterStats,
)
from mqttium.errors import (
    BrokerDisconnectError,
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
    SessionDiscardedError,
)
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import AuthPacket, ConnAckPacket, SubscribeOptions
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Message, Properties


STABLE_ROOT_EXPORTS = {
    "BrokerDisconnectError": BrokerDisconnectError,
    "ConnectionState": ConnectionState,
    "FlowControlError": FlowControlError,
    "MQTTError": MQTTError,
    "MQTTProtocolVersion": MQTTProtocolVersion,
    "MQTTTimeoutError": MQTTTimeoutError,
    "MalformedPacketError": MalformedPacketError,
    "MandatoryResponseTooLargeError": MandatoryResponseTooLargeError,
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
    "PublishBatchReceipt": PublishBatchReceipt,
    "PublishMessage": PublishMessage,
    "PublishReceipt": PublishReceipt,
    "ReconnectPolicy": ReconnectPolicy,
    "SubscribeOptions": SubscribeOptions,
    "SubscribeResult": SubscribeResult,
    "UnsubscribeResult": UnsubscribeResult,
}


def test_root_exports_operational_errors_and_connection_state() -> None:
    assert set(mqttium.__all__) == {*STABLE_ROOT_EXPORTS, "__version__"}
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


def test_async_client_constructor_keywords_and_defaults() -> None:
    expected_defaults = {
        "client_id": "",
        "protocol": MQTTProtocolVersion.MQTTv311,
        "clean_start": True,
        "keepalive": 60,
        "username": None,
        "password": None,
        "connect_properties": None,
        "will": None,
        "will_properties": None,
        "maximum_packet_size": None,
        "topic_alias_maximum": 0,
        "max_inbound_inflight": 100,
        "max_inbound_inflight_bytes": 64 * 1024 * 1024,
        "max_outbound_inflight": None,
        "max_unacknowledged_messages": 10_000,
        "max_unacknowledged_bytes": 64 * 1024 * 1024,
        "max_write_queue_messages": 10_000,
        "max_write_queue_bytes": 1 * 1024 * 1024,
        "message_delivery": "iterator",
        "manual_ack": False,
        "max_iterator_messages": 65_536,
        "max_iterator_bytes": 64 * 1024 * 1024,
        "iterator_admission_timeout": None,
        "store": None,
        "reconnect": None,
        "connect_timeout": 30.0,
        "ping_timeout": None,
        "subscribe_timeout": 30.0,
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


def test_reconnect_policy_describes_only_the_retry_progression() -> None:
    expected_defaults = {
        "initial_delay": 1.0,
        "multiplier": 2.0,
        "max_delay": 60.0,
        "max_retries": None,
        "stable_after": 30.0,
    }
    parameters = inspect.signature(ReconnectPolicy).parameters
    assert {name: parameter.default for name, parameter in parameters.items()} == expected_defaults


def test_client_stats_fields_follow_the_constructor_vocabulary() -> None:
    def names(cls: type) -> tuple[str, ...]:
        return tuple(field.name for field in dataclasses.fields(cls))

    assert names(ClientStats) == (
        "state",
        "connection_epoch",
        "reconnect_attempt",
        "outbound",
        "inbound",
        "writer",
        "decoder",
        "delivery",
        "receipts",
        "transport",
    )
    assert names(OutboundStats) == (
        "unacknowledged_messages",
        "unacknowledged_bytes",
        "unacknowledged_high_water_messages",
        "unacknowledged_high_water_bytes",
        "awaiting_slot",
        "inflight",
        "inflight_limit",
        "packet_ids_in_use",
    )
    assert names(InboundStats) == (
        "inflight",
        "inflight_limit",
        "inflight_bytes",
        "inflight_high_water_bytes",
        "inflight_byte_limit",
        "topic_aliases",
        "replay_pending",
    )
    assert names(WriterStats) == (
        "queued_messages",
        "queued_bytes",
        "high_water_messages",
        "high_water_bytes",
        "max_messages",
        "max_bytes",
        "waiters",
        "last_outbound",
    )
    assert names(DecoderStats) == ("buffered_bytes", "high_water_bytes", "max_packet_size")
    assert names(DeliveryStats) == (
        "iterator_queued",
        "iterator_limit",
        "iterator_bytes",
        "iterator_high_water_bytes",
        "iterator_byte_limit",
        "callback_invocations",
        "waiters",
    )
    assert names(ReceiptStats) == (
        "publish",
        "publish_batches",
        "subscribe",
        "unsubscribe",
        "publish_waiters",
    )
    assert names(TransportStats) == (
        "kind",
        "closing",
        "pending_write_bytes",
        "buffered_read_bytes",
    )


def test_constructor_refuses_configuration_without_effect() -> None:
    with pytest.raises(ValueError, match="iterator delivery only"):
        AsyncClient("c", message_delivery="callback", max_iterator_messages=10)
    with pytest.raises(ValueError, match="iterator delivery only"):
        AsyncClient("c", message_delivery="callback", iterator_admission_timeout=1.0)
    AsyncClient("c", message_delivery="callback")
    for option in (
        {"connect_properties": Properties({"session_expiry_interval": 10})},
        {"will_properties": Properties({"message_expiry_interval": 10}), "will": Message("w", b"")},
        {"topic_alias_maximum": 5},
        {"auth_handler": lambda packet: None},
    ):
        with pytest.raises(ProtocolError, match="MQTT 5"):
            AsyncClient("c", **option)
        AsyncClient("c", protocol=MQTTProtocolVersion.MQTTv5, **option)
    with pytest.raises(ValueError, match="connect_timeout"):
        AsyncClient("c", connect_timeout=0)


def test_async_client_stable_method_parameter_contract() -> None:
    expected = {
        "connect": ("self", "host", "port", "ssl", "timeout"),
        "connect_unix": ("self", "path", "timeout"),
        "connect_ws": ("self", "url", "ssl", "extra_headers", "timeout"),
        "disconnect": ("self", "reason_code"),
        "publish": ("self", "topic", "payload", "qos", "retain", "properties"),
        "publish_nowait": ("self", "topic", "payload", "qos", "retain", "properties"),
        "publish_many": (
            "self",
            "messages",
            "max_failure_details",
        ),
        "subscribe": ("self", "topics", "qos", "properties", "timeout"),
        "unsubscribe": ("self", "topics", "timeout"),
        "messages": ("self",),
        "ack": ("self", "message"),
        "auth": ("self", "reason_code", "properties"),
        "message_callback_add": ("self", "topic_filter", "callback"),
        "message_callback_remove": ("self", "topic_filter"),
        "stats": ("self",),
    }

    for name, parameter_names in expected.items():
        assert tuple(inspect.signature(getattr(AsyncClient, name)).parameters) == parameter_names


def test_internal_pumps_are_not_promoted_to_supported_entry_points() -> None:
    for name in (
        "EffectPump",
        "DeliveryLane",
        "LifecycleHooks",
        "WritePump",
        "InboundSession",
        "OutboundSession",
    ):
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


def test_retired_entry_points_are_absent() -> None:
    import importlib.util

    assert importlib.util.find_spec("mqttium.compat") is None
    assert importlib.util.find_spec("mqttium.helpers") is None
    assert not hasattr(mqttium, "PacketType")
    assert not hasattr(api, "PublishBackpressure")
    assert not hasattr(AsyncClient, "set_auth_handler")
    assert not hasattr(AsyncClient(), "on_publish")
