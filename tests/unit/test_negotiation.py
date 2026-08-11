"""Negotiation, keepalive helpers, reconnect policy, QoS fail tests."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion
from mqttium.errors import PacketTooLargeError, ProtocolError
from mqttium.packets import encode_frame
from mqttium.enums import PacketType
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.reconnect import ReconnectPolicy, is_terminal_connack
from mqttium.types import Properties
import pytest


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    dec = IncrementalDecoder()
    dec.feed(wire)
    engine.handle_raw(dec.next_packet())  # type: ignore[arg-type]


def _connack_v5(props: Properties | None = None, session_present: bool = False) -> bytes:
    body = bytearray()
    body.append(0x01 if session_present else 0x00)
    body.append(0x00)
    body.extend(encode_properties(props, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def test_negotiated_settings_applied() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            local_receive_maximum=1000,
            keepalive=60,
        )
    )
    engine.begin_connect()
    props = Properties()
    props.set("receive_maximum", 5)
    props.set("maximum_qos", 1)
    props.set("retain_available", 0)
    props.set("maximum_packet_size", 200)
    props.set("topic_alias_maximum", 3)
    props.set("server_keep_alive", 25)
    props.set("assigned_client_identifier", "broker-id")
    _feed(engine, _connack_v5(props))
    engine.take_effects()

    n = engine.negotiated
    assert n.receive_maximum == 5
    assert engine.flow.limit == 5
    assert n.maximum_qos == 1
    assert n.retain_available is False
    assert n.maximum_packet_size == 200
    assert n.topic_alias_maximum == 3
    assert n.server_keep_alive == 25
    assert n.effective_client_id("c") == "broker-id"

    with pytest.raises(ProtocolError, match="maximum_qos"):
        engine.queue_publish("t", b"x", qos=2)
    with pytest.raises(ProtocolError, match="retain"):
        engine.queue_publish("t", b"x", qos=0, retain=True)


def test_maximum_packet_size_enforced() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    props = Properties()
    props.set("maximum_packet_size", 20)
    _feed(engine, _connack_v5(props))
    engine.take_effects()
    with pytest.raises(PacketTooLargeError):
        engine.queue_publish("t", b"x" * 100, qos=0)


def test_maximum_packet_size_applies_to_disconnect_before_state_change() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5))
    engine.begin_connect()
    connack = Properties()
    connack.set("maximum_packet_size", 10)
    _feed(engine, _connack_v5(connack))
    engine.take_effects()
    disconnect = Properties()
    disconnect.set("reason_string", "x" * 50)

    with pytest.raises(PacketTooLargeError):
        engine.begin_disconnect(properties=disconnect)

    assert engine.state is ConnectionState.CONNECTED


def test_pubrec_failure_emits_publish_failed() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c"))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    handle = engine.queue_publish("t", b"x", qos=2)
    engine.take_effects()
    mid = handle.mid
    assert mid is not None

    # Craft MQTT 3.1.1 PUBREC — reason codes only on v5, so switch protocol path:
    engine.config.protocol = MQTTProtocolVersion.MQTTv5
    body = bytes([mid >> 8, mid & 0xFF, 0x80, 0x00])  # reason 0x80 + empty props
    _feed(engine, encode_frame(PacketType.PUBREC, 0, body))
    effects = engine.take_effects()
    failed = [e for e in effects if e.kind is EffectKind.PUBLISH_FAILED]
    assert len(failed) == 1
    assert failed[0].data.mid == mid
    assert engine.store.get_out(mid) is None


def test_reconnect_policy_terminal_codes() -> None:
    assert is_terminal_connack(5, MQTTProtocolVersion.MQTTv311)
    assert not is_terminal_connack(3, MQTTProtocolVersion.MQTTv311)
    assert is_terminal_connack(0x87, MQTTProtocolVersion.MQTTv5)
    assert not is_terminal_connack(0x89, MQTTProtocolVersion.MQTTv5)

    policy = ReconnectPolicy(enabled=True, initial_delay=1.0, max_delay=10.0, multiplier=2.0)
    d1 = policy.next_delay()
    assert 0.5 <= d1 <= 1.0
    d2 = policy.next_delay()
    assert 1.0 <= d2 <= 2.0
    policy.reset()
    assert policy.attempt == 0


def test_negotiated_from_empty_props_defaults() -> None:
    n = NegotiatedSettings.from_connack(None, requested_keepalive=60)
    assert n.receive_maximum == 65535
    assert n.maximum_qos == 2
    assert n.retain_available is True
    assert n.topic_alias_maximum == 0
