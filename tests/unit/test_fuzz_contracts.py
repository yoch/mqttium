"""Fuzzer exception contracts and exact engine invariants."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "fuzz"))

import fuzz as fuzzmod  # noqa: E402
from mqttium.enums import (  # noqa: E402
    ConnectionState,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.errors import MQTTError  # noqa: E402
from mqttium.protocol.engine import ProtocolEngine  # noqa: E402
from mqttium.types import OutboundMessage  # noqa: E402


def test_reserved_packet_type_raises_public_mqtt_error() -> None:
    with pytest.raises(MQTTError):
        PacketType.from_byte(0x0D)


def test_low_level_parser_exceptions_are_not_allowed() -> None:
    assert fuzzmod.ALLOWED == (MQTTError, ConnectionError)
    assert IndexError not in fuzzmod.ALLOWED


def test_exact_mid_invariant_detects_unreserved_store_record() -> None:
    engine = ProtocolEngine()
    engine.store.put_out(
        OutboundMessage(
            mid=7,
            topic="fuzz/mid",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.QUEUED,
        )
    )
    with pytest.raises(AssertionError, match="packet-id mismatch"):
        fuzzmod._check_engine_invariants(engine)


def test_exact_flow_invariant_detects_unaccounted_transaction() -> None:
    engine = ProtocolEngine()
    engine.state = ConnectionState.CONNECTED
    message = OutboundMessage(
        mid=9,
        topic="fuzz/flow",
        payload=b"x",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBACK,
    )
    engine.store.put_out(message)
    engine.packet_ids.reserve(message.mid)
    with pytest.raises(AssertionError, match="flow mismatch"):
        fuzzmod._check_engine_invariants(engine)
