"""Session Present validation against durable local state (#239)."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage
from mqttium.types import Properties


def _connack(*, present: bool, protocol: MQTTProtocolVersion) -> bytes:
    body = bytes((1 if present else 0, 0))
    if protocol is MQTTProtocolVersion.MQTTv5:
        body += b"\x00"
    return encode_frame(PacketType.CONNACK, 0, body)


def _feed(engine: ProtocolEngine, wire: bytes) -> list:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    return engine.take_effects()


def _engine(store, protocol=MQTTProtocolVersion.MQTTv5) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="bug239", protocol=protocol, clean_start=False), store
    )
    engine.begin_connect()
    return engine


def test_mqtt5_session_present_rejected_with_empty_client_session() -> None:
    engine = _engine(MemoryInflightStore(), MQTTProtocolVersion.MQTTv5)
    effects = _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_known_empty_durable_session_accepts_session_present() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="known-durable",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=True,
            connect_properties=Properties({"session_expiry_interval": 3600}),
        )
    )
    engine.begin_connect()
    _feed(engine, _connack(present=False, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine._prefer_session_resume is True
    engine.notify_transport_closed()
    engine.take_effects()

    engine.begin_connect()
    effects = _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))

    assert engine.state is ConnectionState.CONNECTED
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_mqtt311_session_present_with_empty_client_state_remains_accepted() -> None:
    engine = _engine(MemoryInflightStore(), MQTTProtocolVersion.MQTTv311)
    effects = _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv311))
    assert engine.state is ConnectionState.CONNECTED
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


@pytest.mark.parametrize(
    "state",
    [
        OutboundQoSState.WAIT_PUBACK,
        OutboundQoSState.WAIT_PUBREC,
        OutboundQoSState.WAIT_PUBCOMP,
    ],
)
def test_session_present_accepted_with_incomplete_outbound_exchange(state) -> None:
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="out",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=state,
        )
    )
    engine = _engine(store)
    _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.CONNECTED


def test_queued_but_never_sent_outbound_is_not_client_session_state() -> None:
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="out",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.QUEUED,
        )
    )
    engine = _engine(store)
    effects = _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert store.get_out(1) is not None


@pytest.mark.parametrize("state", [InboundQoSState.WAIT_PUBREL, InboundQoSState.WAIT_USER_ACK])
def test_session_present_accepted_with_incomplete_inbound_qos2(state) -> None:
    store = MemoryInflightStore()
    store.put_in(
        InboundMessage(
            mid=2,
            topic="in",
            payload=b"x",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=state,
        )
    )
    engine = _engine(store)
    _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.CONNECTED


def test_manual_qos1_inbound_record_is_not_client_session_state() -> None:
    store = MemoryInflightStore()
    store.put_in(
        InboundMessage(
            mid=2,
            topic="in",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBACK,
        )
    )
    engine = _engine(store)
    effects = _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_session_present_zero_still_discards_stale_state() -> None:
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="out",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    engine = _engine(store)
    _feed(engine, _connack(present=False, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.CONNECTED
    assert store.get_out(1) is None


class _NoPayloadHydrationStore(SqliteInflightStore):
    def out_items(self):
        raise AssertionError("Session State detection must not hydrate outbound payloads")

    def in_items(self):
        raise AssertionError("Session State detection must not hydrate inbound payloads")


def test_sqlite_outbound_session_state_uses_summary_metadata(tmp_path: Path) -> None:
    store = _NoPayloadHydrationStore(tmp_path / "out.db")
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="out",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
            logical_size=32,
        )
    )
    engine = _engine(store)
    _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.CONNECTED
    store.close()


def test_sqlite_inbound_session_state_uses_index_metadata(tmp_path: Path) -> None:
    store = _NoPayloadHydrationStore(tmp_path / "in.db")
    store.put_in(
        InboundMessage(
            mid=2,
            topic="in",
            payload=b"payload",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBREL,
            logical_size=32,
        )
    )
    engine = _engine(store)
    _feed(engine, _connack(present=True, protocol=MQTTProtocolVersion.MQTTv5))
    assert engine.state is ConnectionState.CONNECTED
    store.close()
