"""Queued outbound launches must not rewrite an already-persisted payload."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, OutboundQoSState, PacketType
from mqttium.packets import PubAckPacket, encode_frame
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected(store: SqliteInflightStore) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="queued-transition", max_outbound_inflight=1),
        store=store,
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    return engine


@contextmanager
def _traced(store: SqliteInflightStore) -> Iterator[list[str]]:
    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)
    try:
        yield statements
    finally:
        store._conn.set_trace_callback(None)


def test_drain_transitions_queued_sqlite_record_without_payload_rewrite(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "queued.db")
    engine = _connected(store)

    first = engine.queue_publish("first/topic", b"first", qos=1)
    engine.take_effects()
    second_payload = b"x" * (256 * 1024)
    second = engine.queue_publish("second/topic", second_payload, qos=1)
    engine.take_effects()

    queued = store.get_out(second.mid or 0)
    assert queued is not None
    assert queued.state is OutboundQoSState.QUEUED
    assert queued.payload == second_payload

    with _traced(store) as trace:
        _feed(engine, PubAckPacket(mid=first.mid or 0).encode())
        effects = engine.take_effects()

    assert any(effect.kind is EffectKind.SEND for effect in effects)
    launched = store.get_out(second.mid or 0)
    assert launched is not None
    assert launched.state is OutboundQoSState.WAIT_PUBACK
    assert launched.payload == second_payload

    writes = [statement.upper() for statement in trace if "OUTBOUND" in statement.upper()]
    assert not any("INSERT INTO OUTBOUND" in statement for statement in writes)
    state_updates = [statement for statement in writes if "UPDATE OUTBOUND SET STATE=" in statement]
    assert state_updates
    assert all("PAYLOAD" not in statement for statement in state_updates)
    store.close()
