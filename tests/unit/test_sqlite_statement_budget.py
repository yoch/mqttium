"""SQL statements and commits spent by one persisted QoS exchange.

A commit is what a durable exchange costs. An indicative measurement with
plain ``sqlite3`` (hosted container, 4 KiB payload) gave about 64 microseconds
for an insert and a delete committed separately, against about 4 inside a
shared transaction; replacing the metadata ``SELECT`` by ``RETURNING`` changed
neither figure measurably. These tests pin the exact number of statements and
transactions, so a change that adds one to the hot path fails here, without a
timing and on any machine.

Lowering a budget is an improvement: update the figure in the same change.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from pathlib import Path

import pytest

from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_frame,
)
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from tests.support import feed_engine, mark_delivered_messages

V311 = MQTTProtocolVersion.MQTTv311
Cycle = Callable[[ProtocolEngine, int], None]


def _publish(qos: int, mid: int) -> bytes:
    return PublishPacket(
        topic="in/topic", payload=b"payload", qos=QoS(qos), retain=False, dup=False, mid=mid
    ).encode(V311)


def _outbound_qos1(engine: ProtocolEngine, _index: int) -> None:
    handle = engine.queue_publish("out/topic", b"payload", qos=1)
    assert handle.mid is not None
    feed_engine(engine, PubAckPacket(mid=handle.mid).encode(V311))


def _outbound_qos2(engine: ProtocolEngine, _index: int) -> None:
    handle = engine.queue_publish("out/topic", b"payload", qos=2)
    assert handle.mid is not None
    feed_engine(engine, PubRecPacket(mid=handle.mid).encode(V311))
    feed_engine(engine, PubCompPacket(mid=handle.mid).encode(V311))


def _inbound_qos1(engine: ProtocolEngine, index: int) -> None:
    feed_engine(engine, _publish(1, index))
    mark_delivered_messages(engine, engine.take_effects())


def _inbound_qos1_manual(engine: ProtocolEngine, index: int) -> None:
    feed_engine(engine, _publish(1, index))
    mark_delivered_messages(engine, engine.take_effects())
    engine.ack(index)


def _inbound_qos2(engine: ProtocolEngine, index: int) -> None:
    feed_engine(engine, _publish(2, index))
    mark_delivered_messages(engine, engine.take_effects())
    feed_engine(engine, PubRelPacket(mid=index).encode(V311))


# (cycle, manual acknowledgement, statements, transactions when not batched)
BUDGETS: dict[str, tuple[Cycle, bool, dict[str, int], int]] = {
    "outbound QoS 1": (_outbound_qos1, False, {"INSERT": 1, "SELECT": 1, "DELETE": 1}, 2),
    "outbound QoS 2": (
        _outbound_qos2,
        False,
        {"INSERT": 1, "SELECT": 2, "UPDATE": 1, "DELETE": 1},
        3,
    ),
    # An automatically acknowledged QoS 1 delivery is never written.
    "inbound QoS 1": (_inbound_qos1, False, {}, 0),
    "inbound QoS 1, manual acknowledgement": (
        _inbound_qos1_manual,
        True,
        {"INSERT": 1, "SELECT": 3, "UPDATE": 1, "DELETE": 1},
        3,
    ),
    "inbound QoS 2": (
        _inbound_qos2,
        False,
        {"INSERT": 1, "SELECT": 2, "UPDATE": 1, "DELETE": 1},
        3,
    ),
}


def _connected(store: SqliteInflightStore, *, manual_ack: bool) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(client_id="budget", manual_ack=manual_ack), store=store)
    engine.begin_connect()
    feed_engine(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    return engine


def _statements(store: SqliteInflightStore, cycle: Cycle, engine: ProtocolEngine) -> Counter[str]:
    """The SQL verbs of the second cycle; the first one warms caches and counters."""
    cycle(engine, 1)
    engine.take_effects()
    traced: list[str] = []
    store._conn.set_trace_callback(traced.append)
    try:
        cycle(engine, 2)
    finally:
        store._conn.set_trace_callback(None)
    engine.take_effects()
    return Counter(statement.split()[0].upper() for statement in traced)


@pytest.mark.parametrize("name", sorted(BUDGETS))
def test_one_exchange_spends_its_statement_budget(tmp_path: Path, name: str) -> None:
    cycle, manual_ack, statements, transactions = BUDGETS[name]
    store = SqliteInflightStore(tmp_path / "budget.sqlite")
    try:
        spent = _statements(store, cycle, _connected(store, manual_ack=manual_ack))
    finally:
        store.close()
    expected = dict(statements)
    if transactions:
        expected |= {"BEGIN": transactions, "COMMIT": transactions}
    assert dict(spent) == expected


@pytest.mark.parametrize("name", sorted(BUDGETS))
def test_a_batch_adds_no_transaction_per_exchange(tmp_path: Path, name: str) -> None:
    cycle, manual_ack, statements, _transactions = BUDGETS[name]
    store = SqliteInflightStore(tmp_path / "budget.sqlite")
    try:
        engine = _connected(store, manual_ack=manual_ack)
        with store.batch():
            spent = _statements(store, cycle, engine)
    finally:
        store.close()
    assert dict(spent) == statements
