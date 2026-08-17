"""Durable schema versioning, migration and lazy write transactions."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.persistence.sqlite import SQLITE_SCHEMA_VERSION, SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage

# The exact schema shipped before PRAGMA user_version existed: no logical_size
# column, no seq index. Databases in this shape exist in the wild and must keep
# opening.
V1_SCHEMA = """
CREATE TABLE outbound (
    mid INTEGER PRIMARY KEY,
    topic TEXT NOT NULL,
    payload BLOB NOT NULL,
    qos INTEGER NOT NULL,
    retain INTEGER NOT NULL,
    state INTEGER NOT NULL,
    dup INTEGER NOT NULL,
    properties TEXT,
    extra INTEGER NOT NULL DEFAULT 0,
    seq INTEGER NOT NULL
);
CREATE TABLE inbound (
    mid INTEGER PRIMARY KEY,
    topic TEXT NOT NULL,
    payload BLOB NOT NULL,
    qos INTEGER NOT NULL,
    retain INTEGER NOT NULL,
    state INTEGER NOT NULL,
    delivered INTEGER NOT NULL,
    properties TEXT,
    user_acked INTEGER NOT NULL,
    seq INTEGER NOT NULL
);
"""


def write_v1_database(path: Path, *, outbound_records: int = 3) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA)
    for mid in range(1, outbound_records + 1):
        conn.execute(
            """
            INSERT INTO outbound(
                mid, topic, payload, qos, retain, state, dup, properties, extra, seq
            ) VALUES (?, 'legacy/topic', ?, 1, 0, ?, 0, NULL, 0, ?)
            """,
            (mid, sqlite3.Binary(bytes([mid]) * 64), int(OutboundQoSState.QUEUED), mid),
        )
    conn.execute(
        """
        INSERT INTO inbound(
            mid, topic, payload, qos, retain, state, delivered,
            properties, user_acked, seq
        ) VALUES (9, 'legacy/in', ?, 2, 0, ?, 0, NULL, 0, 1)
        """,
        (sqlite3.Binary(b"inbound"), int(InboundQoSState.WAIT_PUBREL)),
    )
    conn.commit()
    conn.close()
    assert user_version(path) == 0


def user_version(path: Path) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])
    finally:
        conn.close()


def column_order(path: Path, table: str) -> list[str]:
    conn = sqlite3.connect(path)
    try:
        return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]
    finally:
        conn.close()


def tables(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {str(row[0]) for row in rows}
    finally:
        conn.close()


def indices(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        return {str(row[0]) for row in rows}
    finally:
        conn.close()


def test_fresh_database_is_created_at_the_current_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "fresh.db"
    store = SqliteInflightStore(path)
    store.close()

    assert user_version(path) == SQLITE_SCHEMA_VERSION
    assert "logical_size" in column_order(path, "outbound")
    assert "logical_size" in column_order(path, "inbound")
    assert "extra" not in column_order(path, "outbound")
    # Ordered pagination is served by a sorted metadata pass, not by a second
    # B-tree maintained on every publish.
    assert not indices(path)
    assert column_order(path, "outbound")[-1] == "payload"
    assert column_order(path, "inbound")[-1] == "payload"


def test_v1_database_migrates_without_losing_or_duplicating_records(tmp_path: Path) -> None:
    path = tmp_path / "v1.db"
    write_v1_database(path)

    store = SqliteInflightStore(path)
    outbound = list(store.out_items())
    inbound = list(store.in_items())
    store.close()

    assert user_version(path) == SQLITE_SCHEMA_VERSION
    assert not indices(path)
    assert column_order(path, "outbound")[-1] == "payload"
    assert "extra" not in column_order(path, "outbound")
    assert [msg.mid for msg in outbound] == [1, 2, 3]
    assert [msg.payload for msg in outbound] == [bytes([mid]) * 64 for mid in (1, 2, 3)]
    assert [msg.state for msg in outbound] == [OutboundQoSState.QUEUED] * 3
    # Migrated rows keep an unknown logical size, which the outbound session
    # already recomputes lazily.
    assert [msg.logical_size for msg in outbound] == [0, 0, 0]
    assert [msg.mid for msg in inbound] == [9]
    assert inbound[0].payload == b"inbound"
    assert inbound[0].logical_size == 0


def test_v3_inbound_size_is_backfilled_once_and_persisted(tmp_path: Path) -> None:
    path = tmp_path / "v3-inbound.db"
    store = SqliteInflightStore(path)
    topic = "legacy/v3"
    payload = b"retained"
    store.put_in(
        InboundMessage(
            mid=17,
            topic=topic,
            payload=payload,
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBREL,
            logical_size=len(topic) + len(payload),
        )
    )
    store.close()

    conn = sqlite3.connect(path)
    conn.executescript(
        """
        ALTER TABLE inbound RENAME TO inbound__v4;
        CREATE TABLE inbound (
            mid INTEGER PRIMARY KEY,
            seq INTEGER NOT NULL,
            qos INTEGER NOT NULL,
            retain INTEGER NOT NULL,
            state INTEGER NOT NULL,
            delivered INTEGER NOT NULL,
            user_acked INTEGER NOT NULL,
            topic TEXT NOT NULL,
            properties TEXT,
            payload BLOB NOT NULL
        );
        INSERT INTO inbound
        SELECT mid, seq, qos, retain, state, delivered, user_acked,
               topic, properties, payload
        FROM inbound__v4;
        DROP TABLE inbound__v4;
        PRAGMA user_version=3;
        """
    )
    conn.commit()
    conn.close()

    migrated = SqliteInflightStore(path)
    record = migrated.get_in(17)
    assert record is not None
    assert record.logical_size == 0

    engine = ProtocolEngine(EngineConfig(clean_start=False), store=migrated)
    expected = len(topic) + len(payload)
    assert engine.inbound.stats().pending_bytes == expected
    migrated.close()

    reopened = SqliteInflightStore(path)
    persisted = reopened.get_in(17)
    assert persisted is not None
    assert persisted.logical_size == expected
    reopened.close()


def test_interrupted_migration_leaves_the_v1_database_intact(tmp_path: Path) -> None:
    path = tmp_path / "interrupted.db"
    write_v1_database(path)

    class Interrupted(SqliteInflightStore):
        # First rebuild a schema-1 database performs; crashing after it proves
        # the whole upgrade is one transaction, not that a particular step is.
        def _rebuild_outbound(self) -> None:
            super()._rebuild_outbound()
            raise RuntimeError("simulated crash mid-migration")

    with pytest.raises(RuntimeError, match="simulated crash"):
        Interrupted(path)

    assert user_version(path) == 0
    assert "logical_size" not in column_order(path, "outbound")
    assert column_order(path, "outbound")[-1] != "payload"
    assert "outbound__v2" not in tables(path)

    # The retry after the crash still succeeds and still sees every record.
    store = SqliteInflightStore(path)
    assert [msg.mid for msg in store.out_items()] == [1, 2, 3]
    store.close()
    assert user_version(path) == SQLITE_SCHEMA_VERSION


def test_reopening_a_migrated_database_does_not_migrate_again(tmp_path: Path) -> None:
    path = tmp_path / "reopen.db"
    write_v1_database(path)
    SqliteInflightStore(path).close()

    class NoSchemaWork(SqliteInflightStore):
        def _create_schema(self) -> None:
            raise AssertionError("schema recreated on an up-to-date database")

        def _migrate_to_v2(self) -> None:
            raise AssertionError("migration re-run on an up-to-date database")

    store = NoSchemaWork(path)
    assert [msg.mid for msg in store.out_items()] == [1, 2, 3]
    store.close()


def test_a_newer_schema_version_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    SqliteInflightStore(path).close()
    conn = sqlite3.connect(path)
    conn.execute(f"PRAGMA user_version={SQLITE_SCHEMA_VERSION + 1}")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="newer MQTTium"):
        SqliteInflightStore(path)


def test_migrated_database_replays_through_the_engine(tmp_path: Path) -> None:
    path = tmp_path / "replay.db"
    write_v1_database(path)
    store = SqliteInflightStore(path)

    engine = ProtocolEngine(EngineConfig(clean_start=False), store=store)

    assert len(engine._queued) == 3
    assert engine.pending_outbound_messages == 3
    # Recomputed from topic and payload size, exactly as before the column.
    assert engine.pending_outbound_bytes == 3 * (64 + len("legacy/topic"))
    store.close()


def test_logical_size_is_persisted_for_records_written_at_v2(tmp_path: Path) -> None:
    path = tmp_path / "sized.db"
    store = SqliteInflightStore(path)
    store.put_out(
        OutboundMessage(
            mid=4,
            topic="a/b",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
            logical_size=123,
        )
    )
    store.close()

    reopened = SqliteInflightStore(path)
    message = reopened.get_out(4)
    assert message is not None
    assert message.logical_size == 123
    summary = next(iter(reopened.out_summary_pages()))[0]
    assert summary.logical_size == 123
    reopened.close()


def test_a_read_only_batch_opens_no_write_transaction(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "lazy.db")
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="a/b",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    trace: list[str] = []
    store._conn.set_trace_callback(trace.append)

    with store.batch():
        assert store.get_out(1) is not None
        assert store.get_out(2) is None

    assert not any(line.startswith(("BEGIN", "COMMIT", "ROLLBACK")) for line in trace)
    assert store._transaction_started is False
    store.close()


def test_a_mutating_batch_opens_exactly_one_transaction(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "lazy-write.db")
    trace: list[str] = []
    store._conn.set_trace_callback(trace.append)

    with store.batch():
        assert store.get_out(1) is None
        store.put_out(
            OutboundMessage(
                mid=1,
                topic="a/b",
                payload=b"x",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBACK,
            )
        )
        store.delete_out(1)

    assert sum(line == "BEGIN IMMEDIATE" for line in trace) == 1
    assert sum(line == "COMMIT" for line in trace) == 1
    assert store._transaction_started is False
    store.close()


def test_a_failing_lazy_batch_rolls_back_its_mutations(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "lazy-rollback.db")

    with pytest.raises(ZeroDivisionError):
        with store.batch():
            store.put_out(
                OutboundMessage(
                    mid=1,
                    topic="a/b",
                    payload=b"x",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    state=OutboundQoSState.WAIT_PUBACK,
                )
            )
            raise ZeroDivisionError

    assert store.get_out(1) is None
    assert store._transaction_started is False
    # The store stays usable: the failed batch left no open transaction behind.
    store.put_out(
        OutboundMessage(
            mid=2,
            topic="a/b",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    assert store.get_out(2) is not None
    store.close()


def test_a_read_only_nested_batch_still_commits_the_outer_mutation(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "lazy-nested.db")

    with store.batch():
        with store.batch():
            store.put_out(
                OutboundMessage(
                    mid=1,
                    topic="a/b",
                    payload=b"x",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    state=OutboundQoSState.WAIT_PUBACK,
                )
            )
        assert store.get_out(1) is not None

    store.close()
    reopened = SqliteInflightStore(tmp_path / "lazy-nested.db")
    assert reopened.get_out(1) is not None
    reopened.close()
