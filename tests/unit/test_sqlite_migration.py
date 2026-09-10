"""Durable schema versioning, refusal and lazy write transactions."""

from __future__ import annotations

from tests.support import stored_record

import sqlite3
from pathlib import Path
from contextlib import closing

import pytest

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.persistence.sqlite import SQLITE_SCHEMA_VERSION, SqliteInflightStore
from mqttium.types import OutboundMessage

# The exact schema shipped before PRAGMA user_version existed: no logical_size
# column, no seq index. Historical files must remain intact while
# being refused without modification.
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


def test_logical_size_is_persisted_for_current_records(tmp_path: Path) -> None:
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
        stored_record(
            OutboundMessage(
                mid=1,
                topic="a/b",
                payload=b"x",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBACK,
            )
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
            stored_record(
                OutboundMessage(
                    mid=1,
                    topic="a/b",
                    payload=b"x",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    state=OutboundQoSState.WAIT_PUBACK,
                )
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
                stored_record(
                    OutboundMessage(
                        mid=1,
                        topic="a/b",
                        payload=b"x",
                        qos=QoS.AT_LEAST_ONCE,
                        retain=False,
                        state=OutboundQoSState.WAIT_PUBACK,
                    )
                )
            )
            raise ZeroDivisionError

    assert store.get_out(1) is None
    assert store._transaction_started is False
    # The store stays usable: the failed batch left no open transaction behind.
    store.put_out(
        stored_record(
            OutboundMessage(
                mid=2,
                topic="a/b",
                payload=b"x",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBACK,
            )
        )
    )
    assert store.get_out(2) is not None
    store.close()


def test_a_read_only_nested_batch_still_commits_the_outer_mutation(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "lazy-nested.db")

    with store.batch():
        with store.batch():
            store.put_out(
                stored_record(
                    OutboundMessage(
                        mid=1,
                        topic="a/b",
                        payload=b"x",
                        qos=QoS.AT_LEAST_ONCE,
                        retain=False,
                        state=OutboundQoSState.WAIT_PUBACK,
                    )
                )
            )
        assert store.get_out(1) is not None

    store.close()
    reopened = SqliteInflightStore(tmp_path / "lazy-nested.db")
    assert reopened.get_out(1) is not None
    reopened.close()


@pytest.mark.parametrize("version", [0, 1, 2, 3, 4, 6, 100])
def test_other_formats_are_refused_without_any_file_modification(tmp_path, version) -> None:
    path = tmp_path / "unsupported.db"
    write_v1_database(path)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(f"PRAGMA user_version={version}")
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    with pytest.raises(RuntimeError):
        SqliteInflightStore(path)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


def test_current_format_reopens_without_schema_work(tmp_path) -> None:
    path = tmp_path / "current.db"
    SqliteInflightStore(path).close()

    class NoSchemaWork(SqliteInflightStore):
        def _create_schema(self):
            raise AssertionError("current schema recreated")

    NoSchemaWork(path).close()
