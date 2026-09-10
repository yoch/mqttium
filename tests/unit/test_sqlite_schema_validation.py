"""Structural validation for databases already marked at the current schema."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mqttium.persistence.sqlite import SQLITE_SCHEMA_VERSION, SqliteInflightStore
from tests.support import sqlite_logical_snapshot


@pytest.mark.parametrize(
    ("table", "column"),
    [
        ("outbound", "logical_size"),
        ("inbound", "logical_size"),
    ],
)
def test_current_schema_missing_required_column_is_refused(
    tmp_path: Path,
    table: str,
    column: str,
) -> None:
    path = tmp_path / f"missing-{table}-{column}.db"
    SqliteInflightStore(path).close()

    conn = sqlite3.connect(path)
    try:
        conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == SQLITE_SCHEMA_VERSION
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match=rf"invalid columns for {table}"):
        SqliteInflightStore(path)


@pytest.mark.parametrize("journal", ["delete", "wal-closed", "wal-live"])
@pytest.mark.parametrize(
    "damage", ["old-version", "wrong-type", "missing-primary-key", "missing-check", "trigger"]
)
def test_refusal_preserves_committed_schema_and_data(tmp_path, journal, damage) -> None:
    path = tmp_path / "reject.db"
    conn = sqlite3.connect(path)
    if journal.startswith("wal"):
        conn.execute("PRAGMA journal_mode=WAL")
    columns = SqliteInflightStore.OUTBOUND_COLUMNS
    if damage == "wrong-type":
        columns = columns.replace("seq INTEGER", "seq TEXT")
    elif damage == "missing-primary-key":
        columns = columns.replace("mid INTEGER PRIMARY KEY", "mid INTEGER")
    elif damage == "missing-check":
        columns = columns.replace(" CHECK(logical_size > 0)", "")
    conn.execute(f"CREATE TABLE outbound ({columns})")
    conn.execute(f"CREATE TABLE inbound ({SqliteInflightStore.INBOUND_COLUMNS})")
    if damage == "trigger":
        conn.execute("CREATE TRIGGER erase AFTER INSERT ON outbound BEGIN DELETE FROM inbound; END")
    conn.execute(f"PRAGMA user_version={4 if damage == 'old-version' else 5}")
    conn.execute(
        "INSERT INTO outbound VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, 1, 0, 1, 0, 129, "t", None, b"x" * 128),
    )
    conn.commit()
    if journal != "wal-live":
        conn.close()
    before = sqlite_logical_snapshot(path)
    try:
        with pytest.raises(RuntimeError):
            SqliteInflightStore(path)
        assert sqlite_logical_snapshot(path) == before
    finally:
        conn.close()


def test_current_live_wal_schema_is_accepted(tmp_path) -> None:
    path = tmp_path / "live.db"
    first = SqliteInflightStore(path)
    try:
        second = SqliteInflightStore(path)
        second.close()
    finally:
        first.close()


@pytest.mark.parametrize("phase", ["before-read", "in-snapshot", "before-pragmas"])
def test_previous_connection_can_close_during_validation(tmp_path, monkeypatch, phase):
    path = tmp_path / "closing.db"
    previous = SqliteInflightStore(path)
    previous._conn.execute(
        "INSERT INTO outbound VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, 1, 0, 1, 0, 4, "t", None, b"old"),
    )
    previous._conn.commit()
    real_connect = sqlite3.connect
    opened = []

    class Connection(sqlite3.Connection):
        read_started = False

        def execute(self, sql, *args, **kwargs):
            if sql == "PRAGMA user_version" and phase == "before-read":
                previous.close()
            if sql == "PRAGMA journal_mode=WAL" and phase == "before-pragmas":
                previous.close()
            cursor = super().execute(sql, *args, **kwargs)
            if sql == "PRAGMA user_version":
                self.read_started = True
                if phase == "in-snapshot":
                    assert self.in_transaction
                    previous.close()
            return cursor

    def connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs, factory=Connection)
        opened.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", connect)
    try:
        with SqliteInflightStore(path) as reopened:
            assert len(opened) == 1
            assert opened[0].read_started
            assert not opened[0].in_transaction
            assert reopened.get_out(1).payload == b"old"
            assert reopened._conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        previous.close()


def test_large_live_wal_opens_without_manual_file_copies(tmp_path, monkeypatch):
    import shutil

    path = tmp_path / "large.db"
    with SqliteInflightStore(path) as first:
        payload = b"x" * (8 * 1024 * 1024)
        first._conn.execute(
            "INSERT INTO outbound VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 1, 1, 0, 1, 0, len(payload) + 1, "t", None, payload),
        )
        first._conn.commit()

        def forbidden_copy(*args, **kwargs):
            raise AssertionError("opening the store must not copy database or WAL files")

        monkeypatch.setattr(shutil, "copyfile", forbidden_copy)
        with SqliteInflightStore(path) as second:
            assert second.get_out(1).payload == payload
            assert not second._conn.in_transaction


def test_unsupported_version_in_crash_wal_is_refused_without_logical_change(tmp_path):
    import subprocess
    import sys
    from contextlib import closing

    path = tmp_path / "crashed.db"
    SqliteInflightStore(path).close()
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA wal_autocheckpoint=0')
conn.execute('PRAGMA user_version=4')
conn.execute("INSERT INTO outbound VALUES (1, 1, 1, 0, 1, 0, 4, 't', NULL, ?)", (b'wal',))
conn.commit()
os._exit(0)
""",
            str(path),
        ],
        check=True,
    )
    assert Path(str(path) + "-wal").stat().st_size > 0
    with pytest.raises(RuntimeError, match="Unsupported SQLite schema 4"):
        SqliteInflightStore(path)
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert conn.execute("SELECT payload FROM outbound WHERE mid=1").fetchone()[0] == b"wal"
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_fresh_schema_failure_rolls_back_partial_ddl(tmp_path):
    from contextlib import closing

    path = tmp_path / "partial.db"

    class PartialSchema(SqliteInflightStore):
        def _create_schema(self):
            self._conn.execute(f"CREATE TABLE outbound ({self.OUTBOUND_COLUMNS})")
            raise RuntimeError("interrupted initialization")

    with pytest.raises(RuntimeError, match="interrupted initialization"):
        PartialSchema(path)
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 0
        assert conn.execute("SELECT name FROM sqlite_master").fetchall() == []
    with SqliteInflightStore(path) as recovered:
        assert not recovered._conn.in_transaction


def test_fresh_initializer_revalidates_after_acquiring_write_lock(tmp_path, monkeypatch):
    path = tmp_path / "initialized-between-snapshots.db"
    real_connect = sqlite3.connect
    raced = False

    class Connection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            nonlocal raced
            if sql == "BEGIN IMMEDIATE" and not raced:
                assert not self.in_transaction
                raced = True
                # A second real connection initializes after the read snapshot
                # but before this connection acquires its creation write lock.
                SqliteInflightStore(path).close()
            return super().execute(sql, *args, **kwargs)

    def connect(*args, **kwargs):
        return real_connect(*args, **kwargs, factory=Connection)

    class AlreadyInitialized(SqliteInflightStore):
        def _create_schema(self):
            raise AssertionError("schema already created by the other initializer")

    monkeypatch.setattr(sqlite3, "connect", connect)
    with AlreadyInitialized(path) as store:
        assert raced
        assert not store._conn.in_transaction
        assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 5
