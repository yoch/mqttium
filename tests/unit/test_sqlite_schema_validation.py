"""Structural validation for databases already marked at the current schema."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mqttium.persistence.sqlite import SQLITE_SCHEMA_VERSION, SqliteInflightStore


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
def test_refusal_does_not_touch_database_or_journals(tmp_path, journal, damage) -> None:
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
    conn.commit()
    if journal != "wal-live":
        conn.close()
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    try:
        with pytest.raises(RuntimeError):
            SqliteInflightStore(path)
        assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before
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
