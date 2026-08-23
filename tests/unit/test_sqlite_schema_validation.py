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

    with pytest.raises(RuntimeError, match=rf"{table}.*{column}"):
        SqliteInflightStore(path)
