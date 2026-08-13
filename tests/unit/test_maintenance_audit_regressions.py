"""Regressions for low-risk maintenance findings from the RC2 audit."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import mqttium.persistence.sqlite as sqlite_module
from mqttium.helpers.subscribe import simple
from mqttium.persistence.sqlite import SQLITE_SCHEMA_VERSION, SqliteInflightStore


def test_sqlite_busy_timeout_is_explicitly_pinned_to_existing_five_seconds(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "state.db")
    try:
        timeout_ms = int(store._conn.execute("PRAGMA busy_timeout").fetchone()[0])
        assert timeout_ms == 5000
    finally:
        store.close()


def test_sqlite_constructor_closes_connection_when_initialization_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_connect = sqlite3.connect
    opened: list[TrackingConnection] = []

    class TrackingConnection(sqlite3.Connection):
        was_closed = False

        def close(self) -> None:
            self.was_closed = True
            super().close()

    def tracking_connect(*args, **kwargs):
        kwargs["factory"] = TrackingConnection
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    def fail_schema(self: SqliteInflightStore) -> None:
        raise RuntimeError("schema setup failed")

    monkeypatch.setattr(sqlite_module.sqlite3, "connect", tracking_connect)
    monkeypatch.setattr(SqliteInflightStore, "_prepare_schema", fail_schema)

    with pytest.raises(RuntimeError, match="schema setup failed"):
        SqliteInflightStore(tmp_path / "broken.db")

    assert len(opened) == 1
    assert opened[0].was_closed


def test_current_sqlite_schema_version_requires_both_tables(tmp_path: Path) -> None:
    path = tmp_path / "missing-table.db"
    store = SqliteInflightStore(path)
    store.close()

    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE inbound")
    conn.execute(f"PRAGMA user_version={SQLITE_SCHEMA_VERSION}")
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match=r"missing table\(s\): inbound"):
        SqliteInflightStore(path)


@pytest.mark.asyncio
async def test_simple_subscribe_rejects_non_positive_message_count_before_connecting() -> None:
    with pytest.raises(ValueError, match="msg_count must be greater than 0"):
        await simple("topic", msg_count=0)
    with pytest.raises(ValueError, match="msg_count must be greater than 0"):
        await simple("topic", msg_count=-1)
