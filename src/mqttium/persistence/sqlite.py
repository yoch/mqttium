"""SQLite-backed inflight persistence.

The store is synchronous by design and is called from the client's event-loop
thread. A re-entrant lock protects accidental cross-thread access, while
``batch()`` groups protocol transitions into one durable transaction before
generated wire effects are released.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.types import InboundMessage, OutboundMessage, Properties


def _decode_payload(data: bytes | str) -> bytes:
    if isinstance(data, str):
        return base64.b64decode(data.encode("ascii"))
    return bytes(data)


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__mqttium_bytes__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return {"__mqttium_tuple__": [_json_sanitize(v) for v in value]}
    if isinstance(value, list):
        return [_json_sanitize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_sanitize(v) for k, v in value.items()}
    return value


def _json_revive(value: Any) -> Any:
    if isinstance(value, dict):
        if "__mqttium_bytes__" in value and len(value) == 1:
            return base64.b64decode(value["__mqttium_bytes__"].encode("ascii"))
        if "__mqttium_tuple__" in value and len(value) == 1:
            return tuple(_json_revive(v) for v in value["__mqttium_tuple__"])
        return {k: _json_revive(v) for k, v in value.items()}
    if isinstance(value, list):
        revived = [_json_revive(v) for v in value]
        if (
            revived
            and all(isinstance(item, list) and len(item) == 2 for item in revived)
            and all(isinstance(item[0], str) for item in revived)
        ):
            return [tuple(item) for item in revived]
        return revived
    return value


def _props_to_json(props: Properties | None) -> str | None:
    if props is None or not props.values:
        return None
    return json.dumps(_json_sanitize(props.values), separators=(",", ":"))


def _props_from_json(raw: str | None) -> Properties | None:
    if not raw:
        return None
    values = _json_revive(json.loads(raw))
    if not isinstance(values, dict):
        raise ValueError("Invalid properties JSON payload")
    return Properties(values=values)


def _row_to_out(row: sqlite3.Row) -> OutboundMessage:
    return OutboundMessage(
        mid=int(row["mid"]),
        topic=str(row["topic"]),
        payload=_decode_payload(row["payload"]),
        qos=QoS(int(row["qos"])),
        retain=bool(row["retain"]),
        state=OutboundQoSState(int(row["state"])),
        dup=bool(row["dup"]),
        properties=_props_from_json(row["properties"]),
    )


def _row_to_in(row: sqlite3.Row) -> InboundMessage:
    return InboundMessage(
        mid=int(row["mid"]),
        topic=str(row["topic"]),
        payload=_decode_payload(row["payload"]),
        qos=QoS(int(row["qos"])),
        retain=bool(row["retain"]),
        state=InboundQoSState(int(row["state"])),
        delivered=bool(row["delivered"]),
        properties=_props_from_json(row["properties"]),
        user_acked=bool(row["user_acked"]),
    )


class SqliteInflightStore:
    """Durable ordered store for outbound and inbound QoS state."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._batch_depth = 0
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._closed = False
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS outbound (
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
            CREATE TABLE IF NOT EXISTS inbound (
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
        )
        self._conn.commit()
        self._out_seq = self._max_seq("outbound")
        self._in_seq = self._max_seq("inbound")

    def _max_seq(self, table: str) -> int:
        row = self._conn.execute(f"SELECT COALESCE(MAX(seq), 0) FROM {table}").fetchone()
        assert row is not None
        return int(row[0])

    @contextmanager
    def batch(self) -> Iterator[None]:
        with self._lock:
            outermost = self._batch_depth == 0
            if outermost:
                self._conn.execute("BEGIN IMMEDIATE")
            self._batch_depth += 1
            try:
                yield
            except BaseException:
                self._batch_depth -= 1
                if outermost:
                    self._conn.rollback()
                raise
            else:
                self._batch_depth -= 1
                if outermost:
                    self._conn.commit()

    def _commit_if_needed(self) -> None:
        if self._batch_depth == 0:
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._batch_depth:
                raise RuntimeError("Cannot close SQLite store inside batch()")
            self._conn.commit()
            self._conn.close()
            self._closed = True

    def __enter__(self) -> SqliteInflightStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def put_out(self, msg: OutboundMessage) -> None:
        with self._lock:
            self._out_seq += 1
            self._conn.execute(
                """
                INSERT INTO outbound(
                    mid, topic, payload, qos, retain, state, dup,
                    properties, extra, seq
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(mid) DO UPDATE SET
                    topic=excluded.topic, payload=excluded.payload,
                    qos=excluded.qos, retain=excluded.retain,
                    state=excluded.state, dup=excluded.dup,
                    properties=excluded.properties
                """,
                (
                    msg.mid,
                    msg.topic,
                    sqlite3.Binary(msg.payload),
                    int(msg.qos),
                    int(msg.retain),
                    int(msg.state),
                    int(msg.dup),
                    _props_to_json(msg.properties),
                    self._out_seq,
                ),
            )
            self._commit_if_needed()

    def get_out(self, mid: int) -> OutboundMessage | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM outbound WHERE mid=?", (mid,)).fetchone()
        return _row_to_out(row) if row else None

    def pop_out(self, mid: int) -> OutboundMessage | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM outbound WHERE mid=?", (mid,)).fetchone()
            if row is None:
                return None
            self._conn.execute("DELETE FROM outbound WHERE mid=?", (mid,))
            self._commit_if_needed()
        return _row_to_out(row)

    def delete_out(self, mid: int) -> bool:
        """Delete an outbound record without reading or reconstructing it."""
        with self._lock:
            cursor = self._conn.execute("DELETE FROM outbound WHERE mid=?", (mid,))
            self._commit_if_needed()
            return cursor.rowcount > 0

    def update_out(self, msg: OutboundMessage) -> None:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE outbound SET state=?, dup=? WHERE mid=?",
                (int(msg.state), int(msg.dup), msg.mid),
            )
            if cur.rowcount == 0:
                raise KeyError(msg.mid)
            self._commit_if_needed()

    def out_items(self) -> Iterator[OutboundMessage]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM outbound ORDER BY seq").fetchall()
        return iter(_row_to_out(row) for row in rows)

    def clear_out(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM outbound")
            self._commit_if_needed()

    def put_in(self, msg: InboundMessage) -> None:
        with self._lock:
            self._in_seq += 1
            self._conn.execute(
                """
                INSERT INTO inbound(
                    mid, topic, payload, qos, retain, state, delivered,
                    properties, user_acked, seq
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mid) DO UPDATE SET
                    topic=excluded.topic, payload=excluded.payload,
                    qos=excluded.qos, retain=excluded.retain,
                    state=excluded.state, delivered=excluded.delivered,
                    properties=excluded.properties,
                    user_acked=excluded.user_acked
                """,
                (
                    msg.mid,
                    msg.topic,
                    sqlite3.Binary(msg.payload),
                    int(msg.qos),
                    int(msg.retain),
                    int(msg.state),
                    int(msg.delivered),
                    _props_to_json(msg.properties),
                    int(msg.user_acked),
                    self._in_seq,
                ),
            )
            self._commit_if_needed()

    def get_in(self, mid: int) -> InboundMessage | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM inbound WHERE mid=?", (mid,)).fetchone()
        return _row_to_in(row) if row else None

    def pop_in(self, mid: int) -> InboundMessage | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM inbound WHERE mid=?", (mid,)).fetchone()
            if row is None:
                return None
            self._conn.execute("DELETE FROM inbound WHERE mid=?", (mid,))
            self._commit_if_needed()
        return _row_to_in(row)

    def update_in(self, msg: InboundMessage) -> None:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE inbound
                SET state=?, delivered=?, user_acked=?
                WHERE mid=?
                """,
                (
                    int(msg.state),
                    int(msg.delivered),
                    int(msg.user_acked),
                    msg.mid,
                ),
            )
            if cur.rowcount == 0:
                raise KeyError(msg.mid)
            self._commit_if_needed()

    def in_items(self) -> Iterator[InboundMessage]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM inbound ORDER BY seq").fetchall()
        return iter(_row_to_in(row) for row in rows)

    def clear_in(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM inbound")
            self._commit_if_needed()
