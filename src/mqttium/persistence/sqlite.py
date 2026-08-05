"""SQLite-backed inflight persistence.

The store is synchronous by design and is called from the client's event-loop
thread. A re-entrant lock protects accidental cross-thread access, while
``batch()`` groups protocol transitions into one durable transaction before
generated wire effects are released.

The durable format is versioned through ``PRAGMA user_version``. A database
written by an older MQTTium is migrated in one transaction on open, so an
interrupted migration leaves the previous version intact rather than a half
schema. A database written by a *newer* MQTTium is refused instead of being
silently reinterpreted.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import threading
from array import array
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, TypeVar

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.types import (
    InboundMessage,
    InboundRecordMeta,
    OutboundMessage,
    OutboundMessageSummary,
    OutboundRecordMeta,
    Properties,
)


_RecordT = TypeVar("_RecordT")

# SQLITE_MAX_VARIABLE_NUMBER is 32766 since SQLite 3.32 but only 999 in older
# builds, and Python may be linked against either. Page fetches stay under the
# floor so a large `page_size` cannot produce "too many SQL variables".
_MAX_SQL_VARIABLES = 900

# Page statements: fully written out, never composed, so no SQL text in this
# module derives from a value. Every column except `payload` is listed first, so
# a summary read stops before the BLOB and never follows an overflow page. Each
# ends in `mid IN`, to which `_pages` appends only a run of `?` placeholders —
# the identifiers themselves always travel as bound parameters.
_OUT_PAGE_SQL = (
    "SELECT mid, seq, qos, retain, state, dup, logical_size, topic, properties, payload"
    " FROM outbound WHERE mid IN"
)
_OUT_SUMMARY_PAGE_SQL = (
    "SELECT mid, seq, qos, retain, state, dup, logical_size, topic, properties,"
    " length(payload) AS payload_size FROM outbound WHERE mid IN"
)
_IN_PAGE_SQL = (
    "SELECT mid, seq, qos, retain, state, delivered, user_acked, topic, properties, payload"
    " FROM inbound WHERE mid IN"
)
_IN_INDEX_PAGE_SQL = "SELECT mid, state, user_acked FROM inbound WHERE mid IN"

SQLITE_SCHEMA_VERSION = 2
"""Durable schema revision stored in ``PRAGMA user_version``.

* 1 — the original schema, which carried no version marker at all.
* 2 — adds ``outbound.logical_size``, drops the never-read ``extra`` column,
  and moves ``payload`` last so metadata reads never traverse BLOB overflow
  pages.
"""


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
        payload=bytes(row["payload"]),
        qos=QoS(int(row["qos"])),
        retain=bool(row["retain"]),
        state=OutboundQoSState(int(row["state"])),
        dup=bool(row["dup"]),
        properties=_props_from_json(row["properties"]),
        # Rows migrated from schema 1 carry 0, which the outbound session reads
        # as "unknown" and recomputes once, exactly as it did before the column
        # existed.
        logical_size=int(row["logical_size"]),
    )


def _row_to_out_summary(row: sqlite3.Row) -> OutboundMessageSummary:
    return OutboundMessageSummary(
        mid=int(row["mid"]),
        topic=str(row["topic"]),
        payload_size=int(row["payload_size"]),
        qos=QoS(int(row["qos"])),
        retain=bool(row["retain"]),
        state=OutboundQoSState(int(row["state"])),
        dup=bool(row["dup"]),
        properties=_props_from_json(row["properties"]),
        logical_size=int(row["logical_size"]),
    )


def _row_to_in_meta(row: sqlite3.Row) -> InboundRecordMeta:
    return InboundRecordMeta(
        mid=int(row["mid"]),
        state=InboundQoSState(int(row["state"])),
        user_acked=bool(row["user_acked"]),
    )


def _row_to_in(row: sqlite3.Row) -> InboundMessage:
    return InboundMessage(
        mid=int(row["mid"]),
        topic=str(row["topic"]),
        payload=bytes(row["payload"]),
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
        self._transaction_started = False
        self._rollback_only = False
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._closed = False
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._prepare_schema()
        self._out_seq = self._max_seq("outbound")
        self._in_seq = self._max_seq("inbound")

    # --- schema ------------------------------------------------------------

    def _prepare_schema(self) -> None:
        """Create, verify or migrate the durable schema, atomically."""
        conn = self._conn
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version > SQLITE_SCHEMA_VERSION:
            conn.close()
            self._closed = True
            raise RuntimeError(
                f"{self._path} was written by a newer MQTTium "
                f"(schema {version} > {SQLITE_SCHEMA_VERSION})"
            )
        if version == SQLITE_SCHEMA_VERSION:
            return
        # A pre-versioning database reports 0 while already holding tables; a
        # genuinely empty file reports 0 with nothing in sqlite_master.
        conn.execute("BEGIN IMMEDIATE")
        try:
            legacy = self._table_exists("outbound") or self._table_exists("inbound")
            self._create_schema()
            if version < 2 and legacy:
                self._migrate_to_v2()
            conn.execute(f"PRAGMA user_version={SQLITE_SCHEMA_VERSION}")
        except BaseException:
            # One transaction for the whole upgrade: an interrupted migration
            # reopens as the version it started from, never as half of two.
            conn.rollback()
            raise
        conn.commit()

    # Column order is a storage decision, not a cosmetic one. SQLite reaches a
    # column by walking the ones declared before it, and a payload past a few
    # hundred bytes spills into overflow pages — so every metadata column placed
    # after `payload` costs an overflow traversal on a read that never wanted
    # the payload. `payload` is therefore declared last, behind every column the
    # metadata reads, summary pages and conditional transitions use. Measured on
    # 10,000 x 4 KiB records: an ordered metadata pass drops from 65.6 ms to
    # 28.1 ms, and a `state`/`logical_size` lookup from 9.1 us to 7.8 us.
    OUTBOUND_COLUMNS = """
        mid INTEGER PRIMARY KEY,
        seq INTEGER NOT NULL,
        qos INTEGER NOT NULL,
        retain INTEGER NOT NULL,
        state INTEGER NOT NULL,
        dup INTEGER NOT NULL,
        logical_size INTEGER NOT NULL DEFAULT 0,
        topic TEXT NOT NULL,
        properties TEXT,
        payload BLOB NOT NULL
    """
    INBOUND_COLUMNS = """
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
    """

    def _create_schema(self) -> None:
        conn = self._conn
        conn.execute(f"CREATE TABLE IF NOT EXISTS outbound ({self.OUTBOUND_COLUMNS})")
        conn.execute(f"CREATE TABLE IF NOT EXISTS inbound ({self.INBOUND_COLUMNS})")

    def _migrate_to_v2(self) -> None:
        """Schema 1 → 2: persisted logical size and payload-last column order.

        Reordering columns requires a table rebuild, which is also the cheapest
        moment to add `logical_size`. Migrated rows keep it at 0, which the
        outbound session already treats as "not computed yet" and backfills once
        at hydration.

        Deliberately no `seq` index: ordered pagination is served by one sorted
        metadata pass (see `_ordered_mids`), which measured faster than the
        indexed page-per-query form while costing nothing on every INSERT and
        DELETE of the publish hot path.
        """
        # Rebuilding drops each old table, and with it any index a previous
        # build had created on it. The literal `0` supplies `logical_size` for
        # rows written before the column existed.
        self._rebuild_table(
            "outbound",
            f"CREATE TABLE outbound__v2 ({self.OUTBOUND_COLUMNS})",
            "INSERT INTO outbound__v2 SELECT mid, seq, qos, retain, state, dup,"
            " 0, topic, properties, payload FROM outbound",
            "DROP TABLE outbound",
            "ALTER TABLE outbound__v2 RENAME TO outbound",
        )
        self._rebuild_table(
            "inbound",
            f"CREATE TABLE inbound__v2 ({self.INBOUND_COLUMNS})",
            "INSERT INTO inbound__v2 SELECT mid, seq, qos, retain, state,"
            " delivered, user_acked, topic, properties, payload FROM inbound",
            "DROP TABLE inbound",
            "ALTER TABLE inbound__v2 RENAME TO inbound",
        )

    def _rebuild_table(self, table: str, *statements: str) -> None:
        """Recreate one table in the current column order, preserving its rows.

        The statements are passed in already written out rather than assembled
        from the table name, so no SQL text here is derived from a value.
        """
        if not self._table_exists(table):
            return
        for statement in statements:
            self._conn.execute(statement)

    def _table_exists(self, table: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        return row is not None

    # Complete, literal SQL selected by table. Nothing in this module builds a
    # statement out of a runtime value: the only text ever appended to a query
    # is a run of `?` placeholders (see `_pages`), so every value still travels
    # as a bound parameter.
    _ORDERED_MIDS_SQL = {
        "outbound": "SELECT mid FROM outbound ORDER BY seq",
        "inbound": "SELECT mid FROM inbound ORDER BY seq",
    }

    def _ordered_mids(self, table: str) -> array[int]:
        """Identifiers in insertion order, in one sorted metadata-only pass.

        This replaces `WHERE seq>? ORDER BY seq LIMIT ?` issued once per page,
        which re-scanned and re-sorted the table for every page and made a full
        replay quadratic. One pass touches `mid` and `seq` only — both declared
        ahead of `payload` — and the result is 8 bytes per record, so a 60,000
        record session costs under half a megabyte instead of a second B-tree
        maintained on every publish.
        """
        with self._lock:
            rows = self._conn.execute(self._ORDERED_MIDS_SQL[table]).fetchall()
        return array("q", (int(row[0]) for row in rows))

    def _pages(
        self,
        table: str,
        select_prefix: str,
        page_size: int,
        build: Callable[[sqlite3.Row], _RecordT],
    ) -> Iterator[tuple[_RecordT, ...]]:
        """Page a table in insertion order.

        `select_prefix` is a complete constant statement ending in `mid IN`; the
        only thing appended is the placeholder run for the page.
        """
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        mids = self._ordered_mids(table)
        for start in range(0, len(mids), page_size):
            chunk = mids[start : start + page_size]
            by_mid: dict[int, sqlite3.Row] = {}
            # `page_size` is caller-controlled and each identifier binds one SQL
            # variable, so the fetch is split under the oldest documented
            # SQLITE_MAX_VARIABLE_NUMBER rather than trusting the build.
            for offset in range(0, len(chunk), _MAX_SQL_VARIABLES):
                sub = chunk[offset : offset + _MAX_SQL_VARIABLES]
                placeholders = ",".join("?" * len(sub))
                with self._lock:
                    rows = self._conn.execute(
                        f"{select_prefix} ({placeholders})", tuple(sub)
                    ).fetchall()
                by_mid.update((int(row["mid"]), row) for row in rows)
            if not by_mid:
                continue
            # `IN` returns rows in primary-key order, so insertion order is
            # restored from the chunk. Records deleted since the snapshot are
            # simply absent, exactly as in MemoryInflightStore.
            page = tuple(build(row) for mid in chunk if (row := by_mid.get(mid)) is not None)
            if page:
                yield page

    _MAX_SEQ_SQL = {
        "outbound": "SELECT COALESCE(MAX(seq), 0) FROM outbound",
        "inbound": "SELECT COALESCE(MAX(seq), 0) FROM inbound",
    }

    def _max_seq(self, table: Literal["outbound", "inbound"]) -> int:
        row = self._conn.execute(self._MAX_SEQ_SQL[table]).fetchone()
        if row is None:
            raise RuntimeError(f"Failed to read maximum sequence from {table}")
        return int(row[0])

    @contextmanager
    def batch(self) -> Iterator[None]:
        """Group mutations into one durable transaction — lazily.

        The reader opens a batch around *every* ingress lot, but most lots
        (QoS 0, PINGRESP, acknowledgements resolving nothing) mutate nothing at
        all. Deferring ``BEGIN IMMEDIATE`` to the first mutation keeps those
        lots from taking SQLite's write lock and from paying a commit.

        Any exception crossing a nested batch marks the outer transaction
        rollback-only. Catching that exception inside the outer block therefore
        cannot accidentally commit the inner block's partial mutations.
        """
        with self._lock:
            outermost = self._batch_depth == 0
            if outermost:
                self._rollback_only = False
            self._batch_depth += 1
            try:
                yield
            except BaseException:
                self._batch_depth -= 1
                self._rollback_only = True
                if outermost:
                    try:
                        if self._transaction_started:
                            self._conn.rollback()
                    finally:
                        self._transaction_started = False
                        self._rollback_only = False
                raise
            else:
                self._batch_depth -= 1
                if not outermost:
                    return
                rollback_only = self._rollback_only
                try:
                    if self._transaction_started:
                        if rollback_only:
                            self._conn.rollback()
                        else:
                            self._conn.commit()
                finally:
                    self._transaction_started = False
                    self._rollback_only = False
                if rollback_only:
                    raise RuntimeError("Cannot commit SQLite batch after nested batch failure")

    def _ensure_write_transaction(self) -> None:
        """Open the batch transaction on its first actual mutation."""
        if self._batch_depth and not self._transaction_started:
            self._conn.execute("BEGIN IMMEDIATE")
            self._transaction_started = True

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
            self._ensure_write_transaction()
            self._out_seq += 1
            self._conn.execute(
                """
                INSERT INTO outbound(
                    mid, topic, payload, qos, retain, state, dup,
                    properties, seq, logical_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mid) DO UPDATE SET
                    topic=excluded.topic, payload=excluded.payload,
                    qos=excluded.qos, retain=excluded.retain,
                    state=excluded.state, dup=excluded.dup,
                    properties=excluded.properties,
                    logical_size=excluded.logical_size
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
                    int(msg.logical_size),
                ),
            )
            self._commit_if_needed()

    def get_out(self, mid: int) -> OutboundMessage | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM outbound WHERE mid=?", (mid,)).fetchone()
        return _row_to_out(row) if row else None

    def delete_out(self, mid: int) -> bool:
        """Delete an outbound record without reading or reconstructing it."""
        with self._lock:
            self._ensure_write_transaction()
            cursor = self._conn.execute("DELETE FROM outbound WHERE mid=?", (mid,))
            self._commit_if_needed()
            return cursor.rowcount > 0

    def update_out(self, msg: OutboundMessage) -> None:
        with self._lock:
            self._ensure_write_transaction()
            cur = self._conn.execute(
                "UPDATE outbound SET state=?, dup=? WHERE mid=?",
                (int(msg.state), int(msg.dup), msg.mid),
            )
            if cur.rowcount == 0:
                raise KeyError(msg.mid)
            self._commit_if_needed()

    def out_items(self) -> Iterator[OutboundMessage]:
        for page in self.out_pages():
            yield from page

    def out_pages(self, page_size: int = 256) -> Iterator[tuple[OutboundMessage, ...]]:
        yield from self._pages("outbound", _OUT_PAGE_SQL, page_size, _row_to_out)

    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]:
        yield from self._pages("outbound", _OUT_SUMMARY_PAGE_SQL, page_size, _row_to_out_summary)

    def clear_out(self) -> None:
        with self._lock:
            self._ensure_write_transaction()
            self._conn.execute("DELETE FROM outbound")
            self._commit_if_needed()

    # --- conditional transitions (TransitionInflightStore) ------------------
    #
    # Every method here reads metadata columns only and never touches `payload`
    # or `properties`. The Python lock excludes callers sharing this instance;
    # the SQL mutation also repeats the expected state and insertion sequence,
    # so a second connection changing or replacing the row makes rowcount zero
    # rather than allowing a stale read to settle a different record.

    def _out_transition_row(self, mid: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT state, logical_size, seq FROM outbound WHERE mid=?", (mid,)
        ).fetchone()

    def set_out_logical_size(self, mid: int, logical_size: int) -> bool:
        with self._lock:
            self._ensure_write_transaction()
            cursor = self._conn.execute(
                "UPDATE outbound SET logical_size=? WHERE mid=?",
                (int(logical_size), mid),
            )
            self._commit_if_needed()
            return cursor.rowcount > 0

    def out_meta(self, mid: int) -> OutboundRecordMeta | None:
        with self._lock:
            row = self._out_transition_row(mid)
        if row is None:
            return None
        return OutboundRecordMeta(
            mid=mid,
            state=OutboundQoSState(int(row["state"])),
            logical_size=int(row["logical_size"]),
        )

    def complete_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
    ) -> OutboundRecordMeta | None:
        with self._lock:
            row = self._out_transition_row(mid)
            if row is None or int(row["state"]) != int(expected_state):
                return None
            self._ensure_write_transaction()
            cursor = self._conn.execute(
                "DELETE FROM outbound WHERE mid=? AND state=? AND seq=?",
                (mid, int(expected_state), int(row["seq"])),
            )
            if cursor.rowcount == 0:
                self._commit_if_needed()
                return None
            self._commit_if_needed()
            return OutboundRecordMeta(
                mid=mid,
                state=expected_state,
                logical_size=int(row["logical_size"]),
            )

    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> OutboundRecordMeta | None:
        with self._lock:
            row = self._out_transition_row(mid)
            if row is None or int(row["state"]) != int(expected_state):
                return None
            self._ensure_write_transaction()
            cursor = self._conn.execute(
                "UPDATE outbound SET state=? WHERE mid=? AND state=? AND seq=?",
                (int(new_state), mid, int(expected_state), int(row["seq"])),
            )
            if cursor.rowcount == 0:
                self._commit_if_needed()
                return None
            self._commit_if_needed()
            # `compact` needs no work here: the encoded PUBLISH frame is a
            # memory-only field this store has never persisted.
            return OutboundRecordMeta(
                mid=mid,
                state=new_state,
                logical_size=int(row["logical_size"]),
            )

    def put_in(self, msg: InboundMessage) -> None:
        with self._lock:
            self._ensure_write_transaction()
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
            self._ensure_write_transaction()
            self._conn.execute("DELETE FROM inbound WHERE mid=?", (mid,))
            self._commit_if_needed()
        return _row_to_in(row)

    def update_in(self, msg: InboundMessage) -> None:
        with self._lock:
            self._ensure_write_transaction()
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
        for page in self.in_pages():
            yield from page

    def in_pages(self, page_size: int = 256) -> Iterator[tuple[InboundMessage, ...]]:
        yield from self._pages("inbound", _IN_PAGE_SQL, page_size, _row_to_in)

    def clear_in(self) -> None:
        with self._lock:
            self._ensure_write_transaction()
            self._conn.execute("DELETE FROM inbound")
            self._commit_if_needed()

    # --- conditional transitions (TransitionInflightStore) ------------------

    def _in_transition_row(self, mid: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT state, user_acked, seq FROM inbound WHERE mid=?", (mid,)
        ).fetchone()

    def contains_in(self, mid: int) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM inbound WHERE mid=?", (mid,)).fetchone()
        return row is not None

    def in_meta(self, mid: int) -> InboundRecordMeta | None:
        with self._lock:
            row = self._in_transition_row(mid)
        if row is None:
            return None
        return InboundRecordMeta(
            mid=mid,
            state=InboundQoSState(int(row["state"])),
            user_acked=bool(row["user_acked"]),
        )

    def in_index_pages(self, page_size: int = 256) -> Iterator[tuple[InboundRecordMeta, ...]]:
        yield from self._pages("inbound", _IN_INDEX_PAGE_SQL, page_size, _row_to_in_meta)

    def mark_in_delivered(self, mid: int) -> bool:
        with self._lock:
            self._ensure_write_transaction()
            cursor = self._conn.execute(
                "UPDATE inbound SET delivered=1 WHERE mid=? AND delivered=0", (mid,)
            )
            self._commit_if_needed()
            return cursor.rowcount > 0

    def transition_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
        new_state: InboundQoSState,
        *,
        user_acked: bool | None = None,
    ) -> InboundRecordMeta | None:
        with self._lock:
            row = self._in_transition_row(mid)
            if row is None or int(row["state"]) != int(expected_state):
                return None
            old_user_acked = int(row["user_acked"])
            self._ensure_write_transaction()
            if user_acked is None:
                cursor = self._conn.execute(
                    """
                    UPDATE inbound SET state=?
                    WHERE mid=? AND state=? AND user_acked=? AND seq=?
                    """,
                    (
                        int(new_state),
                        mid,
                        int(expected_state),
                        old_user_acked,
                        int(row["seq"]),
                    ),
                )
                resulting_user_acked = bool(old_user_acked)
            else:
                cursor = self._conn.execute(
                    """
                    UPDATE inbound SET state=?, user_acked=?
                    WHERE mid=? AND state=? AND user_acked=? AND seq=?
                    """,
                    (
                        int(new_state),
                        int(user_acked),
                        mid,
                        int(expected_state),
                        old_user_acked,
                        int(row["seq"]),
                    ),
                )
                resulting_user_acked = user_acked
            if cursor.rowcount == 0:
                self._commit_if_needed()
                return None
            self._commit_if_needed()
            return InboundRecordMeta(
                mid=mid,
                state=new_state,
                user_acked=resulting_user_acked,
            )

    def complete_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
    ) -> InboundRecordMeta | None:
        with self._lock:
            row = self._in_transition_row(mid)
            if row is None or int(row["state"]) != int(expected_state):
                return None
            old_user_acked = int(row["user_acked"])
            self._ensure_write_transaction()
            cursor = self._conn.execute(
                """
                DELETE FROM inbound
                WHERE mid=? AND state=? AND user_acked=? AND seq=?
                """,
                (mid, int(expected_state), old_user_acked, int(row["seq"])),
            )
            if cursor.rowcount == 0:
                self._commit_if_needed()
                return None
            self._commit_if_needed()
            return InboundRecordMeta(
                mid=mid,
                state=expected_state,
                user_acked=bool(old_user_acked),
            )
