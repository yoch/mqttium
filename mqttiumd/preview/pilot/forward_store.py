"""Durable producer and dashboard stores for the bounded pilot."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import os
from pathlib import Path
import sqlite3
import struct
import threading
import time
import uuid

from common import CAPTEURS, DASHBOARDS, read_db, require, topic


MIB = 1024 * 1024
PRODUCER_BYTES = 64 * MIB
DASHBOARD_BYTES = 256 * MIB
PAGE_BYTES = 4096
WIRE_BYTES = 256
MAX_SEQUENCE = (1 << 63) - 2
HEADER = struct.Struct(">4s16sQqH")
BODY_BYTES = WIRE_BYTES - HEADER.size - 32
OUTBOX_ID = int.from_bytes(b"MQOB", "big")
INBOX_ID = int.from_bytes(b"MQIB", "big")
UNKNOWN = -1
QUOTA = 0x97
SUCCESS = 0
NO_MATCH = 0x10
BLOCKED_REASONS = {
    NO_MATCH: "no_matching_subscribers",
    0x80: "unspecified_error",
    0x83: "implementation_specific_error",
    0x87: "not_authorized",
    0x90: "topic_name_invalid",
    0x91: "packet_identifier_in_use",
    0x99: "payload_format_invalid",
}


class Backpressure(RuntimeError):
    """The caller retains responsibility for an enqueue that raises this."""


@dataclass(frozen=True)
class Event:
    sensor: str
    stream: bytes
    sequence: int
    created_ns: int
    wire: bytes

    @property
    def event_id(self):
        return f"{self.sensor}/{self.stream.hex()}/{self.sequence}"

    @property
    def topic(self):
        return topic(self.sensor)

    @property
    def digest(self):
        return self.wire[-32:]

    @classmethod
    def encode(cls, sensor, stream, sequence, created_ns, body):
        require(sensor in CAPTEURS and len(stream) == 16, "invalid_event_origin")
        require(0 < sequence <= MAX_SEQUENCE and created_ns > 0, "invalid_event_sequence_or_time")
        require(isinstance(body, bytes) and len(body) <= BODY_BYTES, "event_body_budget")
        content = HEADER.pack(b"Q9E1", stream, sequence, created_ns, len(body))
        content += body.ljust(BODY_BYTES, b"\x00")
        digest = hashlib.sha256(topic(sensor).encode() + b"\x00" + content).digest()
        return cls(sensor, stream, sequence, created_ns, content + digest)

    @classmethod
    def decode(cls, name, wire):
        sensor = name.split("/")[1] if name.count("/") == 2 else ""
        require(sensor in CAPTEURS and name == topic(sensor), "unexpected_telemetry_topic")
        require(len(wire) == WIRE_BYTES, "invalid_event_length")
        magic, stream, seq, created, size = HEADER.unpack_from(wire)
        require(magic == b"Q9E1" and size <= BODY_BYTES, "invalid_event_envelope")
        event = cls.encode(sensor, stream, seq, created, wire[HEADER.size:HEADER.size + size])
        require(event.wire == wire, "invalid_event_digest_or_padding")
        return event


def footprint(path):
    sizes = {}
    for key, suffix in (("database", ""), ("wal", "-wal"), ("shm", "-shm")):
        try:
            sizes[key] = Path(f"{path}{suffix}").stat().st_size
        except FileNotFoundError:
            sizes[key] = 0
    return sizes


def lease(path):
    fd = os.open(f"{path}.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


class BoundedStore:
    """One writer, bounded WAL, and a file-page ceiling enforced by SQLite."""

    def __init__(self, path, identity, budget, application_id, schema, seed):
        require(MIB <= budget <= 1024 * MIB, "invalid_sqlite_budget")
        self.path = Path(path).resolve()
        self.identity = identity
        self.lock = threading.Lock()
        self.lease = lease(self.path)
        self.db = None
        self.checkpoints = 0
        self.checkpoint_busy = 0
        self.wal_mark = min(MIB, budget // 8)
        # Reserve a main image, one transaction worth of WAL pages, an
        # existing WAL prefix, and room for the WAL index and file headers.
        reserve = budget // 64 + 128 * 1024
        self.max_pages = (budget - self.wal_mark - reserve) // (2 * PAGE_BYTES + 24)
        try:
            new = not self.path.exists() or self.path.stat().st_size == 0
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self.db = sqlite3.connect(self.path, timeout=.1, isolation_level=None,
                                      check_same_thread=False)
            self.db.row_factory = sqlite3.Row
            if not new:
                require(self.pragma("application_id") == application_id and self.pragma("user_version") == 1,
                        "unsupported_local_store_format")
                meta = self.db.execute("SELECT identity, budget FROM meta").fetchone()
                require(meta is not None and tuple(meta) == (identity, budget), "local_store_identity_or_budget_mismatch")
            self.db.execute(f"PRAGMA page_size={PAGE_BYTES}")
            require(self.pragma("page_size") == PAGE_BYTES, "incompatible_local_page_size")
            require(self.pragma("journal_mode=WAL") == "wal", "local_wal_unavailable")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA cache_size=-512")
            self.db.execute("PRAGMA cache_spill=OFF")
            self.db.execute("PRAGMA wal_autocheckpoint=0")
            require(self.pragma(f"max_page_count={self.max_pages}") == self.max_pages,
                    "local_image_exceeds_budget")
            require(sum(footprint(self.path).values()) <= budget, "local_footprint_exceeds_budget")
            if new:
                with self.transaction():
                    for statement in schema:
                        self.db.execute(statement)
                    self.db.execute(f"PRAGMA application_id={application_id}")
                    self.db.execute("PRAGMA user_version=1")
                    seed(self.db)
        except BaseException:
            self.close()
            raise

    def pragma(self, setting):
        return self.db.execute(f"PRAGMA {setting}").fetchone()[0]

    @contextmanager
    def transaction(self):
        with self.lock:
            if footprint(self.path)["wal"] >= self.wal_mark:
                busy, _, _ = self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if busy:
                    self.checkpoint_busy += 1
                    raise Backpressure("local_wal_checkpoint_busy")
                self.checkpoints += 1
            try:
                self.db.execute("BEGIN IMMEDIATE")
                yield
                self.db.execute("COMMIT")
            except BaseException as error:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
                if isinstance(error, sqlite3.OperationalError) and error.sqlite_errorcode == sqlite3.SQLITE_FULL:
                    raise Backpressure("local_disk_budget_or_filesystem_full") from error
                raise

    def metrics(self):
        with self.lock:
            return {"checkpoint_count": self.checkpoints, "checkpoint_busy": self.checkpoint_busy,
                    "synchronous": self.pragma("synchronous"), "page_limit": self.max_pages,
                    "wal_mark": self.wal_mark}

    def close(self):
        try:
            if self.db is not None:
                self.db.close()
        finally:
            os.close(self.lease)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Outbox(BoundedStore):
    def __init__(self, path, sensor, budget=PRODUCER_BYTES):
        require(sensor in CAPTEURS, "unknown_sensor")
        schema = (
            """CREATE TABLE meta (one INTEGER PRIMARY KEY CHECK(one=1), identity TEXT NOT NULL,
                 budget INTEGER NOT NULL, stream BLOB NOT NULL CHECK(length(stream)=16),
                 enqueued INTEGER NOT NULL DEFAULT 0, confirmed INTEGER NOT NULL DEFAULT 0,
                 inflight INTEGER NOT NULL DEFAULT 0, head_attempts INTEGER NOT NULL DEFAULT 0,
                 streak INTEGER NOT NULL DEFAULT 0, blocked INTEGER NOT NULL DEFAULT 0,
                 attempts INTEGER NOT NULL DEFAULT 0, retries INTEGER NOT NULL DEFAULT 0,
                 quota INTEGER NOT NULL DEFAULT 0, unknown INTEGER NOT NULL DEFAULT 0,
                 rejected INTEGER NOT NULL DEFAULT 0, no_match INTEGER NOT NULL DEFAULT 0,
                 last_reason INTEGER, last_ack_ns INTEGER, last_ack_mono INTEGER,
                 CHECK(confirmed<=enqueued))""",
            """CREATE TABLE queued (seq INTEGER PRIMARY KEY, created_ns INTEGER NOT NULL,
                 wire BLOB NOT NULL CHECK(length(wire)=256))""",
        )
        super().__init__(path, sensor, budget, OUTBOX_ID, schema,
                         lambda db: db.execute("INSERT INTO meta(one,identity,budget,stream) VALUES(1,?,?,?)",
                                               (sensor, budget, uuid.uuid4().bytes)))
        try:
            meta = self.db.execute("SELECT * FROM meta").fetchone()
            if meta["inflight"]:
                self.complete(meta["confirmed"] + 1, meta["attempts"], UNKNOWN)
        except BaseException:
            self.close()
            raise

    def enqueue(self, body):
        with self.transaction():
            seq, stream = self.db.execute("SELECT enqueued+1,stream FROM meta").fetchone()
            event = Event.encode(self.identity, stream, seq, time.time_ns(), body)
            self.db.execute("INSERT INTO queued VALUES(?,?,?)", (seq, event.created_ns, event.wire))
            self.db.execute("UPDATE meta SET enqueued=?", (seq,))
        return event

    def head(self):
        with self.lock:
            meta = self.db.execute("SELECT confirmed,enqueued,blocked,last_reason,streak FROM meta").fetchone()
            if meta["blocked"]:
                reason = meta["last_reason"]
                raise RuntimeError(f"blocked_puback_0x{reason:02x}:{BLOCKED_REASONS.get(reason, 'unexpected_reason')}")
            row = self.db.execute("SELECT wire FROM queued WHERE seq=?", (meta["confirmed"] + 1,)).fetchone()
        require((row is None) == (meta["enqueued"] == meta["confirmed"]), "outbox_fifo_gap")
        event = Event.decode(topic(self.identity), row["wire"]) if row else None
        return event, meta["streak"]

    def collect(self):
        with self.lock:
            removable = self.db.execute("SELECT 1 FROM queued WHERE seq<=(SELECT confirmed FROM meta) LIMIT 1").fetchone()
        if removable:
            with self.transaction():
                self.db.execute("DELETE FROM queued WHERE seq<=(SELECT confirmed FROM meta)")

    def begin(self, event):
        with self.transaction():
            self.db.execute("DELETE FROM queued WHERE seq<=(SELECT confirmed FROM meta)")
            row = self.db.execute("""UPDATE meta SET retries=retries+(head_attempts>0),
                 head_attempts=head_attempts+1, attempts=attempts+1, inflight=1
                 WHERE confirmed+1=? AND inflight=0 AND blocked=0 AND enqueued>=?
                 RETURNING attempts""", (event.sequence, event.sequence)).fetchone()
            require(row is not None, "invalid_outbox_attempt")
        return row[0]

    def complete(self, seq, attempt, reason):
        with self.transaction():
            current = self.db.execute("SELECT confirmed+1, attempts, inflight FROM meta").fetchone()
            require(tuple(current) == (seq, attempt, 1), "outbox_result_without_current_attempt")
            if reason == SUCCESS:
                self.db.execute("""UPDATE meta SET confirmed=?, inflight=0, head_attempts=0, streak=0,
                                last_reason=?, last_ack_ns=?, last_ack_mono=?""",
                                (seq, reason, time.time_ns(), time.monotonic_ns()))
            elif reason in (QUOTA, UNKNOWN):
                self.db.execute("""UPDATE meta SET inflight=0, streak=streak+1, last_reason=?,
                                quota=quota+?, unknown=unknown+?""", (reason, reason == QUOTA, reason == UNKNOWN))
            else:
                self.db.execute("""UPDATE meta SET inflight=0, blocked=1, last_reason=?,
                                rejected=rejected+?, no_match=no_match+?""",
                                (reason, reason != NO_MATCH, reason == NO_MATCH))


class Inbox(BoundedStore):
    def __init__(self, path, dashboard, budget=DASHBOARD_BYTES):
        require(dashboard in DASHBOARDS, "unknown_dashboard")
        schema = (
            """CREATE TABLE meta (one INTEGER PRIMARY KEY CHECK(one=1), identity TEXT NOT NULL,
                 budget INTEGER NOT NULL, subscribed INTEGER NOT NULL DEFAULT 0,
                 unique_count INTEGER NOT NULL DEFAULT 0, duplicates INTEGER NOT NULL DEFAULT 0,
                 mqtt_dup INTEGER NOT NULL DEFAULT 0, invalid INTEGER NOT NULL DEFAULT 0,
                 conflicts INTEGER NOT NULL DEFAULT 0, last_unique_mono INTEGER)""",
            """CREATE TABLE applied (sensor TEXT NOT NULL, stream BLOB NOT NULL, seq INTEGER NOT NULL,
                 wire BLOB NOT NULL CHECK(length(wire)=256), received_ns INTEGER NOT NULL,
                 received_mono INTEGER NOT NULL, PRIMARY KEY(sensor,stream,seq)) WITHOUT ROWID""",
        )
        super().__init__(path, dashboard, budget, INBOX_ID, schema,
                         lambda db: db.execute("INSERT INTO meta(one,identity,budget) VALUES(1,?,?)", (dashboard, budget)))

    def subscribed(self):
        with self.lock:
            return bool(self.db.execute("SELECT subscribed FROM meta").fetchone()[0])

    def mark_subscribed(self):
        with self.transaction():
            self.db.execute("UPDATE meta SET subscribed=1")

    def record_invalid(self):
        with self.transaction():
            self.db.execute("UPDATE meta SET invalid=invalid+1")

    def apply(self, event, dup):
        now, mono = time.time_ns(), time.monotonic_ns()
        key = (event.sensor, event.stream, event.sequence)
        with self.transaction():
            existing = self.db.execute("SELECT wire FROM applied WHERE sensor=? AND stream=? AND seq=?", key).fetchone()
            if existing is None:
                self.db.execute("INSERT INTO applied VALUES(?,?,?,?,?,?)", (*key, event.wire, now, mono))
                self.db.execute("UPDATE meta SET unique_count=unique_count+1, mqtt_dup=mqtt_dup+?, last_unique_mono=?",
                                (bool(dup), mono))
                return "applied"
            if existing["wire"] != event.wire:
                self.db.execute("UPDATE meta SET conflicts=conflicts+1, mqtt_dup=mqtt_dup+?", (bool(dup),))
                return "conflict"
            self.db.execute("UPDATE meta SET duplicates=duplicates+1, mqtt_dup=mqtt_dup+?", (bool(dup),))
            return "duplicate"


def snapshot(path):
    with read_db(path) as db:
        db.row_factory = sqlite3.Row
        stats = dict(db.execute("SELECT * FROM meta").fetchone())
        stats.pop("one")
        if "stream" in stats:
            stats["stream"] = stats["stream"].hex()
            oldest, count = db.execute("SELECT min(created_ns),count(*) FROM queued WHERE seq>?",
                                       (stats["confirmed"],)).fetchone()
            stats.update(pending=count, oldest_ns=oldest,
                         oldest_age_ms=max(0, time.time_ns() - oldest) / 1e6 if oldest else 0,
                         rows=db.execute("SELECT count(*) FROM queued").fetchone()[0])
        else:
            stats["mqtt_received"] = sum(stats[key] for key in ("unique_count", "duplicates", "invalid", "conflicts"))
        stats["page_count"] = db.execute("PRAGMA page_count").fetchone()[0]
        stats["freelist_count"] = db.execute("PRAGMA freelist_count").fetchone()[0]
    files = footprint(path)
    stats.update(files=files, occupied_bytes=sum(files.values()))
    return stats


def backup(source, target):
    source, target = Path(source).resolve(), Path(target).absolute()
    lock = lease(source)
    try:
        with read_db(source) as original:
            app = original.execute("PRAGMA application_id").fetchone()[0]
            require(app in (OUTBOX_ID, INBOX_ID), "unsupported_backup_source")
            fd = os.open(target, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            try:
                destination = sqlite3.connect(target)
                try:
                    original.backup(destination)
                    require(destination.execute("PRAGMA quick_check").fetchone()[0] == "ok", "backup_integrity_failure")
                finally:
                    destination.close()
            except BaseException:
                target.unlink()
                raise
    finally:
        os.close(lock)
