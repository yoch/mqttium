"""Bounded hot deduplication and immutable archives for the pilot dashboards.

Only the dashboard task mutates this store. Files are published before a FULL
SQLite transaction registers them. A second transaction reclaims hot rows.
"""
from __future__ import annotations

from itertools import chain
from pathlib import Path
import errno
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import struct
import time
import uuid
import zlib

from common import CAPTEURS, DASHBOARDS, require, topic
from forward_store import Backpressure, BoundedStore, Event, MIB, WIRE_BYTES, snapshot


ARCHIVE_ID = int.from_bytes(b"MQAR", "big")
HOT_BYTES = 32 * MIB
HOT_ROWS = 8192
SEGMENT_ROWS = 512
ARCHIVE_BYTES = 8192 * MIB
SEGMENT_LIMIT = 32768
STREAM_LIMIT = 64
RECORD = struct.Struct(f">QQ{WIRE_BYTES}s")
MANIFEST_LIMIT = 1024
STAGE = ".pending"
SCHEMA = (
    """CREATE TABLE meta (one INTEGER PRIMARY KEY CHECK(one=1), identity TEXT NOT NULL,
     budget INTEGER NOT NULL, archive_id TEXT NOT NULL, archive_budget INTEGER NOT NULL,
     hot_limit INTEGER NOT NULL, segment_rows INTEGER NOT NULL, segment_limit INTEGER NOT NULL,
     subscribed INTEGER NOT NULL DEFAULT 0, unique_count INTEGER NOT NULL DEFAULT 0,
     hot_count INTEGER NOT NULL DEFAULT 0, archived_count INTEGER NOT NULL DEFAULT 0,
     archive_charge INTEGER NOT NULL DEFAULT 0, segment_count INTEGER NOT NULL DEFAULT 0,
     stream_count INTEGER NOT NULL DEFAULT 0, duplicates INTEGER NOT NULL DEFAULT 0,
     mqtt_dup INTEGER NOT NULL DEFAULT 0, invalid INTEGER NOT NULL DEFAULT 0,
     conflicts INTEGER NOT NULL DEFAULT 0, last_unique_mono INTEGER)""",
    """CREATE TABLE streams (sensor TEXT NOT NULL, stream BLOB NOT NULL CHECK(length(stream)=16),
     archived INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(sensor,stream)) WITHOUT ROWID""",
    """CREATE TABLE applied (sensor TEXT NOT NULL, stream BLOB NOT NULL, seq INTEGER NOT NULL,
     wire BLOB NOT NULL CHECK(length(wire)=256), received_ns INTEGER NOT NULL,
     received_mono INTEGER NOT NULL, PRIMARY KEY(sensor,stream,seq)) WITHOUT ROWID""",
    """CREATE TABLE segments (sensor TEXT NOT NULL, stream BLOB NOT NULL, first INTEGER NOT NULL,
     last INTEGER NOT NULL, fingerprint BLOB NOT NULL CHECK(length(fingerprint)=32),
     charge INTEGER NOT NULL, PRIMARY KEY(sensor,stream,first)) WITHOUT ROWID""",
)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sync_file(path, data):
    with path.open("xb") as file:
        file.write(data)
        file.flush()
        os.fsync(file.fileno())


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"


def digest(data):
    return hashlib.sha256(data).digest()


def read_bounded(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as file:
        info = os.fstat(file.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= limit, "archive_invalid_file_size")
        data = file.read(limit + 1)
        require(len(data) == info.st_size, "archive_file_changed_during_read")
        return data


def segment_name(sensor, stream, first):
    return f"{sensor}-{stream.hex()}-{first:019d}"


class ArchiveInbox(BoundedStore):
    def __init__(self, path, identity, budget=HOT_BYTES, *, hot_limit=HOT_ROWS,
                 segment_rows=SEGMENT_ROWS, archive_budget=ARCHIVE_BYTES,
                 segment_limit=SEGMENT_LIMIT, cut=None):
        require(identity in DASHBOARDS, "unknown_dashboard")
        require(1 <= segment_rows <= 1024 and segment_rows <= hot_limit <= 65536
                and 1 <= segment_limit <= SEGMENT_LIMIT and 0 < archive_budget <= 1024**4,
                "invalid_archive_limits")
        self.hot_limit, self.segment_rows = hot_limit, segment_rows
        self.archive_budget, self.segment_limit = archive_budget, segment_limit
        self.cut = cut
        self.pending = None
        super().__init__(path, identity, budget, ARCHIVE_ID, SCHEMA, lambda db: db.execute(
            """INSERT INTO meta(one,identity,budget,archive_id,archive_budget,hot_limit,segment_rows,segment_limit)
            VALUES(1,?,?,?,?,?,?,?)""", (identity, budget, uuid.uuid4().hex,
                                         archive_budget, hot_limit, segment_rows, segment_limit)))
        self.archive = self.path.with_suffix(self.path.suffix + ".segments")
        try:
            meta = self.state()
            require((meta["hot_limit"], meta["segment_rows"], meta["archive_budget"], meta["segment_limit"]) ==
                    (hot_limit, segment_rows, archive_budget, segment_limit), "archive_budget_mismatch")
            self.archive_id = meta["archive_id"]
            self.recover()
        except BaseException:
            self.close()
            raise

    def state(self):
        return dict(self.db.execute("SELECT * FROM meta").fetchone())

    def subscribed(self):
        return bool(self.db.execute("SELECT subscribed FROM meta").fetchone()[0])

    def mark_subscribed(self):
        with self.transaction():
            self.db.execute("UPDATE meta SET subscribed=1")

    def record_invalid(self):
        with self.transaction():
            self.db.execute("UPDATE meta SET invalid=invalid+1")

    def apply(self, event, dup):
        key = (event.sensor, event.stream, event.sequence)
        stream = self.db.execute("SELECT archived FROM streams WHERE sensor=? AND stream=?", key[:2]).fetchone()
        hot = self.db.execute("SELECT wire FROM applied WHERE sensor=? AND stream=? AND seq=?", key).fetchone()
        existing = hot["wire"] if hot else None
        if stream and event.sequence <= stream["archived"]:
            # Even a remaining hot row cannot mask the loss of its archive.
            segment = self.db.execute("""SELECT * FROM segments WHERE sensor=? AND stream=? AND first<=?
                                      ORDER BY first DESC LIMIT 1""", key).fetchone()
            require(segment is not None and event.sequence <= segment["last"], "archive_coverage_gap")
            existing = self.read_registered(segment)[event.sequence - segment["first"]][2]
            require(hot is None or existing == hot["wire"], "archive_hot_conflict")
        with self.transaction():
            if existing is not None:
                if existing == event.wire:
                    self.db.execute("UPDATE meta SET duplicates=duplicates+1,mqtt_dup=mqtt_dup+?", (bool(dup),))
                    return "duplicate"
                self.db.execute("UPDATE meta SET conflicts=conflicts+1,mqtt_dup=mqtt_dup+?", (bool(dup),))
                return "conflict"
            meta = self.state()
            if meta["hot_count"] == self.hot_limit:
                raise Backpressure("hot_row_budget_exhausted")
            require(meta["hot_count"] < self.hot_limit, "hot_counter_invalid")
            if stream is None:
                if meta["stream_count"] >= STREAM_LIMIT:
                    raise Backpressure("stream_budget_exhausted")
                self.db.execute("INSERT INTO streams(sensor,stream) VALUES(?,?)", key[:2])
            now, mono = time.time_ns(), time.monotonic_ns()
            self.db.execute("INSERT INTO applied VALUES(?,?,?,?,?,?)", (*key, event.wire, now, mono))
            self.db.execute("""UPDATE meta SET unique_count=unique_count+1,hot_count=hot_count+1,
                              stream_count=stream_count+?,mqtt_dup=mqtt_dup+?,last_unique_mono=?""",
                            (stream is None, bool(dup), mono))
            return "applied"

    def batch(self, final):
        for sensor, stream, after in self.db.execute("SELECT * FROM streams ORDER BY sensor,stream"):
            first, last, count = self.db.execute("""
                SELECT min(seq),max(seq),count(*) FROM (
                  SELECT seq FROM applied WHERE sensor=? AND stream=? AND seq>?
                  ORDER BY seq LIMIT ?)""", (sensor, stream, after, self.segment_rows)).fetchone()
            if count == self.segment_rows and first == after + 1 and last == after + count:
                return sensor, stream, first, last
            if final and first == after + 1:
                last = after
                for seq, in self.db.execute("""SELECT seq FROM applied WHERE sensor=? AND stream=? AND seq>?
                                               ORDER BY seq LIMIT ?""", (sensor, stream, after, self.segment_rows)):
                    if seq != last + 1:
                        break
                    last = seq
                return sensor, stream, first, last
        return None

    def hot_rows(self, sensor, stream, first, last):
        return [tuple(row) for row in self.db.execute("""SELECT received_ns,received_mono,wire FROM applied
                 WHERE sensor=? AND stream=? AND seq BETWEEN ? AND ? ORDER BY seq""", (sensor, stream, first, last))]

    def encode_segment(self, sensor, stream, first, rows):
        raw = b"".join(RECORD.pack(*row) for row in rows)
        data = zlib.compress(raw, level=3)
        manifest = canonical({"version": 1, "archive": self.archive_id, "sensor": sensor, "stream": stream.hex(),
                              "first": first, "last": first + len(rows) - 1, "records": len(rows),
                              "data_bytes": len(data), "data_sha256": digest(data).hex(),
                              "record_sha256": digest(raw).hex()})
        return data, manifest

    def charge(self, data_len, manifest_len):
        # Charge two rounded files and one directory. Free space on the
        # underlying filesystem must also cover metadata and other writers.
        block = 4096
        return (1 + (data_len + block - 1) // block + (manifest_len + block - 1) // block) * block

    def inspect_segment(self, path, sensor, stream, first, last, fingerprint=None):
        require(path.is_dir() and not path.is_symlink(), "archive_missing_segment")
        count = last - first + 1
        require(1 <= count <= self.segment_rows and first > 0, "archive_segment_range_invalid")
        meta = read_bounded(path / "manifest.json", MANIFEST_LIMIT)
        require(fingerprint is None or digest(meta) == fingerprint, "archive_manifest_digest_mismatch")
        data = read_bounded(path / "events.zlib", RECORD.size * count + 1024)
        info = json.loads(meta)
        require(info == {"version": 1, "archive": self.archive_id, "sensor": sensor, "stream": stream.hex(),
                         "first": first, "last": last, "records": count, "data_bytes": len(data),
                         "data_sha256": digest(data).hex(), "record_sha256": info.get("record_sha256")},
                "archive_manifest_or_data_mismatch")
        expand = zlib.decompressobj()
        raw = expand.decompress(data, RECORD.size * count + 1)
        require(expand.eof and not expand.unused_data and len(raw) == RECORD.size * count
                and digest(raw).hex() == info["record_sha256"], "archive_record_digest_or_length_mismatch")
        rows = list(RECORD.iter_unpack(raw))
        for offset, (received, monotonic, wire) in enumerate(rows):
            event = Event.decode(topic(sensor), wire)
            require(event.stream == stream and event.sequence == first + offset and received > 0
                    and monotonic > 0, "archive_record_identity_mismatch")
        return rows, digest(meta), self.charge(len(data), len(meta))

    def read_registered(self, row):
        sensor, stream, first, last, expected, charge = row
        path = self.archive / segment_name(sensor, stream, first)
        rows, _, computed = self.inspect_segment(path, sensor, stream, first, last, expected)
        require(charge == computed, "archive_segment_charge_mismatch")
        return rows

    def compact(self, *, final=False):
        if self.pending is not None:
            self.finish_compaction()
            return True
        batch = self.batch(final)
        if batch is None:
            return False
        sensor, stream, first, last = batch
        rows = self.hot_rows(*batch)
        require(len(rows) == last - first + 1, "archive_hot_prefix_gap")
        data, manifest = self.encode_segment(sensor, stream, first, rows)
        charge = self.charge(len(data), len(manifest))
        self.reserve(charge)
        stage = self.archive / STAGE
        try:
            stage.mkdir(mode=0o700)
            sync_file(stage / "events.zlib", data)
            if self.cut:
                self.cut("archive_data_synced")
            sync_file(stage / "manifest.json", manifest)
            sync_directory(stage)
            checked, fingerprint, verified_charge = self.inspect_segment(stage, *batch, digest(manifest))
            require(checked == rows and verified_charge == charge, "archive_stage_verification_failed")
            target = self.archive / segment_name(sensor, stream, first)
            require(not target.exists(), "archive_unregistered_segment_exists")
            stage.rename(target)
            self.pending = (batch, rows, fingerprint, charge)
            self.finish_compaction(published=True)
        except OSError as error:
            if error.errno not in (errno.ENOSPC, errno.EDQUOT):
                raise
            if self.pending is None and stage.exists():
                shutil.rmtree(stage)
                sync_directory(self.archive)
            raise Backpressure("archive_filesystem_capacity_exhausted") from error
        return True

    def finish_compaction(self, *, published=False):
        batch, rows, fingerprint, charge = self.pending
        sensor, stream, first, last = batch
        sync_directory(self.archive)
        if published and self.cut:
            self.cut("segment_published")
        registered = self.db.execute("SELECT * FROM segments WHERE sensor=? AND stream=? AND first=?",
                                     (sensor, stream, first)).fetchone()
        if registered is None:
            self.register(batch, fingerprint, charge)
            if self.cut:
                self.cut("segment_registered")
        else:
            require(registered["last"] == last and registered["fingerprint"] == fingerprint,
                    "archive_pending_registration_mismatch")
        self.collect(batch, rows)
        self.pending = None

    def reserve(self, charge):
        meta = self.state()
        if (meta["archive_charge"] + charge > self.archive_budget
                or meta["segment_count"] == self.segment_limit):
            raise Backpressure("archive_budget_exhausted")

    def register(self, batch, fingerprint, charge):
        sensor, stream, first, last = batch
        with self.transaction():
            self.reserve(charge)
            cursor = self.db.execute("UPDATE streams SET archived=? WHERE sensor=? AND stream=? AND archived=?",
                                     (last, sensor, stream, first - 1))
            require(cursor.rowcount == 1, "archive_frontier_changed")
            self.db.execute("INSERT INTO segments VALUES(?,?,?,?,?,?)", (*batch, fingerprint, charge))
            self.db.execute("""UPDATE meta SET archived_count=archived_count+?,archive_charge=archive_charge+?,
                             segment_count=segment_count+1""", (last - first + 1, charge))

    def collect(self, batch, rows):
        hot = self.hot_rows(*batch)
        if not hot:
            return
        require(hot == rows, "archive_collect_without_exact_coverage")
        sensor, stream, first, last = batch
        with self.transaction():
            require(self.db.execute("SELECT last FROM segments WHERE sensor=? AND stream=? AND first=?",
                                    (sensor, stream, first)).fetchone()[0] == last, "archive_collect_unregistered")
            self.db.execute("DELETE FROM applied WHERE sensor=? AND stream=? AND seq BETWEEN ? AND ?", batch)
            self.db.execute("UPDATE meta SET hot_count=hot_count-?", (len(hot),))

    def recover(self):
        meta = self.state()
        if not self.archive.exists():
            require(not meta["segment_count"], "archive_missing_directory")
            self.archive.mkdir(mode=0o700)
            sync_directory(self.archive.parent)
        require(self.archive.is_dir() and not self.archive.is_symlink(), "archive_invalid_directory")
        streams = {(s, st): n for s, st, n in self.db.execute("SELECT * FROM streams LIMIT ?", (STREAM_LIMIT + 1,))}
        require(len(streams) == meta["stream_count"] <= STREAM_LIMIT, "archive_stream_count_invalid")
        known, progress, charge, total = set(), {}, 0, 0
        catalog = self.db.execute("SELECT * FROM segments ORDER BY sensor,stream,first LIMIT ?",
                                  (self.segment_limit + 1,)).fetchall()
        for row in catalog:
            sensor, stream, first, last, _, cost = row
            key = (sensor, stream)
            require(first == progress.get(key, 0) + 1 and last <= streams.get(key, -1)
                    and len(known) < self.segment_limit, "archive_catalog_not_contiguous")
            rows = self.read_registered(row)
            self.collect((sensor, stream, first, last), rows)
            progress[key] = last
            known.add(segment_name(*key, first))
            charge += cost
            total += last - first + 1
        require(all(progress.get(key, 0) == through for key, through in streams.items())
                and len(known) == meta["segment_count"] and charge == meta["archive_charge"] <= self.archive_budget
                and total == meta["archived_count"], "archive_catalog_accounting")
        extra = None
        with os.scandir(self.archive) as entries:
            for index, entry in enumerate(entries):
                require(index < self.segment_limit + 2 and entry.is_dir(follow_symlinks=False),
                        "archive_excess_or_invalid_entry")
                if entry.name == STAGE:
                    require(extra is None, "archive_multiple_publication_stages")
                    extra = entry.name
                elif entry.name not in known:
                    require(extra is None, "archive_multiple_publication_stages")
                    extra = entry.name
        if extra == STAGE:
            shutil.rmtree(self.archive / STAGE)
            sync_directory(self.archive)
        elif extra is not None:
            path = self.archive / extra
            info = json.loads(read_bounded(path / "manifest.json", MANIFEST_LIMIT))
            batch = (info["sensor"], bytes.fromhex(info["stream"]), info["first"], info["last"])
            require(batch[:2] in streams and batch[2] == streams[batch[:2]] + 1
                    and extra == segment_name(*batch[:3]), "archive_unrecognized_published_segment")
            rows, finger, cost = self.inspect_segment(path, *batch)
            require(rows == self.hot_rows(*batch), "archive_orphan_without_hot_authority")
            sync_directory(self.archive)
            self.register(batch, finger, cost)
            self.collect(batch, rows)
        current = self.state()
        hot = self.db.execute("SELECT count(*) FROM (SELECT seq FROM applied LIMIT ?)",
                              (self.hot_limit + 1,)).fetchone()[0]
        require(hot == current["hot_count"] <= self.hot_limit and hot + current["archived_count"] ==
                current["unique_count"], "archive_hot_accounting")

    def records(self):
        """Stream all verified archived records and the hot tail in identity order."""
        for sensor, stream, after in self.db.execute("SELECT * FROM streams ORDER BY sensor,stream"):
            archived = ((row["first"] + index, record)
                        for row in self.db.execute("SELECT * FROM segments WHERE sensor=? AND stream=? ORDER BY first",
                                                   (sensor, stream))
                        for index, record in enumerate(self.read_registered(row)))
            hot = ((seq, (ns, mono, wire)) for seq, ns, mono, wire in self.db.execute(
                """SELECT seq,received_ns,received_mono,wire FROM applied WHERE sensor=? AND stream=? AND seq>?
                   ORDER BY seq""", (sensor, stream, after)))
            for seq, (ns, mono, wire) in chain(archived, hot):
                yield sensor, stream, seq, wire, ns, mono


def archive_options(args):
    return {name: getattr(args, name) for name in ("hot_limit", "segment_rows", "archive_budget", "segment_limit")}


def archive_args(parser):
    parser.add_argument("--hot-limit", type=int, default=HOT_ROWS)
    parser.add_argument("--segment-rows", type=int, default=SEGMENT_ROWS)
    parser.add_argument("--archive-budget", type=int, default=ARCHIVE_BYTES)
    parser.add_argument("--segment-limit", type=int, default=SEGMENT_LIMIT)


def open_existing(path):
    stats = snapshot(path)
    return ArchiveInbox(path, stats["identity"], stats["budget"],
                        **{k: stats[k] for k in ("hot_limit", "segment_rows", "archive_budget", "segment_limit")})
