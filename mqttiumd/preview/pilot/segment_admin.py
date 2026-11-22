#!/usr/bin/env python3
"""Inspect, export, restore and reconcile a stopped pilot history ledger."""
from __future__ import annotations

import argparse
from contextlib import closing
from pathlib import Path
import json
import os
import stat
import sqlite3
import tempfile

from common import CAPTEURS, require, sha, topic
from forward_store import Event, MAX_SEQUENCE
from segment_store import (MANIFEST_LIMIT, RECORD, canonical, open_existing, read_bounded,
                           segment_name, sync_directory, sync_file)


BUNDLE = "mqttiumd.pilot.history-backup.v1"
IMAGE = "history.sqlite"


def summary(store):
    state = store.state()
    fields = ("identity", "archive_id", "unique_count", "archived_count", "hot_count",
              "archive_charge", "segment_count", "stream_count")
    return {key: state[key] for key in fields}


def copy_sqlite_file(origin, target, expected_size):
    fd = os.open(origin, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source, target.open("xb") as destination:
        info = os.fstat(source.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size == expected_size, "restore_sqlite_size_mismatch")
        left = expected_size
        while left:
            block = source.read(min(left, 1024 * 1024))
            require(block, "restore_sqlite_image_changed")
            destination.write(block)
            left -= len(block)
        require(not source.read(1), "restore_sqlite_image_changed")
        destination.flush()
        os.fsync(destination.fileno())


def copy_image(store, folder):
    path = folder / IMAGE
    with closing(sqlite3.connect(path)) as destination:
        store.db.backup(destination)
        require(destination.execute("PRAGMA quick_check").fetchone()[0] == "ok", "backup_sqlite_quick_check")
    os.chmod(path, 0o600)
    with path.open("rb") as image:
        os.fsync(image.fileno())
    sidecars = path.with_suffix(path.suffix + ".segments")
    sidecars.mkdir(mode=0o700)
    for row in store.db.execute("SELECT * FROM segments ORDER BY sensor,stream,first"):
        name = segment_name(row["sensor"], row["stream"], row["first"])
        target = sidecars / name
        target.mkdir(mode=0o700)
        source = store.archive / name
        count = row["last"] - row["first"] + 1
        for file, bound in (("events.zlib", count * RECORD.size + 1024), ("manifest.json", MANIFEST_LIMIT)):
            sync_file(target / file, read_bounded(source / file, bound))
        sync_directory(target)
    sync_directory(sidecars)
    copied = {"format": BUNDLE, **summary(store)}
    with open_existing(path) as verified:
        require(summary(verified) == summary(store), "backup_ledger_identity_or_counts_changed")
        require(sum(1 for _ in verified.records()) == copied["unique_count"], "backup_record_count_changed")
    for suffix in ("-shm", "-wal", ".lock"):
        sidecar = folder / (IMAGE + suffix)
        if sidecar.exists():
            require(suffix != "-wal" or sidecar.stat().st_size == 0, "backup_unexpected_wal_after_verification")
            sidecar.unlink()
    # Reopening can update SQLite headers. Seal the final bytes.
    copied["sqlite_sha256"] = sha(path)
    sync_file(folder / "MANIFEST.json", canonical(copied))
    sync_directory(folder)
    return copied


def publish_copy(source, target):
    target = target.absolute()
    require(target.parent.is_dir() and target.parent.stat().st_mode & 0o077 == 0
            and not target.exists(), "backup_requires_unused_destination_in_private_directory")
    with tempfile.TemporaryDirectory(prefix=".history-bundle-", dir=target.parent) as staging:
        path = Path(staging) / "data"
        path.mkdir(mode=0o700)
        with open_existing(source) as store:
            manifest = copy_image(store, path)
        path.rename(target)
        sync_directory(target.parent)
    return manifest


def restore(bundle, target):
    manifest = json.loads(read_bounded(bundle / "MANIFEST.json", 2048))
    image = bundle / IMAGE
    require(manifest["format"] == BUNDLE and not image.is_symlink()
            and image.is_file() and image.stat().st_size <= 512 * 1024**2
            and sha(image) == manifest["sqlite_sha256"], "restore_manifest_mismatch")
    # Operate on a private copy so verification never opens the sealed export
    # for writing and the exported SQLite digest remains reproducible.
    target = target.absolute()
    require(target.parent.is_dir() and target.parent.stat().st_mode & 0o077 == 0
            and not target.exists(), "restore_requires_unused_destination_in_private_directory")
    with tempfile.TemporaryDirectory(prefix=".history-restore-", dir=target.parent) as staging:
        path = Path(staging) / "data"
        path.mkdir(mode=0o700)
        # This bundle belongs to the operator. Validate counts, names and
        # declared file sizes before copying any immutable archive member.
        with closing(sqlite3.connect(image.as_uri() + "?mode=ro&immutable=1", uri=True)) as source:
            state = dict(zip(("archive_limit", "segments", "pages"), source.execute(
                "SELECT segment_limit, segment_count, (SELECT page_count FROM pragma_page_count) FROM meta").fetchone()))
            require(0 <= manifest["segment_count"] == state["segments"] <= state["archive_limit"] <= 32768
                    and 0 < state["pages"] <= 131072, "restore_catalog_budget_invalid")
            copy_sqlite_file(image, path / IMAGE, state["pages"] * 4096)
            require(sha(path / IMAGE) == manifest["sqlite_sha256"], "restore_copied_digest_mismatch")
            sidecars = path / (IMAGE + ".segments")
            sidecars.mkdir(mode=0o700)
            n = 0
            for sensor, stream, first, last in source.execute("SELECT sensor,stream,first,last FROM segments"):
                n += 1
                require(n <= state["segments"] and sensor in CAPTEURS and len(stream) == 16
                        and 0 < first <= last <= MAX_SEQUENCE and 1 <= last - first + 1 <= 1024,
                        "restore_segment_identity_or_count_invalid")
                name = segment_name(sensor, stream, first)
                origin = bundle / (IMAGE + ".segments") / name
                require(origin.is_dir() and not origin.is_symlink(), "restore_missing_archive_segment")
                copied = sidecars / name
                copied.mkdir(mode=0o700)
                for file, bound in (("events.zlib", (last - first + 1) * RECORD.size + 1024),
                                    ("manifest.json", MANIFEST_LIMIT)):
                    sync_file(copied / file, read_bounded(origin / file, bound))
                sync_directory(copied)
            require(n == state["segments"], "restore_catalog_count_mismatch")
            sync_directory(sidecars)
        with open_existing(path / IMAGE) as store:
            require(summary(store) == {k: manifest[k] for k in summary(store)}, "restore_ledger_identity_or_counts_changed")
            require(sum(1 for _ in store.records()) == manifest["unique_count"], "restore_record_count_changed")
        sync_directory(path)
        path.rename(target)
        sync_directory(target.parent)
    return manifest


def reconcile(store, sensor, stream, first, last):
    require(sensor in CAPTEURS and len(stream) == 16 and 0 < first <= last <= MAX_SEQUENCE
            and last - first < 65536, "repair_range_budget")
    expected = first
    for name, candidate, sequence, wire, _, _ in store.records():
        if (name, candidate) != (sensor, stream) or not first <= sequence <= last:
            continue
        if expected < sequence:
            yield {"missing_from": expected, "missing_through": sequence - 1}
        event = Event.decode(topic(sensor), wire)
        yield {"event_id": event.event_id, "digest": event.digest.hex(), "wire_hex": wire.hex()}
        expected = sequence + 1
    if expected <= last:
        yield {"missing_from": expected, "missing_through": last}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="command", required=True)
    for name in ("verify", "archive", "backup", "repair"):
        mode = modes.add_parser(name)
        mode.add_argument("database", type=Path)
        if name == "backup":
            mode.add_argument("target", type=Path)
        if name == "repair":
            mode.add_argument("--sensor", required=True)
            mode.add_argument("--stream", required=True, type=bytes.fromhex)
            mode.add_argument("--first", required=True, type=int)
            mode.add_argument("--last", required=True, type=int)
    mode = modes.add_parser("restore")
    mode.add_argument("bundle", type=Path)
    mode.add_argument("target", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    if args.command == "restore":
        print(json.dumps(restore(args.bundle.resolve(), args.target)))
    elif args.command == "backup":
        print(json.dumps(publish_copy(args.database, args.target)))
    else:
        with open_existing(args.database) as store:
            if args.command == "archive":
                while store.compact(final=True):
                    pass
            if args.command == "repair":
                for row in reconcile(store, args.sensor, args.stream, args.first, args.last):
                    print(json.dumps(row))
            else:
                actual = sum(1 for _ in store.records())
                require(actual == store.state()["unique_count"], "archive_verification_count_mismatch")
                print(json.dumps(summary(store)))


if __name__ == "__main__":
    main()
