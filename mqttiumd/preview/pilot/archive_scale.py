#!/usr/bin/env python3
"""Accelerated cold-catalog geometry, recovery and physical usage witness."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

from common import CAPTEURS, machine, require, sha, write
from forward_client import body_for
from forward_store import Backpressure, Event, MIB, snapshot
from load import distribution
from segment_admin import publish_copy, restore
from segment_store import ArchiveInbox, SEGMENT_LIMIT, open_existing, segment_name


FLOOR = 96 * MIB
CREATED = 1_700_000_000_000_000_000


def item(index, sequence):
    stream = (index + 1).to_bytes(16, "big")
    ordinal = (sequence - 1) * len(CAPTEURS) + index
    return Event.encode(CAPTEURS[index], stream, sequence, CREATED + ordinal * 20_000_000, body_for(ordinal))


def fixture_batch(store, sensor, first, size):
    """Fill one synthetic hot batch in one FULL transaction. Use the real publisher afterward."""
    records = [item(sensor, seq) for seq in range(first, first + size)]
    prefix = records[0]
    with store.transaction():
        changed = store.db.execute("INSERT OR IGNORE INTO streams(sensor,stream,archived) VALUES(?,?,0)",
                                   (prefix.sensor, prefix.stream)).rowcount
        store.db.executemany("INSERT INTO applied VALUES (?,?,?,?,?,?)",
                             ((e.sensor, e.stream, e.sequence, e.wire, e.created_ns + 2_000_000, e.sequence + 2_000_000)
                              for e in records))
        store.db.execute("UPDATE meta SET unique_count=unique_count+?,hot_count=hot_count+?,stream_count=stream_count+?",
                         (size, size, changed))
    require(store.compact(final=size < store.segment_rows), "accelerated_segment_not_published")


def files(path):
    sizes = {"directories": 0, "files": 0, "file_bytes": 0, "allocated_file_bytes": 0, "allocated_directory_bytes": 0}
    for root, dirs, filenames in os.walk(path):
        sizes["directories"] += len(dirs)
        sizes["files"] += len(filenames)
        for name in filenames:
            info = (Path(root) / name).stat()
            sizes["file_bytes"] += info.st_size
            sizes["allocated_file_bytes"] += info.st_blocks * 512
        for name in dirs:
            sizes["allocated_directory_bytes"] += (Path(root) / name).stat().st_blocks * 512
    return sizes


def time_op(function):
    start = time.perf_counter_ns()
    result = function()
    return (time.perf_counter_ns() - start) / 1e9, result


def may_continue(path):
    require(shutil.disk_usage(path).free > FLOOR, "archive_scratch_capacity_floor")


def corruptions(path):
    root = path.with_suffix(".sqlite.segments")
    first = root / segment_name(CAPTEURS[0], item(0, 1).stream, 1)
    displaced = first.with_name(first.name + ".held")
    first.rename(displaced)
    found = {}
    try:
        try:
            with open_existing(path):
                pass
        except ValueError as error:
            require(str(error) == "archive_missing_segment", "unexpected_missing_archive_error")
            found["missing"] = str(error)
        else:
            raise ValueError("missing_archive_did_not_fail_closed")
    finally:
        displaced.rename(first)
    file = first / "events.zlib"
    good = file.read_bytes()
    damaged = bytes([good[0] ^ 1]) + good[1:]
    file.write_bytes(damaged)
    try:
        try:
            with open_existing(path):
                pass
        except ValueError as error:
            require(str(error) == "archive_manifest_or_data_mismatch", "unexpected_corrupt_archive_error")
            found["altered"] = str(error)
        else:
            raise ValueError("corrupt_archive_did_not_fail_closed")
    finally:
        file.write_bytes(good)
    return found


def run(args):
    args.output.mkdir(mode=0o700)
    result = {"status": "FAIL", "platform": machine(args.output), "target_events": args.events,
              "target_segment_ceiling": args.ceiling, "fixture": "512 hot inserts per FULL SQLite transaction, real segment publication",
              "component_sha256": {name: sha(Path(__file__).with_name(name)) for name in
                                   ("archive_scale.py", "segment_store.py", "segment_admin.py", "forward_store.py")}}
    try:
        with tempfile.TemporaryDirectory(prefix="archive-geometry-", dir=args.output.parent) as directory:
            populate(args, result, Path(directory))
        result["status"] = "PASS"
    finally:
        write(args.output / "result.json", result)
    print(json.dumps({"status": result["status"], "dense_records": args.events, "segment_cap": args.ceiling}))


def populate(args, result, root):
    layout = root / "source"
    layout.mkdir(mode=0o700)
    path = layout / "input.sqlite"
    start = time.monotonic()
    frontier = [0] * len(CAPTEURS)
    with ArchiveInbox(path, "dashboard-1", segment_limit=args.ceiling) as store:
        for number in range(args.events // 512):
            if number % 32 == 0:
                may_continue(root)
            index = number % len(CAPTEURS)
            fixture_batch(store, index, frontier[index] + 1, 512)
            frontier[index] += 512
            if (number + 1) % 512 == 0:
                print(json.dumps({"phase": "dense", "events": (number + 1) * 512}), flush=True)
        require(store.state()["archived_count"] == args.events and store.state()["hot_count"] == 0, "dense_archive_count")
        result["dense"] = {"elapsed_s": time.monotonic() - start, "state": snapshot(path), "cold": files(store.archive)}
    result["dense"]["recovery_s"], recovered = time_op(lambda: open_existing(path))
    with recovered as store:
        ages = []
        for index, count in enumerate(frontier):
            for sequence in (1, count // 2, count):
                for _ in range(3):
                    begin = time.perf_counter_ns()
                    require(store.apply(item(index, sequence), True) == "duplicate", "old_archive_duplicate_not_verified")
                    ages.append(time.perf_counter_ns() - begin)
        result["dense"]["verified_duplicate_latency_ms"] = distribution(ages)
    exported = root / "export"
    result["export_s"], manifest = time_op(lambda: publish_copy(path, exported))
    require(manifest["unique_count"] == args.events, "volume_backup_count")
    # This site is generated scratch. The verified export remains; release the
    # redundant original before materializing its restored copy on this volume.
    shutil.rmtree(layout)
    restored = root / "restored"
    result["restore_s"], restored_manifest = time_op(lambda: restore(exported, restored))
    require(restored_manifest == manifest, "volume_restore_manifest")
    rep = restored / "history.sqlite"
    result["restored"] = {"manifest": restored_manifest, "cold": files(rep.with_suffix(".sqlite.segments"))}
    shutil.rmtree(exported)
    result["corruption"] = corruptions(rep)
    begin = time.monotonic()
    with open_existing(rep) as store:
        done = store.state()["segment_count"]
        for number in range(done, args.ceiling):
            if number % 128 == 0:
                may_continue(root)
            index = number % len(CAPTEURS)
            fixture_batch(store, index, frontier[index] + 1, 1)
            frontier[index] += 1
            if (number + 1) % 4096 == 0:
                print(json.dumps({"phase": "catalog", "segments": number + 1}), flush=True)
        require(store.state()["segment_count"] == args.ceiling, "catalog_never_filled")
        extra = item(0, frontier[0] + 1)
        require(store.apply(extra, False) == "applied", "warm_reserve_not_available")
        try:
            store.compact(final=True)
        except Backpressure as error:
            cause = str(error)
        else:
            raise ValueError("segment_ceiling_not_enforced")
        state = snapshot(rep)
        require(state["segment_count"] == args.ceiling and state["hot_count"] == 1
                and state["unique_count"] == sum(frontier) + 1, "catalog_backpressure_lost_an_event")
        result["ceiling"] = {"elapsed_s": time.monotonic() - begin, "refusal": cause,
                             "state": state, "cold": files(store.archive)}
    seconds, store = time_op(lambda: open_existing(rep))
    with store:
        require(store.apply(extra, True) == "duplicate", "held_hot_event_lost_on_reopen")
        result["ceiling"]["recovery_s"] = seconds


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--events", type=int, default=2_097_152)
    parser.add_argument("--ceiling", type=int, default=SEGMENT_LIMIT)
    args = parser.parse_args()
    require(args.events >= 4096 and args.events % (512 * 8) == 0
            and args.events // 512 < args.ceiling <= SEGMENT_LIMIT, "archive_geometry_budget_invalid")
    os.umask(0o077)
    run(args)


if __name__ == "__main__":
    main()
