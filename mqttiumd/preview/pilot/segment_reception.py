#!/usr/bin/env python3
"""Exercise archive bounds, ordering, interrupted handoffs and verified restore."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time

from common import CAPTEURS, read_db, require, sha, wait_for, write
from forward_client import body_for, emit
from forward_store import Backpressure, Event, Inbox, MIB, snapshot
from segment_admin import IMAGE, publish_copy, reconcile, restore
from segment_store import ArchiveInbox, open_existing


ARGS = dict(budget=MIB, hot_limit=32, segment_rows=16)
SENSOR = CAPTEURS[0]
DASHBOARD = "dashboard-1"


def event(sequence, *, inconsistent=False, sensor=SENSOR):
    stream = hashlib.sha256(sensor.encode()).digest()[:16]
    return Event.encode(sensor, stream, sequence, 1_700_000_000_000_000_000 + sequence * 20_000_000,
                        b"contradictory" if inconsistent else body_for(sequence))


def old_ledger(out):
    path = out / "ledger.sqlite"
    start = time.monotonic()
    with Inbox(path, DASHBOARD) as store:
        for index in range(1_000_000):
            actor = CAPTEURS[index % 8]
            sequence = index // 8 + 1
            item = Event.encode(actor, hashlib.sha256(actor.encode()).digest()[:16], sequence,
                                1_700_000_000_000_000_000 + index * 20_000_000, body_for(index))
            try:
                require(store.apply(item, False) == "applied", "legacy_apply_not_unique")
            except Backpressure as error:
                sqlite_error = error.__cause__
                state = snapshot(path)
                require(sqlite_error is not None and sqlite_error.sqlite_errorcode == sqlite3.SQLITE_FULL
                        and state["unique_count"] == index and state["page_count"] >= store.max_pages - 2
                        and store.pragma("quick_check") == "ok", "legacy_ledger_did_not_hit_configured_ceiling")
                return {"rows_committed": index, "first_refused_event_id": item.event_id,
                        "error": sqlite_error.sqlite_errorname, "elapsed_s": time.monotonic() - start,
                        "capacity": store.metrics(), "state_when_full": state, "sqlite": sqlite3.sqlite_version,
                        "source_component_sha256": sha(Path(__file__).with_name("forward_store.py"))}
        raise ValueError("legacy_ledger_did_not_saturate")


def ordering(path):
    with ArchiveInbox(path, DASHBOARD, **ARGS) as store:
        for sequence in range(2, 18):
            require(store.apply(event(sequence), False) == "applied", "out_of_order_insert")
        require(not store.compact(), "gap_archived_before_predecessor")
        require(store.state()["archived_count"] == 0, "gap_advanced_archive_frontier")
        require(store.apply(event(1), False) == "applied", "gap_fill_did_not_apply")
        require(store.compact() and store.state()["archived_count"] == 16, "filled_gap_did_not_archive_prefix")
    return {"initial_missing": 1, "hot_out_of_order": 16, "archived_before_gap_fill": 0, "archived_after_gap_fill": 16}


def rotations(out):
    path = out / "rotations.sqlite"
    measurements = []
    with ArchiveInbox(path, DASHBOARD, **ARGS) as store:
        for first in range(1, 2049, 16):
            for seq in reversed(range(first, first + 16)):
                require(store.apply(event(seq), False) == "applied", "rotation_event_not_unique")
            before = snapshot(path)
            require(store.compact(), "rotation_did_not_publish")
            after = snapshot(path)
            measurements.append({"first": first, "hot_before": before["hot_count"], **after})
        require(max(row["occupied_bytes"] for row in measurements) <= MIB
                and max(row["page_count"] for row in measurements) < store.max_pages
                and all(row["hot_count"] == 0 for row in measurements)
                and measurements[-1]["archived_count"] == 2048,
                "hot_budget_not_reused_across_archive_rotations")
        require(store.apply(event(1), True) == "duplicate", "archived_repeat_reapplied")
        require(store.apply(event(1, inconsistent=True), True) == "conflict", "archived_contradiction_not_detected")
        # Leave the final row hot so the backup has both representations.
        require(store.apply(event(2049), False) == "applied", "hot_tail_not_applied")
    bundle, target = out / "backup", out / "restored"
    recorded = publish_copy(path, bundle)
    require(recorded == restore(bundle, target), "restore_manifest_differs")
    with open_existing(target / IMAGE) as restored:
        require([seq for _, _, seq, _, _, _ in restored.records()] == list(range(1, 2050)),
                "lost_history_after_actual_restore")
        repairs = list(reconcile(restored, SENSOR, event(1).stream, 1, 2050))
        require(repairs[0]["digest"] == event(1).digest.hex() and repairs[-1] ==
                {"missing_from": 2050, "missing_through": 2050}, "repair_did_not_preserve_gap_evidence")
        require(restored.apply(event(1), True) == "duplicate", "restored_archive_reapplied_event")
    return {"rotations": measurements, "backup": recorded, "restored_records": 2049,
            "old_duplicate": "duplicate", "old_contradiction": "conflict", "repair_through": 2049}


def budgets(path):
    def pressure(action, expected):
        try:
            action()
        except Backpressure as error:
            require(str(error) == expected, "unexpected_archive_pressure_reason")
            return
        raise ValueError("archive_budget_did_not_refuse")

    # One small segment fits. The second cannot publish, and fills the warm
    # reserve with events that remain recoverable from SQLite after exit.
    with ArchiveInbox(path, DASHBOARD, **ARGS, archive_budget=12288) as store:
        for seq in range(1, 17):
            store.apply(event(seq), False)
        require(store.compact(), "exact_archive_budget_was_not_usable")
        for seq in range(17, 33):
            store.apply(event(seq), False)
        pressure(store.compact, "archive_budget_exhausted")
        for seq in range(33, 49):
            store.apply(event(seq), False)
        pressure(lambda: store.apply(event(49), False), "hot_row_budget_exhausted")
        require(store.state()["unique_count"] == 48, "quota_failure_partially_applied")
        require(len(list(store.archive.iterdir())) == 1, "budget_created_extra_segment")
    with open_existing(path) as store:
        require([row[2] for row in store.records()] == list(range(1, 49)), "quota_lost_accepted_history")
    return {"archive_limit_bytes": 12288, "hot_limit": 32, "first_refused": 49, "accepted_preserved": 48}


def damaged(path, damage):
    with ArchiveInbox(path, DASHBOARD, **ARGS) as store:
        for seq in range(1, 17):
            store.apply(event(seq), False)
        store.compact()
        segment = next(store.archive.iterdir())
        file = segment / ("manifest.json" if damage else "events.zlib")
        if damage:
            file.write_bytes(file.read_bytes().replace(b'"data_bytes":', b'"DATA_BYTES":'))
        else:
            file.unlink()
        try:
            store.apply(event(1), True)
        except (ValueError, FileNotFoundError) as error:
            require(store.state()["duplicates"] == 0 and store.state()["unique_count"] == 16,
                    "invalid_archive_confirmed_duplicate")
            require(isinstance(error, FileNotFoundError) or str(error) == "archive_manifest_digest_mismatch",
                    "archive_damage_wrong_refusal")
            return str(error)
        raise ValueError("missing_or_damaged_archive_accepted_old_identity")


def worker(path, point):
    def cut(reached):
        if reached == point:
            emit("cut_reached", point=point, pid=os.getpid())
            signal.pause()

    with ArchiveInbox(path, DASHBOARD, **ARGS, cut=cut) as store:
        for i in range(1, 17):
            store.apply(event(i), False)
        store.compact()
    raise ValueError("archive_crash_cut_missed")


def crash(out, point):
    path = out / (point + ".sqlite")
    log = out / (point + ".jsonl")
    with log.open("xb") as file:
        process = subprocess.Popen([sys.executable, __file__, "--worker", str(path), point],
                                   stdout=file, stderr=file)
    try:
        def barrier():
            require(process.poll() is None, "archive_worker_exited_before_cut")
            lines = log.read_text().splitlines()
            return json.loads(lines[0]) if lines else None

        found = wait_for(barrier, "archive_worker_cut_not_reached")
        require(found["kind"] == "cut_reached" and found["point"] == point and found["pid"] == process.pid,
                "archive_cut_identity_mismatch")
        prior = snapshot(path)
        require(prior["hot_count"] == 16, "hot_rows_removed_before_committed_archive_authority")
        registered = int(point == "segment_registered")
        require(prior["segment_count"] == registered and prior["archived_count"] == registered * 16,
                "archive_catalog_visible_at_wrong_boundary")
        archive = path.with_suffix(path.suffix + ".segments")
        require(len(list(archive.iterdir())) == 1
                and (archive / ".pending").exists() == (point == "archive_data_synced"), "archive_phase_not_observed")
    finally:
        signalled = process.poll() is None
        if signalled:
            process.kill()
        status = process.wait(timeout=5)
    require(signalled and status == -signal.SIGKILL, "archive_process_not_killed")
    with open_existing(path) as recovered:
        if point == "archive_data_synced":
            require(recovered.state()["archived_count"] == 0, "unfinished_archive_registered_after_crash")
            require(recovered.compact(), "staged_segment_did_not_retry")
        require(recovered.state()["unique_count"] == recovered.state()["archived_count"] == 16
                and recovered.state()["hot_count"] == 0 and recovered.state()["segment_count"] == 1,
                "archive_cut_lost_or_duplicated_responsibility")
        require([record[2] for record in recovered.records()] == list(range(1, 17)),
                "archive_cut_recovered_wrong_identities")
        require(recovered.apply(event(16), True) == "duplicate", "recovered_archive_reapplied_event")
    return {"observation": found, "pid": process.pid, "wait_status": status,
            "before_kill": prior, "recovered_count": 16}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path, nargs="?")
    parser.add_argument("--old-ledger", action="store_true")
    parser.add_argument("--worker", nargs=2)
    args = parser.parse_args()
    os.umask(0o077)
    if args.worker:
        worker(Path(args.worker[0]), args.worker[1])
        return
    args.out.mkdir(mode=0o700)
    if args.old_ledger:
        result = old_ledger(args.out)
    else:
        result = {"ordering": ordering(args.out / "order.sqlite"),
                  "crashes": {point: crash(args.out, point) for point in
                              ("archive_data_synced", "segment_published", "segment_registered")},
                  "budgets": budgets(args.out / "budgets.sqlite"),
                  "rotations_and_restore": rotations(args.out),
                  "missing_archive_refusal": damaged(args.out / "missing.sqlite", False),
                  "corrupt_archive_refusal": damaged(args.out / "corrupted.sqlite", True)}
    result["status"] = "PASS"
    result["implementation"] = {name: sha(Path(__file__).with_name(name))
                                for name in ("segment_reception.py", "segment_admin.py", "segment_store.py", "forward_store.py")}
    write(args.out / "result.json", result)
    print(json.dumps({"status": "PASS", "receipt": str(args.out / "result.json")}))


if __name__ == "__main__":
    main()
