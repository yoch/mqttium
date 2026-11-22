#!/usr/bin/env python3
"""Check that six isolated implementation faults fail their business assertions."""
from __future__ import annotations

import argparse
import difflib
import json
import os
from pathlib import Path
import py_compile
import shutil
import subprocess
import sys
import tempfile

from common import require, sha, write


HERE = Path(__file__).resolve().parent
CONTROLS = {
    "collect-negative": (
        "forward_store.py", "if reason == SUCCESS:", "if reason in (SUCCESS, QUOTA):",
        "test_quota_preserves_fifo_and_event_id", "negative_puback_dropped_fifo_head",
    ),
    "forget-on-reopen": (
        "forward_store.py", 'self.complete(meta["confirmed"] + 1, meta["attempts"], UNKNOWN)',
        'self.complete(meta["confirmed"] + 1, meta["attempts"], SUCCESS)',
        "test_recovery_keeps_uncertain_event", "uncertain_event_lost_on_reopen",
    ),
    "reapply-repeat": (
        "forward_store.py",
        'self.db.execute("UPDATE meta SET duplicates=duplicates+1, mqtt_dup=mqtt_dup+?", (bool(dup),))\n'
        '            return "duplicate"',
        'self.db.execute("INSERT OR REPLACE INTO applied VALUES(?,?,?,?,?,?)", (*key,event.wire,now,mono))\n'
        '            self.db.execute("UPDATE meta SET unique_count=unique_count+1")\n'
        '            return "applied"',
        "test_repeat_has_no_second_application", "duplicate_was_applied_again",
    ),
    "overwrite-conflict": (
        "forward_store.py", 'return "conflict"',
        'self.db.execute("UPDATE applied SET wire=? WHERE sensor=? AND stream=? AND seq=?", (event.wire,*key))\n'
        '                return "applied"',
        "test_conflict_cannot_replace_application", "contradictory_payload_applied",
    ),
    "ignore-disk-budget": (
        "forward_store.py", "self.max_pages = (budget - self.wal_mark - reserve) // (2 * PAGE_BYTES + 24)",
        "self.max_pages = 1_000_000", "test_local_disk_quota_preserves_prior_events_and_recovers",
        "physical_local_quota_exceeded",
    ),
    "spin-on-quota": (
        "forward_client.py", "return rng.uniform(ceiling / 2, ceiling)", "return 0.0",
        "test_real_sender_waits_between_quota_retries", "quota_retries_without_backoff",
    ),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(mode=0o700)
    results = {}
    for name, (module, old, new, test, assertion) in CONTROLS.items():
        with tempfile.TemporaryDirectory(prefix="pilot-forward-control-") as private:
            isolated = Path(private)
            for source in ("common.py", "forward_store.py", "forward_client.py", "forward_checks.py"):
                shutil.copy2(HERE / source, isolated / source)
            changed = isolated / module
            original = changed.read_text()
            require(original.count(old) == 1, f"control_edit_not_unique:{name}")
            expected_original = sha(changed)
            faulty = original.replace(old, new)
            changed.write_text(faulty)
            py_compile.compile(str(changed), doraise=True)
            code_hash = sha(changed)
            command = [sys.executable, str(isolated / "forward_checks.py"), f"ForwardChecks.{test}"]
            outcome = subprocess.run(command, cwd=isolated, capture_output=True, text=True, timeout=20)
            output = outcome.stdout + outcome.stderr
            (args.output / f"{name}.log").write_text(output)
            (args.output / f"{name}.mutant").write_text("".join(difflib.unified_diff(
                original.splitlines(keepends=True), faulty.splitlines(keepends=True), module, module)))
            require(outcome.returncode == 1 and f"FAIL: {test}" in output and "Ran 1 test" in output
                    and f" : {assertion}" in output and "FAILED (failures=1)" in output,
                    f"control_not_detected_for_expected_reason:{name}")
            require(sha(HERE / module) == expected_original, f"control_modified_reference:{name}")
            results[name] = {"status": "DETECTED", "assertion": assertion, "test": test,
                             "reference_sha256": expected_original, "mutant_sha256": code_hash,
                             "module": module, "exit_status": outcome.returncode}
            print(json.dumps({"control": name, **results[name]}), flush=True)
    write(args.output / "controls.json", {"status": "PASS", "controls": results})


if __name__ == "__main__":
    main()
