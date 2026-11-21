#!/usr/bin/env python3
"""Run deliberate faults against the real pilot checks in disposable trees."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from common import PACKAGE, require, sha, write


CONTROLS = (
    ("live_waits_for_history", "split_client.py", (
        ("        try:\n            self.live_queue.put_nowait", """        head, _ = self.head()
        if head is not None and head.sequence < event.sequence:
            self.dropped(event, "mutant_history_barrier")
            return event
        try:
            self.live_queue.put_nowait"""),
    ), "live_completeness_below_99_9_percent"),
    ("collect_before_registration", "segment_store.py", (
        ('        if published and self.cut:\n            self.cut("segment_published")',
         '''        if published and self.cut:
            with self.transaction():
                self.db.execute("DELETE FROM applied WHERE sensor=? AND stream=? AND seq BETWEEN ? AND ?", batch)
                self.db.execute("UPDATE meta SET hot_count=hot_count-?", (len(rows),))
            self.cut("segment_published")'''),
    ), "hot_rows_removed_before_committed_archive_authority"),
    ("skip_gap", "segment_store.py", (
        ("count == self.segment_rows and first == after + 1 and last == after + count", "count == self.segment_rows"),
        ("AND stream=? AND archived=?\",\n                                     (last, sensor, stream, first - 1)",
         "AND stream=?\",\n                                     (last, sensor, stream)"),
    ), "gap_archived_before_predecessor"),
    ("trust_old_id_without_archive", "segment_store.py", (
        ('existing = self.read_registered(segment)[event.sequence - segment["first"]][2]', 'existing = event.wire'),
    ), "archived_contradiction_not_detected"),
)


def one(package, out, control):
    name, target, replacements, expected = control
    folder = out / name
    folder.mkdir()
    scripts = folder / "pilot"
    shutil.copytree(Path(__file__).resolve().parent, scripts, ignore=shutil.ignore_patterns("__pycache__"))
    path = scripts / target
    original = path.read_text()
    code = original
    for old, new in replacements:
        require(code.count(old) == 1, f"mutation_location_not_unique:{name}")
        code = code.replace(old, new)
    compile(code, str(path), "exec")
    path.write_text(code)
    measured = folder / "witness"
    receipt = None
    if target == "split_client.py":
        argv = [sys.executable, str(scripts / "split_reception.py"), str(measured), "--short",
                "--package", str(package), "--catchup-limit", "60"]
    else:
        # The default narrow archive suite invokes the real SIGKILL/restore
        # commands and checks exact identities at each reached boundary.
        argv = [sys.executable, str(scripts / "segment_reception.py"), str(measured)]
    result = subprocess.run(argv, capture_output=True, text=True, timeout=90)
    (folder / "stdout").write_text(result.stdout)
    (folder / "stderr").write_text(result.stderr)
    require(result.returncode == 1, f"mutant_did_not_fail_on_witness:{name}")
    if target == "split_client.py":
        receipt = json.loads((measured / "reception.json").read_text())
        require(receipt["failure"] == f"ValueError('{expected}')" and receipt["outage"]["total"]["quota"] > 0
                and receipt["live_delivery"]["outage_events_eventually_seen_live"] < 500,
                "lane_coupling_mutant_did_not_fail_for_loss_of_freshness")
        require(any("mutant_history_barrier" in p.read_text() for p in measured.glob("producer-*.jsonl")),
                "lane_coupling_injection_not_reached")
    else:
        require(result.stderr.splitlines()[-1] == f"ValueError: {expected}", f"wrong_mutation_failure:{name}")
    return {"control": name, "compiled": True, "exit_status": result.returncode, "assertion": expected,
            "original_sha256": sha(Path(__file__).with_name(target)), "mutant_sha256": sha(path)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(mode=0o700)
    observed = [one(args.package.resolve(), args.output, control) for control in CONTROLS]
    write(args.output / "result.json", {"status": "PASS", "detected": observed})
    print(json.dumps({"status": "PASS", "detected": [x["control"] for x in observed]}))


if __name__ == "__main__":
    main()
