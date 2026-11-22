#!/usr/bin/env python3
"""Run the two operator launcher commands from the extracted package."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace

from common import PACKAGE, event_rows, inventory, require, wait_for, write
from split_reception import independent_control, require_binary


def journal_paths(home, lane):
    return set(home.glob(lane + "-*.jsonl")) | set(home.glob(lane + "-*/events.jsonl"))


def start(site, home, lane, output, run, running):
    db = home / (lane + ".sqlite")
    prior = journal_paths(home, lane)
    log = output / f"{lane}-{run}.log"
    with log.open("xb") as stream:
        child = subprocess.Popen([sys.executable, site, "broker", home, lane, "--port", "0"],
                                 stdout=stream, stderr=stream)
    running.append(child)

    def ready():
        require(child.poll() is None, f"sealed_launcher_refused_{lane}:{log.read_text().strip()}")
        return next(((path, event["details"]) for path in journal_paths(home, lane) - prior
                     for event in event_rows(path) if event["kind"] == "ready"), None)

    journal, row = wait_for(ready, f"sealed_launcher_no_readiness_{lane}")
    port = int(row["address"].rsplit(":", 1)[1])
    proof = independent_control(SimpleNamespace(port=port), home / lane)
    require(inventory(db)["invariants_ok"] and row["sqlite_schema"] == 13, "sealed_launcher_bad_inventory")
    return {"pid": child.pid, "inode": db.stat().st_ino, "port": port,
            "journal": str(journal.relative_to(home)), "probe": proof}


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        status = process.wait(10)
    except subprocess.TimeoutExpired:
        process.kill()
        status = process.wait(5)
    return status


def check(package, output):
    require_binary(package / "mqttiumd", package)
    site = package / "pilot/split_site.py"
    steps = []
    with tempfile.TemporaryDirectory(prefix="mqttiumd-site-check-") as temp:
        home = Path(temp) / "private"
        subprocess.run([sys.executable, site, "provision", home], check=True)
        for run in range(1, 3):
            actors = []
            try:
                record = {lane: start(site, home, lane, output, run, actors) for lane in ("live", "history")}
                require(record["live"]["inode"] != record["history"]["inode"], "sealed_launcher_shared_db")
            finally:
                statuses = [stop(actor) for actor in actors]
            require(statuses == [0, 0], "sealed_launcher_shutdown_failed")
            steps.append({"brokers": record, "statuses": statuses})
    for lane in ("live", "history"):
        earlier, later = (step["brokers"][lane] for step in steps)
        require(earlier["inode"] == later["inode"] and earlier["pid"] != later["pid"]
                and earlier["journal"] != later["journal"], "sealed_launcher_did_not_reopen_same_store")
    return steps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    args = parser.parse_args()
    os.umask(0o077)
    args.output.mkdir(mode=0o700)
    steps = check(args.package, args.output)
    write(args.output / "result.json", {"status": "PASS", "starts": steps})
    print(json.dumps({"status": "PASS", "case": "sealed_site_launchers"}))


if __name__ == "__main__":
    main()
