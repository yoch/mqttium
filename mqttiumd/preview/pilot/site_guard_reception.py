#!/usr/bin/env python3
"""Discriminate dangerous recovery, session loss and cold data damage."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from common import PACKAGE, require, wait_for, write
from operate import request, schedule
from rc_reception import CLIENT, cleanup, ports
from site_health import probe
from site_ops import CONSUMERS, DEFAULTS, PRODUCERS, Site, init_site, tls_context
import site_backup


def exact_refusal(task, expected):
    try:
        task()
    except ValueError as error:
        require(str(error) == expected, f"wrong_refusal:{error}")
        return str(error)
    raise ValueError(f"guard_not_enforced:{expected}")


def supervise(site, log):
    argv = [sys.executable, str(CLIENT), "--package", str(site.package), "run", str(site.root)]
    with log.open("xb") as target:
        proc = subprocess.Popen(argv, stdout=target, stderr=target)
    def running():
        require(proc.poll() is None, f"guard_site_exited:{proc.returncode}")
        data = site.status()
        return data.get("pid") == proc.pid and data["phase"] == "running" and "brokers" in data["metrics"]
    try:
        wait_for(running, "guard_site_no_readiness", 25)
        return proc
    except Exception:
        cleanup(proc)
        raise


def generate(site, additional, total):
    schedule(site, additional, 50)
    def applied():
        hist = site.status()["metrics"]["history"]
        return hist["confirmed"] == total and all(
            ledger["unique_count"] == total for ledger in hist["ledgers"].values())
    wait_for(applied, "guard_fixture_no_durable_deliveries", 20)


def missing_store(site, output):
    path = site.db("broker-live")
    displaced = path.with_suffix(".sqlite.held")
    path.rename(displaced)
    try:
        args = [sys.executable, str(CLIENT), "--package", str(site.package), "run", str(site.root)]
        result = subprocess.run(args, text=True, capture_output=True, timeout=10, check=False)
        require(result.returncode == 1 and "No such file" in result.stdout and not path.exists(),
                "supervisor_recreated_missing_database")
        (output / "missing-store.log").write_text(result.stdout + result.stderr)
    finally:
        displaced.rename(path)
    return {"recreated": False, "exit": result.returncode}


def crash_guards(site):
    consumer1, consumer2 = CONSUMERS
    request(site, "pause", consumer1)
    # Explicit test-only MQTT clean start with the owning certificate. A real
    # rollout must not do this to a history identity.
    probe(tls_context(site, "history", "dashboard-1"), site.config["ports"]["history"], "dashboard-1")
    request(site, "resume", consumer1)
    def session_lost():
        status = site.status()
        flag = status["actors"][consumer1]["suspended"]
        if flag:
            require(flag == "history_session_missing", "missing_session_not_typed")
            require(any(a["role"] == consumer1 and a["severity"] == "critical"
                        for a in status["metrics"]["suspended_services"]), "session_loss_not_alerted")
            require(all(status["actors"][sensor]["pid"] is None for sensor in PRODUCERS),
                    "producer_admission_remains_open_after_history_session_loss")
            return status["actors"][consumer1]
        return None
    lost = wait_for(session_lost, "consumer_did_not_suspend_on_session_loss", 12)
    exact_refusal(lambda: request(site, "resume", consumer1), "component_suspended_reconcile_offline")

    # Corrupt one generated immutable segment at rest on a separate consumer.
    # Its original bytes are retained and restored at the end of the witness.
    archive = Path(str(site.db(consumer2)) + ".segments")
    file = next(archive.glob("*/events.zlib"))
    original = file.read_bytes()
    file.write_bytes(bytes([original[0] ^ 1]) + original[1:])
    try:
        def cold_failure():
            status = site.status()
            if status["actors"][consumer2]["suspended"]:
                require(any(a["cause"] == "archive_integrity_unverifiable" and a["role"] == consumer2
                            for a in status["metrics"]["alerts"]), "cold_integrity_not_alerted")
                return status["actors"][consumer2]
            return None
        damaged = wait_for(cold_failure, "corrupt_archive_not_suspended", 15)
    finally:
        file.write_bytes(original)
    return {"missing_session": lost, "damaged_archive": damaged}


def run(output, package):
    output.mkdir(mode=0o700)
    with tempfile.TemporaryDirectory(prefix="pilot-guard-reception-", dir=output.parent) as temp:
        root = Path(temp)
        site = init_site(root / "site", package, ports(), None, demo=True, budgets={**DEFAULTS, "segment_rows": 32})
        absent = missing_store(site, output)
        first = supervise(site, output / "primary.log")
        try:
            generate(site, 320, 320)
            request(site, "stop")
            require(first.wait(15) == 0, "guard_first_stop")
        finally:
            cleanup(first)
        bundle = root / "older"
        site_backup.backup(site, bundle)
        second = supervise(site, output / "changed.log")
        try:
            generate(site, 96, 416)
            request(site, "stop")
            require(second.wait(15) == 0, "guard_second_stop")
        finally:
            cleanup(second)
        restore = root / "restore"
        site_backup.restore(bundle, restore, site.credentials, package)
        copy = Site(restore, package)
        denied = exact_refusal(lambda: site_backup.handoff(site, copy), "backup_stale_or_source_changed_reconcile")
        require(not (site.root / "retired.json").exists(), "stale_handoff_fenced_source")
        exact_refusal(copy.allow_run, "restored_site_requires_handoff")
        third = supervise(site, output / "suspension.log")
        try:
            suspended = crash_guards(site)
            request(site, "stop")
            require(third.wait(20) == 0, "guard_third_stop")
        finally:
            cleanup(third)
    write(output / "result.json", {"status": "PASS", "missing_store": absent, "late_copy": denied,
                                   "closed_guards": suspended, "source_fence_on_refused_restore": False})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    args = parser.parse_args()
    os.umask(0o077)
    run(args.output, args.package)


if __name__ == "__main__":
    main()
