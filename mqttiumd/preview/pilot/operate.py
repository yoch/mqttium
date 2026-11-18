#!/usr/bin/env python3
"""Operate a private mTLS LIVE/HISTORY site without an admin listener."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from common import PACKAGE, require, wait_for
from forward_store import MIB, snapshot
from site_ops import DEFAULTS, PRODUCERS, ROLES, Site, atomic_json, init_site, locked
from site_supervisor import run
import site_backup


def request(site, mode, role=None, *, timeout=45):
    token = uuid.uuid4().hex
    path = site.runtime / "request.json"
    with locked(site.runtime / "operator.lock"):
        status = site.status()
        if mode == "stop" and status["phase"] == "stopped":
            return status
        require(status["phase"] == "running"
                and 0 <= time.monotonic_ns() - status["monotonic_ns"] < 10_000_000_000,
                "no_fresh_running_supervisor")
        require(not path.exists(), "previous_operator_request_unresolved")
        atomic_json(path, {"mode": mode, "role": role, "token": token})
        def accepted():
            updated = site.status()
            receipt = updated.get("last_command") or {}
            if receipt.get("token") != token:
                return None
            require(receipt["status"] == "accepted", receipt.get("reason", "command_refused"))
            return updated if mode != "stop" or updated["phase"] == "stopped" else None
        return wait_for(accepted, "operator_command_not_acknowledged", timeout)


def schedule(site, additional, rate, *, start_ns=None):
    require(site.config["demo"], "reference_signal_requires_demo_site")
    require(1 <= additional <= 14_000_000 and 1 <= rate <= 50, "demo_schedule_budget_invalid")
    with locked(site.runtime / "operator.lock"):
        path = site.runtime / "schedule.json"
        require(not path.exists(), "site_already_has_source_schedule")
        counts = [snapshot(site.db(role))["enqueued"] for role in PRODUCERS]
        boundary = max((count - 1) * len(PRODUCERS) + i + 1 for i, count in enumerate(counts) if count) if any(counts) else 0
        now = time.monotonic_ns() + 300_000_000 if start_ns is None else start_ns
        plan = {"start_ns": now - boundary * 1_000_000_000 // rate, "rate": rate, "events": boundary + additional}
        atomic_json(path, plan)
        return plan


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--package", type=Path, default=PACKAGE)
    commands = cli.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("site", type=Path)
    credentials = init.add_mutually_exclusive_group(required=True)
    credentials.add_argument("--demo-pki", action="store_true")
    credentials.add_argument("--credentials", type=Path)
    init.add_argument("--live-port", type=int, default=8883)
    init.add_argument("--history-port", type=int, default=8884)
    init.add_argument("--archive-mib", type=int, default=DEFAULTS["archive_budget"] // MIB)
    for name in ("run", "status", "pause", "resume", "stop", "backup", "schedule-demo"):
        mode = commands.add_parser(name)
        mode.add_argument("site", type=Path)
        if name in ("pause", "resume"):
            mode.add_argument("role", choices=ROLES)
        elif name == "run":
            mode.add_argument("--observe-ordinals", nargs=2, type=int, metavar=("FIRST", "STOP"))
        elif name == "backup":
            mode.add_argument("target", type=Path)
        elif name == "schedule-demo":
            mode.add_argument("--events", type=int, required=True)
            mode.add_argument("--rate", type=int, default=50)
    restore = commands.add_parser("restore")
    restore.add_argument("bundle", type=Path)
    restore.add_argument("site", type=Path)
    restore.add_argument("--credentials", type=Path, required=True)
    transfer = commands.add_parser("handoff")
    transfer.add_argument("source", type=Path)
    transfer.add_argument("target", type=Path)
    args = cli.parse_args()
    os.umask(0o077)
    try:
        if args.command == "init":
            require(1 <= args.archive_mib <= 1024 * 1024, "invalid_site_archive_mib")
            site = init_site(args.site, args.package, {"live": args.live_port, "history": args.history_port},
                             args.credentials, demo=args.demo_pki,
                             budgets={**DEFAULTS, "archive_budget": args.archive_mib * MIB})
            result = {"site": str(site.root), "site_id": site.config["site_id"], "phase": "stopped"}
        elif args.command == "restore":
            result = site_backup.restore(args.bundle, args.site, args.credentials, args.package)
        elif args.command == "handoff":
            result = site_backup.handoff(Site(args.source, args.package), Site(args.target, args.package))
        else:
            site = Site(args.site, args.package)
            if args.command == "run":
                run(site, args.observe_ordinals)
                return 0
            elif args.command == "status":
                result = site.status()
                result["fresh"] = result["phase"] == "running" and 0 <= (
                    time.monotonic_ns() - result["monotonic_ns"]) < 10_000_000_000
                result["fenced"] = (site.root / "retired.json").exists()
            elif args.command == "schedule-demo":
                result = schedule(site, args.events, args.rate)
            elif args.command == "backup":
                request(site, "stop")
                result = site_backup.backup(site, args.target)
            else:
                result = request(site, args.command, getattr(args, "role", None))["last_command"]
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as error:
        print(json.dumps({"status": "refused", "cause": str(error)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
