#!/usr/bin/env python3
"""Check the effective SQLite ceiling of a small outbox before it runs out."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import tempfile

from common import PACKAGE, require, write
from forward_client import body_for
from forward_store import Backpressure, MIB, Outbox, snapshot
from rc_reception import ports
from site_health import Health, capacity
from site_ops import DEFAULTS, PRODUCERS, ROLES, init_site
from site_supervisor import Child


def run(out, package):
    out.mkdir(mode=0o700)
    with tempfile.TemporaryDirectory(prefix="outbox-effective-budget-", dir=out.parent) as temp:
        site = init_site(Path(temp) / "site", package, ports(), None, demo=True,
                         budgets={**DEFAULTS, "producer_budget": MIB})
        actor = PRODUCERS[0]
        actors = {role: Child(role) for role in ROLES}
        capacity_levels = [capacity("test", n, 100, "stop") for n in (69, 70, 85, 95)]
        require([row["severity"] if row else None for row in capacity_levels] ==
                [None, "notice", "warning", "critical"], "capacity_alert_thresholds")
        health = Health(site, None)
        levels = {}
        with Outbox(site.db(actor), actor, MIB) as store:
            actors[actor].sqlite_page_limit = store.metrics()["page_limit"]
            for i in range(20000):
                try:
                    store.enqueue(body_for(i))
                except Backpressure as error:
                    require(str(error) == "local_disk_budget_or_filesystem_full", "unexpected_pressure_cause")
                    break
                state = snapshot(store.path)
                percent = 100 * (state["page_count"] - state["freelist_count"]) / actors[actor].sqlite_page_limit
                for mark in (70, 85, 95):
                    if percent >= mark and mark not in levels:
                        sample = health.collect(actors, False)
                        found = next((a for a in sample["alerts"]
                                      if a["cause"] == "outbox_page_capacity" and a["role"] == actor), None)
                        require(found is not None and found["threshold"] >= mark,
                                f"no_effective_page_warning_at_{mark}_percent")
                        levels[mark] = found
            else:
                raise ValueError("outbox_budget_did_not_refuse")
            result = snapshot(store.path)
            require(result["enqueued"] == i and result["pending"] == i
                    and result["occupied_bytes"] < MIB * .70, "pressure_witness_expected_effective_ceiling")
    write(out / "result.json", {"status": "PASS", "levels": levels, "at_refusal": result,
                               "database_page_limit": actors[actor].sqlite_page_limit,
                               "refusal": "local_disk_budget_or_filesystem_full"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--package", type=Path, default=PACKAGE)
    args = parser.parse_args()
    os.umask(0o077)
    run(args.output, args.package)


if __name__ == "__main__":
    main()
