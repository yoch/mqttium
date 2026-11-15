#!/usr/bin/env python3
"""A semantic negative control of each fresh transaction-kill campaign.

The copied trace gives the reached FaultHit another declared store point.
The independent oracle must reject that contradiction for its declared-point
reason. This is an altered observation, not another process-crash experiment.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    parser.add_argument("out", type=Path)
    options = parser.parse_args()
    if options.out.exists():
        parser.error("negative-control directory must not exist")
    shutil.copytree(options.campaign, options.out)
    campaign = json.loads((options.out / "campaign.json").read_text())
    scenario_path = campaign["scenario"]
    if isinstance(scenario_path, dict):
        scenario_path = scenario_path["path"]
    scenario = json.loads((options.out / scenario_path).read_text())
    selected = next(
        fault for fault in scenario["faults"]
        if fault["point"] in {
            "store.commit.prepared", "store.commit.logged", "store.commit.applied"
        }
    )
    records = [json.loads(line) for line in (options.out / "trace.jsonl").read_text().splitlines()]
    hits = [record for record in records if record["kind"] == "FaultHit"
            and record["payload"].get("fault_id") == selected["id"]]
    # Older records call the binding `fault`; accept only one exact target.
    if not hits:
        hits = [record for record in records if record["kind"] == "FaultHit"
                and record["payload"].get("fault") == selected["id"]]
    if len(hits) != 1:
        sys.exit(f"expected one target FaultHit, found {len(hits)}")
    old = hits[0]["payload"]["point"]
    new = "store.commit.logged" if old == "store.commit.prepared" else "store.commit.prepared"
    hits[0]["payload"]["point"] = new
    (options.out / "trace.jsonl").write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records)
    )
    environment = dict(os.environ, PYTHONPATH="tests/oracle")
    judged = subprocess.run(
        [sys.executable, "-m", "mqttiumd_oracle", "--root", str(options.out),
         "--contracts", "contracts/v1", "judge", "--scenario", scenario_path,
         "--trace", "trace.jsonl"],
        env=environment, capture_output=True, text=True, timeout=60, check=False,
    )
    (options.out / "negative-verdict.json").write_text(judged.stdout)
    if judged.stderr:
        (options.out / "negative-stderr.log").write_text(judged.stderr)
    verdict = json.loads(judged.stdout)
    result = verdict["assertions"].get("transactional_kill_matches_recovery", {})
    reasons = " | ".join(result.get("reasons", []))
    detected = (judged.returncode == 1 and verdict["verdict"] == "FAIL"
                and result.get("status") == "FAIL" and "declared point" in reasons)
    summary = {
        "kind": "altered-observation-negative-control",
        "scenario": scenario_path, "mutation": {"FaultHit.point": {"before": old, "after": new}},
        "expected_assertion": "transactional_kill_matches_recovery",
        "expected_reason": "declared point", "detected_for_expected_reason": detected,
        "oracle_exit": judged.returncode, "assertion": result,
    }
    (options.out / "negative-control.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))
    if not detected:
        sys.exit("transactional negative control was not rejected for its expected reason")


if __name__ == "__main__":
    main()
