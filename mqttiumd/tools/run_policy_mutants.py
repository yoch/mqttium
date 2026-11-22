#!/usr/bin/env python3
"""Compile four isolated faults and require their policy-specific detections."""
import argparse
from pathlib import Path

from run_secure_mutants import receive

POLICY = "mqttiumd/crates/mqttiumd/src/security.rs"
STORE = "mqttiumd/crates/mqttiumd-store-sqlite/src/policy_transition.rs"
CASES = [
    ("reduction", "non_monotone_plan_accepted", POLICY,
     "for prefix in old {",
     "for prefix in old.iter().take(0) {"),
    ("transfer", "foreign_owner_transfer_planned", POLICY,
     '                    Some(name) if name != &previous.name => "client_id_owner_changed",\n',
     ""),
    ("atomicity", "partially_committed_authority", STORE,
     "        observe(PolicyTransitionPoint::FingerprintStaged);",
     '        tx.execute_batch("COMMIT; BEGIN IMMEDIATE")?;\n'
     "        observe(PolicyTransitionPoint::FingerprintStaged);"),
    ("business", "transition_touched_mqtt_business_rows", STORE,
     "        new.verify_in(&tx)?;",
     '        tx.execute("DELETE FROM retained", [])?;\n'
     "        new.verify_in(&tx)?;"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--fixture-binary", type=Path, required=True)
    args = parser.parse_args()
    receive(args.out, args.target, CASES, "policy_transition_reception.py",
            ("--fixture-binary", str(args.fixture_binary.resolve())))


if __name__ == "__main__":
    main()
