#!/usr/bin/env python3
"""Check that the outbound QoS 2 regressions reject both isolated faults."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile


# (name, test, source file, anchor, mutation, the assertion that must fail).
# Each anchor names one site of the product. The window of a connection is
# the count the delivery table keeps (`Deliveries::active`); the first
# mutation takes out of it the deliveries that await PUBCOMP.
CASES = (
    ("early-window", "qos2_keeps_window_and_identifier_until_pubcomp_finish_is_known",
     "deliveries.rs",
     "        self.by_window\n            .get(&generation)\n            .map_or(0, SmallIndexSet::len)\n",
     "        self.by_window.get(&generation).map_or(0, |set| {\n"
     "            set.iter().filter(|&index| !self.rows[index].awaits_pubcomp()).count()\n"
     "        })\n",
     "qos2_receive_maximum_released_before_pubcomp"),
    ("wrong-replay-leg", "recovered_qos2_phase_selects_pubrel_even_with_publish_bytes_available",
     "pubsub.rs",
     "if claim.await_pubcomp {\n            let effect = draft.emit",
     "if claim.await_pubcomp && delivery.claim_result.is_some() {\n            let effect = draft.emit",
     "qos2_recovery_replayed_publish_instead_of_pubrel"),
)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output, target = args.output.resolve(), args.target.resolve()
    output.mkdir()
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    receipts = {}
    for name, test, source_file, old, mutant, expected in CASES:
        with tempfile.TemporaryDirectory(prefix=f"mqttiumd-qos2-{name}-") as temp:
            with subprocess.Popen(["git", "archive", source, "mqttiumd"], cwd=root,
                                  stdout=subprocess.PIPE) as archive:
                with tarfile.open(fileobj=archive.stdout, mode="r|") as stream:
                    stream.extractall(temp, filter="data")
                if archive.wait() != 0:
                    raise RuntimeError("source_archive_failed")
            base = Path(temp) / "mqttiumd"
            path = base / "crates/mqttiumd-core/src/owner" / source_file
            original = path.read_text()
            if original.count(old) != 1:
                raise RuntimeError(f"{name}: expected one mutation anchor")
            path.write_text(original.replace(old, mutant, 1))
            result = subprocess.run(
                ["cargo", "test", "--locked", "-p", "mqttiumd-core", "--lib", "--target-dir",
                 str(target), f"owner::pubsub::tests::{test}", "--", "--exact"],
                cwd=base, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=90,
            )
            (output / f"{name}.log").write_text(result.stdout)
            if not (result.returncode == 101 and f"::{test} ... FAILED" in result.stdout
                    and expected in result.stdout and "0 passed; 1 failed" in result.stdout):
                raise RuntimeError(f"{name}: expected its precise assertion failure")
            receipts[name] = {"test": test, "failure": expected, "source_sha": source,
                              "mutated_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    (output / "receipt.json").write_text(json.dumps(receipts, indent=2) + "\n")


if __name__ == "__main__":
    main()
