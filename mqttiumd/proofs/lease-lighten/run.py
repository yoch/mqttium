#!/usr/bin/env python3
"""Verify the production LeaseTable::lighten and require its accounting mutant to fail."""

import os
import sys
import tempfile
from pathlib import Path

PROOFS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROOFS / "credits"))
import prooflib

HARNESS = "lighten_preserves_the_other_lease_and_accounts_for_both"


def check(proofs: Path, out: Path, mutated: bool) -> bool:
    command = ["cargo", "kani", "-p", "mqttiumd-proof-lease-lighten"]
    code, output, _ = prooflib.run_bounded(command, proofs, 300)
    out.write_text(output)
    results = prooflib.parse_kani(output)
    if not results or set(results) != {HARNESS}:
        return False
    result = results[HARNESS]
    if mutated:
        return (
            code not in (0, None)
            and result["verdict"] == "failed"
            and result["failures"] == 1
            and result["other"] == ["L1a total credits match the two live leases"]
        )
    return (
        code == 0
        and result["verdict"] == "verified"
        and result["failures"] == 0
        and result["covers"] == (8, 8)
    )


def main() -> int:
    out = Path(os.environ.get("LIGHTEN_PROOF_OUT", PROOFS / "target/proof-lease-lighten"))
    out.mkdir(parents=True, exist_ok=True)
    if not check(PROOFS, out / "control.log", False):
        print(f"LeaseTable::lighten proof failed; see {out / 'control.log'}", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory(prefix="mqttiumd-lighten-") as temporary:
        product = Path(temporary)
        prooflib.copy_sources(product, PROOFS.parent)
        leases = product / "crates/mqttiumd-core/src/leases.rs"
        body = leases.read_text()
        original = "self.restore(lighter, cost).ok()?;"
        if body.count(original) != 1:
            print("The mutant no longer identifies the restore in lighten.", file=sys.stderr)
            return 1
        leases.write_text(body.replace(original, "self.restore(lighter, held).ok()?;"))
        if not check(product / "proofs", out / "mutant.log", True):
            print(f"Accounting mutant missed its expected check; see {out / 'mutant.log'}", file=sys.stderr)
            return 1
    print(f"LeaseTable::lighten passed with eight witnesses; accounting mutant refuted. Logs: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
