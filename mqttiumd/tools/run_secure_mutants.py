#!/usr/bin/env python3
"""Run compiled security controls from isolated worktrees on loopback."""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

CRATE = "mqttiumd/crates/mqttiumd/src"
POLICY = CRATE + "/security.rs"
CASES = [
    ("chain", "invalid_client_certificate_established", POLICY,
     "let verified = self.standard.verify_client_cert(leaf, rest, now)?;",
     "let _ = now; let verified = ClientCertVerified::assertion();"),
    ("client-id", "foreign_client_id_accepted", POLICY,
     ".is_some_and(|owner| owner == &self.rules[principal.0].name)",
     ".is_some_and(|_| { let _ = principal; true })"),
    ("wildcard", "overbroad_subscription_accepted", POLICY,
     ".any(|prefix| within(filter, prefix))", ".any(|_| { let _ = filter; true })"),
    ("seal", "tls_plaintext_declared_started", CRATE + "/net/tls.rs",
     "const MUTANT_TLS_EARLY_STARTED: bool = false;", "const MUTANT_TLS_EARLY_STARTED: bool = true;"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("out", type=Path)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    out, target = args.out.resolve(), args.target.resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    env = {**os.environ, "CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0"}
    outcomes = []
    for mode, reason, location, before, after in CASES:
        with tempfile.TemporaryDirectory(prefix="mqttiumd-secure-mutant-") as temp:
            tree = Path(temp) / "source"
            subprocess.run(["git", "worktree", "add", "--detach", str(tree), source],
                           cwd=root, check=True, capture_output=True, timeout=60)
            try:
                path = tree / location
                orig = path.read_text()
                if orig.count(before) != 1:
                    raise ValueError(f"{mode} needs exactly one mutation site")
                altered = orig.replace(before, after)
                path.write_text(altered)
                patch = "".join(difflib.unified_diff(orig.splitlines(True), altered.splitlines(True),
                                      fromfile=location, tofile=location))
                (out / f"{mode}.mutant").write_text(patch)
                binary = target / "debug/mqttiumd"
                with (out / f"{mode}.build.log").open("wb") as log:
                    subprocess.run(["cargo", "build", "--locked", "-p", "mqttiumd", "--features", "test-hooks",
                                    "--target-dir", str(target)], cwd=tree / "mqttiumd", check=True, env=env,
                                   timeout=300, stdout=log, stderr=subprocess.STDOUT)
                digest = hashlib.sha256(binary.read_bytes()).hexdigest()
                command = [sys.executable, str(root / "mqttiumd/tests/system/secure_reception.py"),
                           "--mode", mode, "--binary", str(binary), "--out", str(out / mode)]
                with (out / f"{mode}.run.log").open("wb") as log:
                    result = subprocess.run(command, cwd=root / "mqttiumd", env=env,
                                            stdout=log, stderr=subprocess.STDOUT, timeout=45)
                evidence = json.loads((out / mode / "receipt.json").read_text())
                if result.returncode != 1 or evidence.get("failure", {}).get("code") != reason:
                    raise ValueError(f"{mode} did not detect {reason}: {evidence.get('failure')}")
                if evidence["binary_sha256"] != digest:
                    raise ValueError("executed mutant binary mismatch")
                outcomes.append({"site": location, "mode": mode, "compiled": True, "expected": reason,
                                 "detected": True, "binary_sha256": digest, "exit": result.returncode})
                print(mode, reason, flush=True)
            finally:
                subprocess.run(["git", "worktree", "remove", "--force", str(tree)], cwd=root,
                               check=True, capture_output=True, timeout=60)
    (out / "receipt.json").write_text(json.dumps({"status": "PASS", "source_sha": source,
                                                "mutants": outcomes}, indent=2) + "\n")


if __name__ == "__main__":
    main()
