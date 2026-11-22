#!/usr/bin/env python3
"""Compiled Will store mutants, isolated from the source tree and other runs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


def execute(arguments: list[str], *, cwd: Path, env: dict[str, str], log: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(arguments, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=300, check=False)
    if log is not None:
        log.write_text(result.stdout)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--target-dir", type=Path)
    args = parser.parse_args()
    daemon = Path(__file__).resolve().parents[1]
    root = daemon.parent
    output = args.output.resolve()
    if output == root or root in output.parents:
        raise SystemExit("keep reception output outside the source tree")
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ)
    env.update(CARGO_BUILD_JOBS="1", CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0",
               CARGO_PROFILE_TEST_DEBUG="0")
    source = execute(["git", "rev-parse", "HEAD"], cwd=root, env=env)
    if source.returncode != 0:
        raise SystemExit(source.stdout)
    source_sha = source.stdout.strip()
    mutation_dir = daemon / "tests/mutants/will-store"
    cases = json.loads((mutation_dir / "expected.json").read_text())["mutations"]
    target = (args.target_dir or output / "target").resolve()
    manifest: dict[str, object] = {"source_sha": source_sha, "features": [], "results": []}
    report = output / "result.json"
    with tempfile.TemporaryDirectory(prefix="mqttiumd-will-mutants-") as temporary:
        worktree = Path(temporary) / "source"
        added = execute(["git", "worktree", "add", "--detach", str(worktree), source_sha], cwd=root, env=env)
        if added.returncode != 0:
            raise SystemExit(added.stdout)
        try:
            for case in cases:
                name, test, reason = case["name"], case["test"], case["reason"]
                command = ["cargo", "test", "-p", "mqttiumd-store-sqlite", "--test", "will", "--locked",
                           "--target-dir", str(target), test, "--", "--exact", "--nocapture"]
                control = execute(command, cwd=worktree / "mqttiumd", env=env, log=output / f"{name}.control.log")
                if control.returncode != 0:
                    raise RuntimeError(f"{name}: clean control failed; no mutation detection")
                patch = mutation_dir / f"{name}.mutant"
                applied = execute(["git", "apply", str(patch)], cwd=worktree, env=env)
                if applied.returncode != 0:
                    raise RuntimeError(f"{name}: injection not reached: {applied.stdout}")
                mutated = execute(command, cwd=worktree / "mqttiumd", env=env, log=output / f"{name}.mutated.log")
                detected = (mutated.returncode == 101 and reason in mutated.stdout
                            and "test result: FAILED" in mutated.stdout and "error[E" not in mutated.stdout)
                row = {"name": name, "test": test, "reason": reason, "detected": detected,
                       "control_exit": control.returncode, "mutated_exit": mutated.returncode,
                       "mutant_sha256": hashlib.sha256(patch.read_bytes()).hexdigest()}
                manifest["results"].append(row)
                report.write_text(json.dumps(manifest, indent=2) + "\n")
                restored = execute(["git", "restore", "mqttiumd/crates/mqttiumd-store-sqlite/src/will.rs"], cwd=worktree, env=env)
                if restored.returncode != 0:
                    raise RuntimeError("could not restore isolated mutation worktree")
                if not detected:
                    raise RuntimeError(f"{name}: expected business assertion {reason!r} not observed")
                print(f"{name}: detected {reason}", flush=True)
            manifest["status"] = "PASS"
            report.write_text(json.dumps(manifest, indent=2) + "\n")
        finally:
            removed = execute(["git", "worktree", "remove", "--force", str(worktree)], cwd=root, env=env)
            if removed.returncode != 0:
                raise RuntimeError(f"could not remove isolated worktree: {removed.stdout}")


if __name__ == "__main__":
    main()
