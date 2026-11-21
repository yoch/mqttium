#!/usr/bin/env python3
"""Receive two session-store negative controls in an isolated temporary worktree.

These are compiled store regressions, not MQTT system or oracle qualifications.
The tested SHA must already contain the guarded lifecycle regression tests.
Only the temporary worktree is mutated; each patch and its exact failure survive
in OUTPUT. Cargo's target is also outside the source checkout.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], cwd: Path, **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False, **kwargs)


def checked(command: list[str], cwd: Path) -> str:
    result = run(command, cwd)
    if result.returncode:
        raise RuntimeError(f"{command!r}: {result.stdout}{result.stderr}")
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--target-dir", type=Path, help="optional persistent Cargo cache outside the source checkout")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project = Path(__file__).resolve().parents[1]
    repository = project.parent
    source_sha = checked(["git", "rev-parse", "HEAD"], repository)
    if checked(["git", "status", "--porcelain"], repository):
        raise RuntimeError("negative controls require a committed, clean source checkout")
    relative = Path("mqttiumd/crates/mqttiumd-store-sqlite/src/sessions.rs")
    controls = [
        {
            "name": "terminal-history",
            "before": '"DELETE FROM sessions WHERE id=?1"',
            "after": '"UPDATE sessions SET state=2 WHERE id=?1"',
            "test": "sqlite_session_cycles_do_not_retain_terminal_history",
            "reason": "session_terminal_history_grew",
        },
        {
            "name": "other-destination",
            "before": "WHERE o.target=?1 ORDER BY o.publication,o.idx",
            "after": "WHERE (?1 IS NOT NULL) ORDER BY o.publication,o.idx",
            "test": "loaded_termination_cancels_only_its_target_and_keeps_shared_payloads",
            "reason": "session_other_destination_cancelled",
        },
    ]
    results: list[dict[str, object]] = []
    baseline: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="mqttiumd-session-store-controls-") as directory:
        temporary = Path(directory)
        tree = temporary / "worktree"
        target = args.target_dir.resolve() if args.target_dir else temporary / "target"
        checked(["git", "worktree", "add", "--detach", str(tree), source_sha], repository)
        try:
            baseline_command = [
                "cargo", "test", "-p", "mqttiumd-store-sqlite", "--test", "session_lifecycle",
                "--locked", "--target-dir", str(target), "--", "--nocapture",
            ]
            environment = dict(os.environ, CARGO_BUILD_JOBS="1")
            clean = run(baseline_command, tree / "mqttiumd", env=environment, timeout=300)
            clean_log = output / "baseline.log"
            clean_log.write_text(clean.stdout + clean.stderr)
            baseline = {"command": baseline_command, "process_exit": clean.returncode, "log_sha256": sha256(clean_log)}
            if clean.returncode or "test result: ok." not in clean.stdout:
                raise RuntimeError(f"clean lifecycle baseline did not pass; inspect {clean_log}")
            for control in controls:
                source = tree / relative
                original = source.read_text()
                if original.count(control["before"]) != 1:
                    raise RuntimeError(f"{control['name']}: mutation anchor is not unique")
                source.write_text(original.replace(control["before"], control["after"], 1))
                patch = output / f"{control['name']}.patch"
                patch.write_text(checked(["git", "diff", "--", str(relative)], tree) + "\n")
                command = [
                    "cargo", "test", "-p", "mqttiumd-store-sqlite", "--test", "session_lifecycle",
                    "--locked", "--target-dir", str(target), control["test"], "--", "--exact", "--nocapture",
                ]
                completed = run(command, tree / "mqttiumd", env=environment, timeout=300)
                log = output / f"{control['name']}.log"
                transcript = completed.stdout + completed.stderr
                log.write_text(transcript)
                executed = re.search(r"Running tests/session_lifecycle\.rs \(([^)]+)\)", transcript)
                binary = Path(executed.group(1)) if executed else None
                detected = (
                    completed.returncode == 101
                    and "running 1 test" in transcript
                    and control["reason"] in transcript
                    and "test result: FAILED. 0 passed; 1 failed" in transcript
                    and "could not compile" not in transcript
                    and binary is not None and binary.is_file()
                )
                result = {
                    "name": control["name"], "source_sha": source_sha,
                    "command": command, "features": [], "expected_reason": control["reason"],
                    "process_exit": completed.returncode, "compiled_and_executed": executed is not None,
                    "test_binary_sha256": sha256(binary) if binary and binary.is_file() else None,
                    "patch_sha256": sha256(patch), "log_sha256": sha256(log),
                    "detected": detected,
                }
                results.append(result)
                checked(["git", "restore", "--source=HEAD", "--worktree", "--", str(relative)], tree)
                if checked(["git", "status", "--porcelain"], tree):
                    raise RuntimeError("the isolated worktree was not fully restored")
                if not detected:
                    raise RuntimeError(f"{control['name']}: exact property was not detected; inspect {log}")
        finally:
            checked(["git", "worktree", "remove", "--force", str(tree)], repository)
            summary = {
                "scope": "compiled-store-negative-controls", "source_sha": source_sha,
                "baseline": baseline,
                "controls": results,
                "original_checkout_unchanged": (
                    checked(["git", "rev-parse", "HEAD"], repository) == source_sha
                    and not checked(["git", "status", "--porcelain"], repository)
                ),
            }
            (output / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
    if not summary["original_checkout_unchanged"]:
        raise RuntimeError("source checkout changed concurrently; receive the controls again from a stable checkout")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
