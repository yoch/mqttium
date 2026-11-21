#!/usr/bin/env python3
"""Receive two compiled pure-Owner session lifecycle controls in isolation.

These are transition tests. They do not establish network, SQLite, crash or
oracle qualification. Each positive runs the exact test before its mutation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

SOURCE = Path("mqttiumd/crates/mqttiumd-core/src/owner/sessions.rs")
CASES = (
    (
        "early-capacity",
        """            Change::Submit(flight) => {
                self.lifecycle_enabled = true;""",
        """            Change::Submit(flight) => {
                if let SessionAction::Close { guard, .. } = flight.command.action {
                    self.retire_session(guard.session)?;
                }
                self.lifecycle_enabled = true;""",
        "unknown_closure_keeps_capacity_and_resolves_the_same_operation",
        "session_capacity_released_before_close_commit",
    ),
    (
        "stale-close",
        """        if session.opened_by != opened_by
            || session
                .connection
                .is_some_and(|g| g != connection.generation)
        {
            return Ok(super::Change::Nothing);
        }
""",
        "",
        "resuming_an_infinite_session_with_zero_expiry_rebinds_before_later_closure",
        "old_session_close_rebound",
    ),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], cwd: Path, log: Path, timeout: float) -> dict:
    start = time.monotonic()
    try:
        result = subprocess.run(
            command, cwd=cwd, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=timeout, check=False,
            env={**os.environ, "CARGO_BUILD_JOBS": "1"},
        )
        output, status, expired = result.stdout, result.returncode, False
    except subprocess.TimeoutExpired as error:
        output = error.stdout or b""
        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")
        status, expired = None, True
    log.write_text(output)
    return {"command": command, "status": status, "timeout": expired,
            "seconds": time.monotonic() - start, "log": log.name}


def executable(build_log: Path) -> Path:
    for line in build_log.read_text().splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (record.get("reason") == "compiler-artifact"
                and record.get("target", {}).get("name") == "mqttiumd_core"
                and record.get("profile", {}).get("test")
                and record.get("executable")):
            return Path(record["executable"])
    raise RuntimeError("successful build did not identify its test executable")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    if output == root or root in output.parents:
        parser.error("output must be outside the source checkout")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    output.mkdir(parents=True, exist_ok=False)
    target = (args.target_dir or output.with_name(output.name + "-target")).resolve()
    worktree = output.with_name(output.name + "-worktree")
    if worktree.exists():
        parser.error("isolated worktree path already exists")
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True,
    ).strip()
    if subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"], cwd=root):
        parser.error("tracked source must be clean")
    report = {"scope": "pure Owner session lifecycle controls", "source_revision": revision,
              "features": [], "baseline": [], "mutants": [], "passed": False}
    build = ["cargo", "test", "-p", "mqttiumd-core", "--lib", "--locked",
             "--target-dir", str(target), "--no-run", "--message-format=json"]
    subprocess.run(["git", "worktree", "add", "--detach", str(worktree), revision],
                   cwd=root, check=True, stdout=subprocess.DEVNULL)
    source = worktree / SOURCE
    original = source.read_text()
    try:
        report["source_sha256"] = sha256(source)
        baseline_log = output / "baseline-build.log"
        report["baseline_build"] = run(build, worktree / "mqttiumd", baseline_log, args.timeout)
        if report["baseline_build"]["status"] != 0:
            raise RuntimeError("unmodified core did not compile")
        binary = executable(baseline_log)
        report["baseline_binary_sha256"] = sha256(binary)
        for name, _, _, suffix, reason in CASES:
            test = "owner::sessions::tests::" + suffix
            log = output / (name + "-positive.log")
            baseline = run([str(binary), test, "--exact", "--nocapture"],
                           worktree / "mqttiumd", log, args.timeout)
            baseline["test"] = test
            report["baseline"].append(baseline)
            if baseline["status"] != 0 or "1 passed;" not in log.read_text():
                raise RuntimeError(f"unmodified exact test did not pass once: {test}")
        for name, before, after, suffix, reason in CASES:
            if original.count(before) != 1:
                raise RuntimeError(f"mutation site is not unique: {name}")
            source.write_text(original.replace(before, after, 1))
            patch = subprocess.check_output(["git", "diff", "--", str(SOURCE)], cwd=worktree)
            (output / (name + ".mutant")).write_bytes(patch)
            build_log = output / (name + "-build.log")
            item = {"name": name, "test": "owner::sessions::tests::" + suffix,
                    "expected_reason": reason, "source_sha256": sha256(source)}
            item["build"] = run(build, worktree / "mqttiumd", build_log, args.timeout)
            report["mutants"].append(item)
            if item["build"]["status"] != 0:
                raise RuntimeError(f"mutant did not compile: {name}")
            binary = executable(build_log)
            item["binary_sha256"] = sha256(binary)
            test_log = output / (name + "-test.log")
            item["test_run"] = run([str(binary), item["test"], "--exact", "--nocapture"],
                                   worktree / "mqttiumd", test_log, args.timeout)
            result = test_log.read_text()
            item["detected"] = (item["test_run"]["status"] == 101
                                and "1 failed;" in result and reason in result)
            source.write_text(original)
            print(f"{name}: {'DETECTED' if item['detected'] else 'NOT DETECTED'} ({reason})", flush=True)
            if not item["detected"]:
                raise RuntimeError(f"mutant was not detected for its expected reason: {name}")
        report["passed"] = True
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
    finally:
        source.write_text(original)
        report["source_restored"] = sha256(source) == report.get("source_sha256")
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        subprocess.run(["git", "worktree", "remove", "--force", str(worktree)],
                       cwd=root, check=True, stdout=subprocess.DEVNULL)
    print(output / "result.json")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
