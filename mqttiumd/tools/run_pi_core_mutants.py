#!/usr/bin/env python3
"""Receive three compiled pure-Owner PI mutants in an isolated worktree.

These are transition tests, distinct from the real runtime queue mutant and
from network, SQLite commit, crash or MQTT qualification evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

SOURCE = Path("mqttiumd/crates/mqttiumd-core/src/owner/pubsub.rs")
CASES = (
    (
        "MUTANT_EARLY_PI_RELEASE",
        "a_finish_committed_but_not_known_and_its_resolution_keep_the_identifier_reserved",
        "finish_unknown_freed_pi",
    ),
    (
        "MUTANT_IGNORE_OCCUPIED_PI",
        "an_unknown_claim_keeps_its_packet_identifier_unavailable",
        "occupied_unknown_pi_reused",
    ),
    (
        "MUTANT_STALE_OUTGOING_FRAME_RESULT",
        "old_finish_results_and_frame_callbacks_cannot_change_a_reused_slot",
        "stale_outgoing_frame_result_applied",
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
        output, status = result.stdout, result.returncode
        expired = False
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
    report = {"scope": "pure Owner transition mutants", "source_revision": revision,
              "features": [], "baseline": [], "mutants": [], "passed": False}
    build = ["cargo", "test", "-p", "mqttiumd-core", "--lib", "--locked",
             "--target-dir", str(target), "--no-run", "--message-format=json"]
    subprocess.run(["git", "worktree", "add", "--detach", str(worktree), revision],
                   cwd=root, check=True, stdout=subprocess.DEVNULL)
    try:
        source = worktree / SOURCE
        original = source.read_text()
        report["source_sha256"] = sha256(source)
        baseline_log = output / "baseline-build.log"
        report["baseline_build"] = run(build, worktree / "mqttiumd", baseline_log, args.timeout)
        if report["baseline_build"]["status"] != 0:
            raise RuntimeError("unmodified core did not compile")
        binary = executable(baseline_log)
        report["baseline_binary_sha256"] = sha256(binary)
        for flag, name, reason in CASES:
            test = "owner::pubsub::tests::" + name
            baseline = run([str(binary), test, "--exact", "--nocapture"],
                           worktree / "mqttiumd", output / (flag + "-positive.log"), args.timeout)
            report["baseline"].append(baseline)
            if baseline["status"] != 0:
                raise RuntimeError(f"unmodified test failed: {test}")
        for flag, name, reason in CASES:
            disabled = f"const {flag}: bool = false;"
            if original.count(disabled) != 1:
                raise RuntimeError(f"mutation site is not unique: {flag}")
            source.write_text(original.replace(disabled, f"const {flag}: bool = true;", 1))
            patch = subprocess.check_output(["git", "diff", "--", str(SOURCE)], cwd=worktree)
            (output / (flag + ".mutant")).write_bytes(patch)
            build_log = output / (flag + "-build.log")
            item = {"flag": flag, "test": "owner::pubsub::tests::" + name,
                    "expected_reason": reason, "source_sha256": sha256(source)}
            item["build"] = run(build, worktree / "mqttiumd", build_log, args.timeout)
            report["mutants"].append(item)
            if item["build"]["status"] != 0:
                raise RuntimeError(f"mutant did not compile: {flag}")
            binary = executable(build_log)
            item["binary_sha256"] = sha256(binary)
            test_log = output / (flag + "-test.log")
            item["test_run"] = run([str(binary), item["test"], "--exact", "--nocapture"],
                                   worktree / "mqttiumd", test_log, args.timeout)
            item["detected"] = (item["test_run"]["status"] == 101
                                and reason in test_log.read_text())
            print(f"{flag}: {'DETECTED' if item['detected'] else 'NOT DETECTED'} ({reason})", flush=True)
            if not item["detected"]:
                raise RuntimeError(f"mutant was not detected for its expected reason: {flag}")
        report["passed"] = True
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
    finally:
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        subprocess.run(["git", "worktree", "remove", "--force", str(worktree)],
                       cwd=root, check=True, stdout=subprocess.DEVNULL)
    print(output / "result.json")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
