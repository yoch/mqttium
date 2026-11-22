#!/usr/bin/env python3
"""Compile four kept Will controls in an isolated, exact-source worktree.

Usage: python tools/check_will_core_mutants.py NEW_OUTPUT --target-dir PRIVATE_TARGET
These are pure Owner transition tests. They are not proof that a client received
an early packet, a process-crash campaign, or independent MQTT qualification.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], cwd: Path, log: Path, timeout: float) -> dict:
    start = time.monotonic()
    environment = {**os.environ, "CARGO_BUILD_JOBS": "1", "CARGO_INCREMENTAL": "0"}
    environment.pop("CARGO_ENCODED_RUSTFLAGS", None)
    environment.pop("RUSTFLAGS", None)
    try:
        result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout, check=False,
                                env=environment)
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
                and record.get("profile", {}).get("test") and record.get("executable")):
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
    target = (args.target_dir or output.with_name(output.name + "-target")).resolve()
    if target == output or output in target.parents:
        parser.error("Cargo target must stay outside the evidence directory")
    output.mkdir(parents=True, exist_ok=False)
    worktree = output.with_name(output.name + "-worktree")
    if worktree.exists():
        parser.error("isolated worktree path already exists")
    git = lambda *arguments: subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()
    revision, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    if git("status", "--porcelain", "--untracked-files=normal"):
        parser.error("source checkout must be clean, including generated files")
    catalog = root / "mqttiumd/tests/mutants/will-core/expected.json"
    cases = json.loads(catalog.read_text())["mutants"]
    report = {"format": "mqttiumd.will-core-mutation-reception", "schema_version": 1,
              "scope": "pure Owner transition mutants", "qualification": "implementation-test-only",
              "source_revision": revision, "source_tree": tree, "features": [],
              "catalog_sha256": sha256(catalog), "runner_sha256": sha256(Path(__file__)),
              "baseline": [], "mutants": [], "passed": False, "restored": False,
              "build": {"CARGO_BUILD_JOBS": 1, "CARGO_INCREMENTAL": 0,
                        "CARGO_PROFILE_TEST_DEBUG": os.environ.get("CARGO_PROFILE_TEST_DEBUG"),
                        "target": str(target)}}
    build = ["cargo", "test", "-p", "mqttiumd-core", "--lib", "--locked",
             "--target-dir", str(target), "--no-run", "--message-format=json"]
    subprocess.run(["git", "worktree", "add", "--detach", str(worktree), revision],
                   cwd=root, check=True, stdout=subprocess.DEVNULL)
    try:
        baseline_log = output / "baseline-build.log"
        report["baseline_build"] = run(build, worktree / "mqttiumd", baseline_log, args.timeout)
        if report["baseline_build"]["status"] != 0:
            raise RuntimeError("unmodified core did not compile")
        binary = executable(baseline_log)
        report["baseline_binary_sha256"] = sha256(binary)
        for case in cases:
            baseline = run([str(binary), case["test"], "--exact", "--nocapture"],
                           worktree / "mqttiumd", output / (case["name"] + "-positive.log"), args.timeout)
            report["baseline"].append(baseline)
            if baseline["status"] != 0 or "test result: ok. 1 passed;" not in (output / baseline["log"]).read_text():
                raise RuntimeError(f"unmodified exact test failed: {case['test']}")
        for case in cases:
            patch = worktree / "mqttiumd/tests/mutants/will-core" / case["patch"]
            kept = output / case["patch"]
            kept.write_bytes(patch.read_bytes())
            subprocess.run(["git", "apply", "--check", str(patch)], cwd=worktree, check=True)
            subprocess.run(["git", "apply", str(patch)], cwd=worktree, check=True)
            item = {**case, "patch_sha256": sha256(kept), "source_sha256": sha256(worktree / case["source"])}
            report["mutants"].append(item)
            build_log = output / (case["name"] + "-build.log")
            item["build"] = run(build, worktree / "mqttiumd", build_log, args.timeout)
            if item["build"]["status"] != 0:
                raise RuntimeError(f"mutant did not compile: {case['name']}")
            binary = executable(build_log)
            item["binary_sha256"] = sha256(binary)
            test_log = output / (case["name"] + "-test.log")
            item["test_run"] = run([str(binary), case["test"], "--exact", "--nocapture"],
                                   worktree / "mqttiumd", test_log, args.timeout)
            text = test_log.read_text()
            item["detected"] = (item["test_run"]["status"] == 101
                                and case["reason"] in text
                                and "test result: FAILED. 0 passed; 1 failed;" in text)
            subprocess.run(["git", "restore", "--", case["source"]], cwd=worktree, check=True)
            status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=worktree, text=True)
            item["restored"] = not status.strip()
            if not item["detected"] or not item["restored"]:
                raise RuntimeError(f"mutant not detected/restored for exact reason: {case['name']}")
            print(f"{case['name']}: DETECTED ({case['reason']})", flush=True)
        report["passed"] = True
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
    finally:
        # Only this isolated worktree has been mutated; restore even on a failed build.
        subprocess.run(["git", "restore", "--", "mqttiumd/crates/mqttiumd-core"], cwd=worktree, check=True)
        report["tree_status_before_cleanup"] = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=worktree, text=True).strip()
        report["restored"] = not report["tree_status_before_cleanup"]
        # A shared Cargo cache can retain the last mutant executable even when
        # the restored source files have older mtimes. Invalidate this private
        # package, then rebuild and identify the exact restored baseline.
        clean = ["cargo", "clean", "-p", "mqttiumd-core", "--target-dir", str(target)]
        report["cache_invalidation"] = run(clean, worktree / "mqttiumd", output / "restore-clean.log", args.timeout)
        restored_log = output / "restored-baseline-build.log"
        report["restored_baseline_build"] = run(build, worktree / "mqttiumd", restored_log, args.timeout)
        if report["restored_baseline_build"]["status"] == 0:
            report["restored_baseline_binary_sha256"] = sha256(executable(restored_log))
        report["cache_restored"] = (report["cache_invalidation"]["status"] == 0
                                   and report.get("restored_baseline_binary_sha256") == report.get("baseline_binary_sha256"))
        report["passed"] = report["passed"] and report["restored"] and report["cache_restored"]
        subprocess.run(["git", "worktree", "remove", "--force", str(worktree)],
                       cwd=root, check=True, stdout=subprocess.DEVNULL)
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        files = sorted(path for path in output.rglob("*") if path.is_file())
        (output / "SHA256SUMS").write_text("".join(f"{sha256(path)}  {path.relative_to(output)}\n" for path in files))
    print(output / "result.json")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
