#!/usr/bin/env python3
"""Run reproducible, sharded runtime-fuzzer campaigns without changing the target."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


CAMPAIGN_SCHEMA = "mqttium-runtime-campaign-v1"
FUZZER_SCHEMA = "mqttium-runtime-fuzz-v1"
SCHEDULING_ACTORS = {"checkpoint", "schedule", "factory", "effect"}
SCHEDULING_ACTIONS = {
    ("callback", "block_once"),
    ("callback", "cancel_once"),
    ("transport", "fail_active_write"),
    ("app", "cancel_last"),
}
FAMILIES = (
    "writer_failure_reader_admission",
    "callback_cancelled_error",
    "explicit_reconnect_epoch_replacement",
    "automatic_reconnect",
    "callback_lifecycle",
    "effectpump_failure_window",
)


@dataclass(frozen=True)
class Shard:
    shard_id: int
    seed_start: int
    seed_count: int


def _run(command: list[str], *, cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def _read_first(prefix: str, path: Path) -> str | None:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith(prefix):
                return line.removeprefix(prefix).strip()
    except OSError:
        pass
    return None


def _ram_bytes() -> int | None:
    value = _read_first("MemTotal:", Path("/proc/meminfo"))
    if value is None:
        return None
    return int(value.split()[0]) * 1024


def _cpu_model() -> str:
    for prefix in ("model name", "Model"):
        value = _read_first(f"{prefix}\t:", Path("/proc/cpuinfo"))
        if value:
            return value
    return platform.processor() or "unknown"


def _dependency_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in ("mqttium", "hypothesis", "pytest", "pytest-asyncio"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "not-installed"
    return versions


def _environment(runner_kind: str) -> dict[str, Any]:
    loop = asyncio.new_event_loop()
    try:
        loop_name = f"{type(loop).__module__}.{type(loop).__name__}"
    finally:
        loop.close()
    return {
        "architecture": platform.machine(),
        "asyncio_event_loop": loop_name,
        "core_count": os.cpu_count(),
        "cpu_model": _cpu_model(),
        "dependencies": _dependency_versions(),
        "os": platform.platform(),
        "python": sys.version,
        "ram_bytes": _ram_bytes(),
        "runner_id": os.environ.get("RUNNER_NAME", platform.node()),
        "runner_kind": runner_kind,
        "runner_labels": os.environ.get("RUNNER_LABELS"),
    }


def _parse_result(stdout: str) -> dict[str, Any]:
    done = next(line for line in stdout.splitlines() if line.startswith("[DONE]"))
    coverage_line = next(line for line in stdout.splitlines() if line.startswith("[COVERAGE]"))
    values = dict(re.findall(r"(\w+)=([^ ]+)", done))
    return {
        "completed": int(values["seeds"]),
        "failures": int(values["failures"]),
        "unique_operation_traces": int(values["operation_traces"]),
        "unique_scheduling_traces": int(values["scheduling_traces"]),
        "coverage": json.loads(coverage_line.removeprefix("[COVERAGE] ")),
    }


def _run_shard(
    *,
    repo: Path,
    campaign_root: Path,
    campaign_id: str,
    steps: int,
    shard: Shard,
    python: Path,
) -> dict[str, Any]:
    shard_name = f"shard-{shard.shard_id:03d}"
    shard_root = campaign_root / shard_name
    failure_root = shard_root / "failures"
    failure_root.mkdir(parents=True, exist_ok=True)
    stdout_path = shard_root / "stdout.log"
    stderr_path = shard_root / "stderr.log"
    command = [
        str(python),
        "tests/fuzz/runtime_fuzzer.py",
        "--seed",
        str(shard.seed_start),
        "--seeds",
        str(shard.seed_count),
        "--steps",
        str(steps),
        "--artifacts-dir",
        str(failure_root),
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo / "src")
    started = time.monotonic()
    with (
        stdout_path.open("w", encoding="utf-8") as stdout_file,
        stderr_path.open("w", encoding="utf-8") as stderr_file,
    ):
        process = subprocess.Popen(
            command,
            cwd=repo,
            env=env,
            text=True,
            stdout=stdout_file,
            stderr=stderr_file,
        )
        _, wait_status, usage = os.wait4(process.pid, 0)
        process.returncode = os.waitstatus_to_exitcode(wait_status)
    observed_wall = time.monotonic() - started
    stdout = stdout_path.read_text(encoding="utf-8")
    summary: dict[str, Any] = {
        "campaign_id": campaign_id,
        "command": command,
        "exit_status": process.returncode,
        "performance": {
            "cpu_system_seconds": usage.ru_stime,
            "cpu_user_seconds": usage.ru_utime,
            "peak_rss_kib": usage.ru_maxrss,
            "wall_seconds": observed_wall,
        },
        "seed_count": shard.seed_count,
        "seed_end_exclusive": shard.seed_start + shard.seed_count,
        "seed_start": shard.seed_start,
        "shard_id": shard_name,
        "steps": steps,
        "wall_observed_seconds": observed_wall,
    }
    if stdout:
        try:
            summary.update(_parse_result(stdout))
        except (KeyError, StopIteration, ValueError, json.JSONDecodeError) as exc:
            summary["parse_error"] = repr(exc)
    summary_path = shard_root / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _trace_summary(repo: Path, seed_start: int, seeds: int, steps: int) -> dict[str, Any]:
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "src"))
    from tests.fuzz.runtime_fuzzer import generate_schedule

    operation_hashes: set[bytes] = set()
    scheduling_hashes: set[bytes] = set()
    operation_corpus = hashlib.sha256()
    scheduling_corpus = hashlib.sha256()
    for seed in range(seed_start, seed_start + seeds):
        schedule = generate_schedule(seed, steps)
        operations = tuple(operation.render() for operation in schedule.operations)
        scheduling = tuple(
            operation.render()
            for operation in schedule.operations
            if operation.actor in SCHEDULING_ACTORS
            or (operation.actor, operation.action) in SCHEDULING_ACTIONS
        )
        operation_blob = json.dumps(operations, separators=(",", ":")).encode()
        scheduling_blob = json.dumps(scheduling, separators=(",", ":")).encode()
        operation_hashes.add(hashlib.blake2b(operation_blob, digest_size=16).digest())
        scheduling_hashes.add(hashlib.blake2b(scheduling_blob, digest_size=16).digest())
        operation_corpus.update(seed.to_bytes(8, "big"))
        operation_corpus.update(operation_blob)
        scheduling_corpus.update(seed.to_bytes(8, "big"))
        scheduling_corpus.update(scheduling_blob)
    return {
        "hash_algorithm": "blake2b-128 for uniqueness; sha256 ordered corpus",
        "operation_corpus_sha256": operation_corpus.hexdigest(),
        "scheduling_corpus_sha256": scheduling_corpus.hexdigest(),
        "unique_operation_traces": len(operation_hashes),
        "unique_scheduling_traces": len(scheduling_hashes),
    }


def _attach_failure_context(
    campaign_root: Path, *, identity: dict[str, Any], environment: dict[str, Any]
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for artifact in sorted(campaign_root.glob("shard-*/failures/runtime-seed*.json")):
        original = json.loads(artifact.read_text(encoding="utf-8"))
        context = {
            "artifact": str(artifact.relative_to(campaign_root)),
            "campaign": identity,
            "environment": environment,
            "failure": original,
        }
        sidecar = artifact.with_suffix(".context.json")
        sidecar.write_text(json.dumps(context, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        failures.append(context)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--seed-start", type=int, required=True)
    parser.add_argument("--seeds", type=int, required=True)
    parser.add_argument("--steps", type=int, required=True)
    parser.add_argument("--shard-size", type=int, default=5000)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--runner-kind", required=True)
    parser.add_argument("--expected-sha", required=True)
    args = parser.parse_args()
    repo = args.repo.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    sha = _run(["git", "rev-parse", "HEAD"], cwd=repo)
    dirty = bool(_run(["git", "status", "--porcelain"], cwd=repo))
    if sha != args.expected_sha or dirty:
        raise SystemExit(f"invalid baseline: sha={sha} dirty={dirty}")
    branch = _run(["git", "branch", "--show-current"], cwd=repo) or None
    identity = {
        "branch": branch,
        "campaign_id": args.campaign_id,
        "date_utc": datetime.now(UTC).isoformat(),
        "dirty": dirty,
        "fuzzer_schema": FUZZER_SCHEMA,
        "git_sha": sha,
        "schema": CAMPAIGN_SCHEMA,
        "seed_count": args.seeds,
        "seed_end_exclusive": args.seed_start + args.seeds,
        "seed_start": args.seed_start,
        "steps": args.steps,
    }
    environment = _environment(args.runner_kind)
    shards = [
        Shard(index, start, min(args.shard_size, args.seed_start + args.seeds - start))
        for index, start in enumerate(
            range(args.seed_start, args.seed_start + args.seeds, args.shard_size)
        )
    ]
    (output / "metadata.json").write_text(
        json.dumps(
            {
                "identity": identity,
                "environment": environment,
                "shards": [asdict(shard) for shard in shards],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    python = Path(sys.executable).resolve()
    campaign_started = time.monotonic()
    summaries: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = {
            pool.submit(
                _run_shard,
                repo=repo,
                campaign_root=output,
                campaign_id=args.campaign_id,
                steps=args.steps,
                shard=shard,
                python=python,
            ): shard
            for shard in shards
        }
        for future in as_completed(futures):
            summary = future.result()
            summaries.append(summary)
            print(
                f"[SHARD] id={summary['shard_id']} seeds={summary.get('completed', 0)} "
                f"failures={summary.get('failures', 'unknown')} "
                f"exit={summary['exit_status']}",
                flush=True,
            )
    summaries.sort(key=lambda item: item["seed_start"])
    coverage: Counter[str] = Counter()
    for summary in summaries:
        coverage.update(summary.get("coverage", {}))
    print("[DIVERSITY] computing deterministic corpus signatures", flush=True)
    traces = _trace_summary(repo, args.seed_start, args.seeds, args.steps)
    failures = _attach_failure_context(output, identity=identity, environment=environment)
    total_operations = sum(coverage.values())
    cpu_user = sum(item.get("performance", {}).get("cpu_user_seconds", 0) for item in summaries)
    cpu_system = sum(item.get("performance", {}).get("cpu_system_seconds", 0) for item in summaries)
    wall = time.monotonic() - campaign_started
    family_counts = Counter(
        FAMILIES[seed % len(FAMILIES)]
        for seed in range(args.seed_start, args.seed_start + args.seeds)
    )
    manifest = {
        "identity": identity,
        "environment": environment,
        "performance": {
            "cpu_system_seconds": cpu_system,
            "cpu_user_seconds": cpu_user,
            "operations_per_second": total_operations / wall,
            "peak_shard_rss_kib": max(
                (item.get("performance", {}).get("peak_rss_kib", 0) for item in summaries),
                default=0,
            ),
            "schedules_per_second": args.seeds / wall,
            "wall_seconds": wall,
        },
        "coverage": dict(sorted(coverage.items())),
        "family_counts": dict(sorted(family_counts.items())),
        "failures": failures,
        "result": {
            "exit_status": max((item["exit_status"] for item in summaries), default=0),
            "failure_count": len(failures),
            "seeds_executed": sum(item.get("completed", 0) for item in summaries),
            "total_operations": total_operations,
            **traces,
        },
        "shards": summaries,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"[DONE] campaign={args.campaign_id} seeds={manifest['result']['seeds_executed']} "
        f"failures={len(failures)} schedules_per_second="
        f"{manifest['performance']['schedules_per_second']:.2f}",
        flush=True,
    )
    return int(bool(failures) or manifest["result"]["exit_status"] != 0)


if __name__ == "__main__":
    raise SystemExit(main())
