"""Run a reproducible multi-process local fuzz campaign outside GitHub Actions."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ("codec", "engine", "websocket")


def _run_logged(command: list[str], log: Path, environment: dict[str, str]) -> None:
    with log.open("a", encoding="utf-8") as stream:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            stdout=stream,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    if completed.returncode:
        raise subprocess.CalledProcessError(completed.returncode, command)


def _shard(
    shard: int,
    *,
    output: str,
    base_seed: int,
    duration_seconds: int,
    iterations: int,
) -> dict[str, int]:
    root = Path(output) / f"shard-{shard}"
    failures = root / "failures"
    hypothesis = root / "hypothesis"
    failures.mkdir(parents=True, exist_ok=True)
    hypothesis.mkdir(parents=True, exist_ok=True)
    log = root / "campaign.log"
    seed_start = base_seed * 1_000_000 + shard * 200_000
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment["HYPOTHESIS_PROFILE"] = "aggressive"
    environment["HYPOTHESIS_STORAGE_DIRECTORY"] = str(hypothesis)
    started = time.monotonic()
    deadline = started + duration_seconds
    _run_logged(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "tests/fuzz/test_hypothesis_fuzz.py",
            f"--hypothesis-seed={seed_start}",
        ],
        log,
        environment,
    )
    batch = 0
    while time.monotonic() < deadline:
        for target in TARGETS:
            if time.monotonic() >= deadline:
                break
            seed = seed_start + batch
            _run_logged(
                [
                    sys.executable,
                    "tests/fuzz/fuzz.py",
                    "--target",
                    target,
                    "--seed",
                    str(seed),
                    "--iterations",
                    str(iterations),
                    "--progress-every",
                    str(iterations),
                    "--artifacts-dir",
                    str(failures),
                ],
                log,
                environment,
            )
            batch += 1
    result = {
        "shard": shard,
        "seed_start": seed_start,
        "batches": batch,
        "elapsed_seconds": round(time.monotonic() - started),
    }
    (root / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shards", type=int, default=5)
    parser.add_argument("--duration-minutes", type=float, default=288.0)
    parser.add_argument("--base-seed", type=int, default=77)
    parser.add_argument("--iterations", type=int, default=100_000)
    args = parser.parse_args()
    if args.shards <= 0 or args.duration_minutes <= 0 or args.iterations <= 0:
        parser.error("shards, duration and iterations must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    duration_seconds = max(1, round(args.duration_minutes * 60))
    metadata = {
        "revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "base_seed": args.base_seed,
        "shards": args.shards,
        "duration_seconds_per_shard": duration_seconds,
        "planned_cpu_hours": args.shards * duration_seconds / 3600,
        "iterations_per_batch": args.iterations,
        "targets": TARGETS,
    }
    (args.output / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    with ProcessPoolExecutor(max_workers=args.shards) as executor:
        futures = [
            executor.submit(
                _shard,
                shard,
                output=str(args.output),
                base_seed=args.base_seed,
                duration_seconds=duration_seconds,
                iterations=args.iterations,
            )
            for shard in range(args.shards)
        ]
        results = [future.result() for future in futures]
    metadata["results"] = results
    (args.output / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
