"""Reconstruct the exact reviewed prototype and summarize diagnostic RTT evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import statistics
import subprocess
from pathlib import Path

BASE = "9ad1f01857306ac5079ffb1d073a59fdb60e1931"
BASE_SRC = "395c73fabc1b0b04fab4b356fff07ac064b1eef8"
PATCH_SHA256 = "c6faba951d28eb8824520ee4340c19d39e066a1d0fd8782503149d4eb535da48"
HASHES = {
    "src/mqttium/api/_delivery.py": (
        "19352c23232fca07bdec3e38e23291358615aa2fb075be87020b89373b16c0fc",
        "241593a97eceea8a45abcc2ac9e1e03b2b8f508900ef54cf981c4686e00eaa5d",
    ),
    "src/mqttium/api/_effects.py": (
        "7a8c3bfb400a13a04f4d1c36be75f2864808bf83536d5b6e6ea584ed4b895748",
        "d793b4d526e80651fed8bf9abaf3d502950427ed9d2be0896012a44ec32ee60a",
    ),
    "src/mqttium/api/async_client.py": (
        "3895ed930941dda28968d902581eea6e54b59a3458a29e605a71c602d4d7269a",
        "e7885d7d7e207d5547780cfe750ea622a13489d3312a3358dacf754e92f04f0a",
    ),
}


def git(root: Path, *args: str, env: dict | None = None, text: str | None = None) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True,
        text=True, input=text, env=env, timeout=30,
    ).stdout.strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def verify(root: Path, candidate: bool) -> dict:
    actual = {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in HASHES}
    require(actual == {p: h[int(candidate)] for p, h in HASHES.items()}, f"source mismatch: {root}")
    require(not git(root, "status", "--porcelain", "--untracked-files=no"), f"dirty source: {root}")
    head = git(root, "rev-parse", "HEAD")
    src = git(root, "rev-parse", "HEAD:src")
    if not candidate:
        require(head == BASE and src == BASE_SRC, "wrong baseline commit/tree")
    else:
        require(git(root, "rev-parse", "HEAD^") == BASE, "wrong candidate parent")
        require(set(git(root, "diff", "--name-only", BASE, "HEAD").splitlines()) == set(HASHES),
                "candidate changes more than the three reviewed runtime files")
    return {"commit": head, "src_tree": src, "runtime_sha256": actual}


def identities(workspace: Path) -> dict:
    return {name: verify(workspace / name, name == "arm-b") for name in ("arm-a", "arm-a-aa", "arm-b")}


def prepare(workspace: Path, out: Path) -> None:
    patch = Path(__file__).with_name("first-inline-runtime.patch").resolve()
    require(hashlib.sha256(patch.read_bytes()).hexdigest() == PATCH_SHA256, "patch checksum mismatch")
    for name in ("arm-a", "arm-a-aa", "arm-b"):
        verify(workspace / name, False)
    root = workspace / "arm-b"
    git(root, "apply", "--check", str(patch))
    git(root, "apply", "--index", str(patch))
    require(set(git(root, "diff", "--cached", "--name-only").splitlines()) == set(HASHES), "unexpected patch paths")
    tree = git(root, "write-tree")
    env = os.environ | {
        "GIT_AUTHOR_NAME": "MQTTium benchmark", "GIT_AUTHOR_EMAIL": "benchmark@example.invalid",
        "GIT_COMMITTER_NAME": "MQTTium benchmark", "GIT_COMMITTER_EMAIL": "benchmark@example.invalid",
        "GIT_AUTHOR_DATE": "2026-09-10T00:00:00Z", "GIT_COMMITTER_DATE": "2026-09-10T00:00:00Z",
    }
    commit = git(root, "commit-tree", tree, "-p", BASE, env=env,
                 text="Local-only first-inline benchmark candidate; runtime patch " + PATCH_SHA256 + "\n")
    git(root, "checkout", "--detach", commit)
    record = {"sources": identities(workspace), "candidate_commit_is_local_only": True,
              "patch_sha256": PATCH_SHA256, "workflow_sha": os.environ.get("GITHUB_SHA"),
              "harness_sha": git(workspace / "harness", "rev-parse", "HEAD"),
              "python": platform.python_version(), "kernel": platform.release(),
              "machine": platform.machine(), "profile": os.environ.get("PROFILE"),
              "target_rate": 3942, "blocks_per_phase": 2, "max_block_retries": 0,
              "governors": {str(p): p.read_text().strip() for p in Path('/sys/devices/system/cpu').glob('cpu*/cpufreq/scaling_governor')},
              "temperature_start": {str(p): p.read_text().strip() for p in Path('/sys/class/thermal').glob('thermal_zone*/temp')}}
    (out / "provenance.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))


def check(workspace: Path, out: Path) -> None:
    previous = json.loads((out / "provenance.json").read_text())["sources"]
    require(identities(workspace) == previous, "source changed after preparation")
    print("All source commits, trees and reviewed runtime checksums match.")


def fields(run: dict) -> dict:
    worker = next((w for w in run.get("workers", []) if w.get("role") == "rtt_initiator"), {})
    latency = worker.get("latency_summary") or {}
    delta = (worker.get("runtime") or {}).get("measure_delta") or {}
    completed = worker.get("completed_in_window")
    cpu = None
    if completed and "ru_utime_s" in delta and "ru_stime_s" in delta:
        cpu = 1e6 * (delta["ru_utime_s"] + delta["ru_stime_s"]) / completed
    return {"RTT p50 (ms)": latency.get("p50_ms"), "RTT p95 (ms)": latency.get("p95_ms"),
            "Completed RTT/s": run.get("primary_msgs_per_s"),
            "Initiator CPU (us/completed RTT)": cpu,
            "Completion fraction": worker.get("completion"),
            "Completed in window": completed, "Timeouts": worker.get("timeouts"),
            "Missed due to backpressure": worker.get("missed_due_to_backpressure")}


def paired_ratio(runs: list[dict], key: str) -> float | None:
    ratios = []
    blocks = sorted({r["logical_block"] for r in runs})
    for block in blocks:
        values = {arm: [fields(r)[key] for r in runs if r["logical_block"] == block and r["ab_label"] == arm] for arm in ("A", "B")}
        if any(len(v) != 2 or any(x is None or not math.isfinite(float(x)) for x in v) for v in values.values()):
            return None
        a, b = (statistics.mean(values[k]) for k in ("A", "B"))
        if a <= 0 or b <= 0:
            return None
        ratios.append(b / a)
    return math.exp(statistics.mean(math.log(r) for r in ratios)) if ratios else None


def summary(out: Path) -> None:
    lines = ["# First-inline network diagnostic", "", "**Not an acceptance or release gate.**",
             "Two balanced ABBA/BAAB blocks per phase; no retries, no noise subtraction.",
             "A = exact main 9ad1f018; B = the exact three runtime files of the supplied prototype.",
             "CPU is initiator process CPU per completed request/response, not whole-system CPU.",
             "Callback latency and library counters remain in raw JSON when provided by the harness.", ""]
    reliability = {}
    for name in ("aa", "ab"):
        path = out / f"{name}.json"
        if not path.exists():
            lines += [f"## {name.upper()}: no report (inspect execution log)", ""]
            reliability[name] = False
            continue
        report = json.loads(path.read_text())
        q = report.get("qualification") or {}
        usable = q.get("ok") is True and q.get("blocks_complete") == q.get("blocks_requested") == 2
        reliability[name] = usable
        runs = [r for r in report.get("runs", []) if r.get("active_for_verdict", True)]
        lines += [f"## {name.upper()}", f"Profile: `{report.get('profile')}`. Harness qualification: `{usable}`.",
                  "Smoke remains NON_COMPARABLE even when harness observations are usable.",
                  f"Harness verdict: `{json.dumps(report.get('verdict') or {}, sort_keys=True)}`", "",
                  "| Metric | A median of runs | B median of runs | Paired B/A change |", "|---|---:|---:|---:|"]
        metrics = list(fields({}))
        def fmt(x):
            return "n/a" if x is None else f"{x:.6g}"
        for key in metrics:
            values = {a: [fields(r)[key] for r in runs if r.get("ab_label") == a] for a in ("A", "B")}
            medians = [statistics.median(v) if v and all(x is not None for x in v) else None for v in values.values()]
            ratio = paired_ratio(runs, key)
            change = "n/a" if ratio is None else f"{100*(ratio-1):+.2f}%"
            lines += [f"| {key} | {fmt(medians[0])} | {fmt(medians[1])} | {change} |"]
        lines += ["", "These per-run medians are not pooled latency percentiles. Relative changes use balanced block ratios.", ""]
        if name == "aa":
            drift = (report.get("verdict") or {}).get("absolute_effect_pct")
            reliability["aa_within_3pct"] = drift is not None and abs(drift) <= 3
        for run in runs:
            if run.get("status") != "valid" or not run.get("version_ab_observation_usable"):
                lines += [f"Unusable slot {run.get('execution_slot')}: `{run.get('reasons')}`; temporal `{run.get('temporal_quality')}`."]
    lines += ["", f"Quality flags: `{json.dumps(reliability, sort_keys=True)}`.",
              "If A/A is noisy or either phase is unqualified, A/B is descriptive only: no causal performance conclusion.",
              "Only one complementary block pair is measured; this is insufficient for a reliable confidence interval."]
    text = "\n".join(lines) + "\n"
    (out / "summary.md").write_text(text)
    print(text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("prepare", "check", "summary"))
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    if args.operation == "summary":
        summary(args.out)
    else:
        globals()[args.operation](args.workspace.resolve(), args.out.resolve())


if __name__ == "__main__":
    main()
