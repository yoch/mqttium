#!/usr/bin/env python3
from __future__ import annotations

import base64
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

JOB_RE = re.compile(r"^[A-Za-z0-9._-]{1,80}$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class WorkbenchError(RuntimeError):
    pass


def env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise WorkbenchError(f"Missing environment variable: {name}")
    return value


def run(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout: int | None = None,
    check: bool = True,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    proc_env = os.environ.copy()
    if extra_env:
        proc_env.update(extra_env)
    completed = subprocess.run(
        args,
        cwd=cwd,
        timeout=timeout,
        text=True,
        capture_output=True,
        env=proc_env,
    )
    if check and completed.returncode != 0:
        raise WorkbenchError(
            f"Command failed ({completed.returncode}): {' '.join(args)}\n"
            f"{completed.stdout}\n{completed.stderr}"
        )
    return completed


def safe_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")


def github_get(path: str, token: str) -> Any:
    request = urllib.request.Request(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "chatgpt-persistent-workbench",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def decode_job(encoded: str) -> dict[str, Any]:
    padded = encoded.strip() + "=" * (-len(encoded.strip()) % 4)
    try:
        raw = gzip.decompress(base64.urlsafe_b64decode(padded))
        job = json.loads(raw)
    except Exception as exc:
        raise WorkbenchError(f"Cannot decode job: {exc}") from exc
    if not isinstance(job, dict):
        raise WorkbenchError("Job must be an object")
    return job


def validate_job(job: dict[str, Any], expected_id: str, expected_head: str) -> dict[str, Any]:
    if job.get("version") != 1:
        raise WorkbenchError("Unsupported job version")
    if job.get("job_id") != expected_id:
        raise WorkbenchError("Job ID mismatch")
    if job.get("expected_head") != expected_head or not SHA_RE.fullmatch(expected_head):
        raise WorkbenchError("Expected head mismatch")
    patch = job.get("patch")
    if not isinstance(patch, str) or not patch.strip():
        raise WorkbenchError("Patch is empty")
    if "\n+++ " not in patch or "\n--- " not in patch:
        raise WorkbenchError("Patch is not a unified Git patch")
    normalized: dict[str, Any] = {
        "version": 1,
        "job_id": expected_id,
        "expected_head": expected_head,
        "patch": patch,
        "setup": validate_commands(job.get("setup", [])),
        "checks": validate_commands(job.get("checks", [])),
        "commit_message": str(job.get("commit_message", "")).strip(),
        "push": bool(job.get("push", True)),
    }
    if normalized["push"] and not normalized["commit_message"]:
        raise WorkbenchError("commit_message is required")
    return normalized


def validate_commands(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list) or len(items) > 20:
        raise WorkbenchError("Command list must contain at most 20 entries")
    allowed = {x.strip() for x in env("CGW_ALLOWED_IMAGES").split(",") if x.strip()}
    result: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise WorkbenchError(f"Command {index} must be an object")
        name = str(item.get("name", f"command-{index + 1}"))
        image = str(item.get("image", ""))
        command = str(item.get("command", ""))
        timeout = int(item.get("timeout", 1200))
        network = bool(item.get("network", False))
        if image not in allowed:
            raise WorkbenchError(f"Image is not allowed: {image}")
        if not command or len(command) > 8000:
            raise WorkbenchError(f"Invalid command at index {index}")
        if timeout < 1 or timeout > 7200:
            raise WorkbenchError(f"Invalid timeout at index {index}")
        result.append(
            {
                "name": name[:100],
                "image": image,
                "command": command,
                "timeout": timeout,
                "network": network,
            }
        )
    return result


def paths() -> dict[str, Path]:
    root = Path(env("CGW_ROOT")).resolve()
    repo = env("CGW_REPOSITORY")
    pr = env("CGW_PR_NUMBER")
    job_id = env("CGW_JOB_ID")
    repo_key = safe_component(repo)
    return {
        "root": root,
        "workspace": root / "workspaces" / repo_key / f"pr-{pr}",
        "cache": root / "caches" / repo_key,
        "lock": root / "locks" / repo_key / f"pr-{pr}.lock",
        "result": root / "results" / repo / job_id,
        "state": root / "results" / repo / job_id / "state.json",
    }


def get_job(token: str) -> dict[str, Any]:
    repo = env("CGW_REPOSITORY")
    branch = env("CGW_JOB_BRANCH")
    job_id = env("CGW_JOB_ID")
    if not JOB_RE.fullmatch(job_id):
        raise WorkbenchError("Invalid job ID")
    data = github_get(
        f"/repos/{repo}/contents/.cgw/jobs/{job_id}.b64?ref={branch}",
        token,
    )
    content = str(data["content"]).replace("\n", "")
    decoded_file = base64.b64decode(content).decode()
    return decode_job(decoded_file)


def prepare_workspace(workspace: Path, expected_head: str) -> None:
    repo = env("CGW_REPOSITORY")
    url = f"https://github.com/{repo}.git"
    workspace.parent.mkdir(parents=True, exist_ok=True)
    if not (workspace / ".git").exists():
        if workspace.exists():
            shutil.rmtree(workspace)
        run(["git", "clone", "--no-checkout", url, str(workspace)])
    run(["git", "remote", "set-url", "origin", url], cwd=workspace)
    run(["git", "-c", "core.hooksPath=/dev/null", "fetch", "--prune", "origin"], cwd=workspace)
    run(["git", "-c", "core.hooksPath=/dev/null", "reset", "--hard", expected_head], cwd=workspace)
    run(["git", "-c", "core.hooksPath=/dev/null", "clean", "-ffd"], cwd=workspace)
    actual = run(["git", "rev-parse", "HEAD"], cwd=workspace).stdout.strip()
    if actual != expected_head:
        raise WorkbenchError(f"Workspace head {actual} != {expected_head}")


def apply_patch(workspace: Path, patch_path: Path) -> None:
    run(
        ["git", "-c", "core.hooksPath=/dev/null", "apply", "--check", "--whitespace=error-all", str(patch_path)],
        cwd=workspace,
    )
    run(
        ["git", "-c", "core.hooksPath=/dev/null", "apply", "--index", "--whitespace=error-all", str(patch_path)],
        cwd=workspace,
    )
    run(["git", "diff", "--cached", "--check"], cwd=workspace)


def docker_command(
    workspace: Path,
    cache: Path,
    item: dict[str, Any],
) -> subprocess.CompletedProcess[str]:
    cache.mkdir(parents=True, exist_ok=True)
    network = "bridge" if item["network"] else "none"
    args = [
        "docker",
        "run",
        "--rm",
        "--init",
        "--network",
        network,
        "--cpus",
        "2",
        "--memory",
        "4g",
        "--pids-limit",
        "512",
        "--security-opt",
        "no-new-privileges",
        "--cap-drop",
        "ALL",
        "-e",
        "HOME=/tmp/home",
        "-e",
        "XDG_CACHE_HOME=/cache/xdg",
        "-e",
        "PIP_CACHE_DIR=/cache/pip",
        "-e",
        "npm_config_cache=/cache/npm",
        "-v",
        f"{workspace}:/workspace:rw",
        "-v",
        f"{cache}:/cache:rw",
        "-w",
        "/workspace",
        item["image"],
        "bash",
        "--noprofile",
        "--norc",
        "-lc",
        item["command"],
    ]
    return run(args, timeout=item["timeout"], check=False)


def execute_commands(
    workspace: Path,
    cache: Path,
    result_dir: Path,
    items: list[dict[str, Any]],
    category: str,
) -> list[dict[str, Any]]:
    logs = result_dir / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for index, item in enumerate(items, 1):
        started = time.monotonic()
        try:
            completed = docker_command(workspace, cache, item)
            status = "success" if completed.returncode == 0 else "failure"
            returncode = completed.returncode
            stdout, stderr = completed.stdout, completed.stderr
        except subprocess.TimeoutExpired as exc:
            status, returncode = "timeout", 124
            stdout, stderr = exc.stdout or "", exc.stderr or ""
        duration = time.monotonic() - started
        filename = f"{category}-{index:02d}-{safe_component(item['name'])}.log"
        (logs / filename).write_text(
            f"$ {item['command']}\n"
            f"image={item['image']} network={item['network']}\n\n"
            f"STDOUT\n{stdout}\n\nSTDERR\n{stderr}\n"
        )
        record = {
            "category": category,
            "name": item["name"],
            "status": status,
            "returncode": returncode,
            "duration_seconds": round(duration, 3),
        }
        results.append(record)
        if status != "success":
            break
    return results


def execute() -> None:
    p = paths()
    for key in ("result", "cache", "lock"):
        p[key].parent.mkdir(parents=True, exist_ok=True)
    p["result"].mkdir(parents=True, exist_ok=True)
    token = env("GH_TOKEN")
    job = validate_job(get_job(token), env("CGW_JOB_ID"), env("CGW_HEAD_SHA"))
    (p["result"] / "job.json").write_text(json.dumps(job, indent=2) + "\n")
    patch_path = p["result"] / "job.patch"
    patch_path.write_text(job["patch"])

    with p["lock"].open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        prepare_workspace(p["workspace"], job["expected_head"])
        apply_patch(p["workspace"], patch_path)
        records: list[dict[str, Any]] = []
        for category in ("setup", "checks"):
            current = execute_commands(
                p["workspace"], p["cache"], p["result"], job[category], category
            )
            records.extend(current)
            if current and current[-1]["status"] != "success":
                break

        staged = run(["git", "diff", "--cached", "--binary"], cwd=p["workspace"])
        (p["result"] / "final.patch").write_text(staged.stdout)
        status = "success"
        if records and records[-1]["status"] != "success":
            status = "failure"
        state = {
            "job": job,
            "status": status,
            "records": records,
            "workspace": str(p["workspace"]),
        }
        p["state"].write_text(json.dumps(state, indent=2) + "\n")
        if status != "success":
            raise WorkbenchError("One or more commands failed")


def push() -> None:
    p = paths()
    state = json.loads(p["state"].read_text())
    job = state["job"]
    if state["status"] != "success":
        raise WorkbenchError("Cannot push failed job")
    if not job["push"]:
        return
    token = env("CGW_PUSH_TOKEN")
    with p["lock"].open("w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        expected = job["expected_head"]
        remote = run(
            ["git", "ls-remote", "origin", f"refs/heads/{env('CGW_HEAD_REF')}"],
            cwd=p["workspace"],
        ).stdout.split()
        if not remote or remote[0] != expected:
            raise WorkbenchError("Remote branch moved before push")
        run(
            [
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "user.name=ChatGPT Persistent Workbench",
                "-c",
                "user.email=41898282+github-actions[bot]@users.noreply.github.com",
                "commit",
                "-m",
                job["commit_message"],
            ],
            cwd=p["workspace"],
        )
        sha = run(["git", "rev-parse", "HEAD"], cwd=p["workspace"]).stdout.strip()
        authenticated = f"https://x-access-token:{token}@github.com/{env('CGW_REPOSITORY')}.git"
        run(
            [
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "push",
                authenticated,
                f"HEAD:refs/heads/{env('CGW_HEAD_REF')}",
            ],
            cwd=p["workspace"],
        )
        state["commit_sha"] = sha
        p["state"].write_text(json.dumps(state, indent=2) + "\n")


def comment() -> None:
    p = paths()
    state: dict[str, Any] = {}
    if p["state"].exists():
        state = json.loads(p["state"].read_text())
    execute_outcome = os.environ.get("EXECUTE_OUTCOME", "unknown")
    push_outcome = os.environ.get("PUSH_OUTCOME", "unknown")
    ok = execute_outcome == "success" and push_outcome in {"success", "skipped"}
    icon = "✅" if ok else "❌"
    lines = [
        f"<!-- cgw-result:{env('CGW_JOB_ID')} -->",
        f"### {icon} ChatGPT Persistent Workbench",
        "",
        f"- **Job:** `{env('CGW_JOB_ID')}`",
        f"- **Execution:** `{execute_outcome}`",
        f"- **Push:** `{push_outcome}`",
    ]
    if state.get("commit_sha"):
        lines.append(f"- **Commit:** `{state['commit_sha']}`")
    lines.append(f"- **Run:** {env('CGW_RUN_URL')}")
    records = state.get("records", [])
    if records:
        lines += ["", "| Phase | Check | Status | Duration |", "| --- | --- | --- | ---: |"]
        for item in records:
            lines.append(
                f"| {item['category']} | `{item['name']}` | {item['status']} | "
                f"{item['duration_seconds']:.3f}s |"
            )
    lines += ["", "Detailed logs and patches are available in the workflow artifact."]
    body = "\n".join(lines)
    run(
        [
            "gh",
            "pr",
            "comment",
            env("CGW_ISSUE_NUMBER"),
            "--repo",
            env("CGW_REPOSITORY"),
            "--body",
            body,
        ],
        extra_env={"GH_TOKEN": env("GH_TOKEN")},
    )


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in {"execute", "push", "comment"}:
        print("Usage: processor.py execute|push|comment", file=sys.stderr)
        return 2
    try:
        globals()[sys.argv[1]]()
        return 0
    except WorkbenchError as exc:
        print(f"CGW error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
