"""Reevaluate retained open-loop evidence after a confirmation-screen correction."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import open_loop_release_gate


ROOT = Path(__file__).resolve().parents[1]


def _git_sha() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def _policy_source_sha256() -> str:
    digest = hashlib.sha256()
    for path in (Path(open_loop_release_gate.__file__), Path(__file__)):
        digest.update(path.name.encode("utf-8") + b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def reevaluate(input_path: Path, output_path: Path) -> dict[str, Any]:
    if input_path.resolve() == output_path.resolve():
        raise ValueError("reevaluation output must not overwrite the original artifact")
    source_bytes = input_path.read_bytes()
    payload = json.loads(source_bytes)
    if not isinstance(payload, dict):
        raise ValueError("open-loop evidence must be a JSON object")
    result = open_loop_release_gate.reevaluate_confirmation_overflow(payload)
    result["reevaluation"].update(
        {
            "source_artifact": str(input_path.resolve()),
            "source_artifact_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "evaluator_git_sha": _git_sha(),
            "policy_source_sha256": _policy_source_sha256(),
        }
    )
    open_loop_release_gate._write_result(output_path, result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = reevaluate(args.input, args.output)
    except (OSError, ValueError, KeyError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(f"open-loop evidence reevaluation invalid: {exc}", file=sys.stderr)
        return 2
    print(f"open-loop evidence reevaluation {result['status']}: {args.output}")
    return 0 if result["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
