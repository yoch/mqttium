"""Rotated base/candidate regression check for native RTT and QoS 0 ingress.

Each sample runs in a fresh interpreter with only one checkout on PYTHONPATH.
ABBA ordering limits runner drift; the gate rejects only median regressions larger
than the configured tolerance because absolute hosted-runner throughput varies.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


def _run(
    script: Path,
    root: Path,
    output: Path,
    *arguments: str,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    subprocess.run(
        [sys.executable, str(script), *arguments, "--output", str(output)],
        check=True,
        env=env,
    )
    return json.loads(output.read_text())


def _pair(
    script: Path,
    base_root: Path,
    candidate_root: Path,
    directory: Path,
    *,
    cycle: int,
    arguments: tuple[str, ...],
    result_key: str,
) -> dict[str, Any]:
    documents: dict[str, dict[str, Any]] = {}
    order = (
        ("base-a", base_root),
        ("candidate-a", candidate_root),
        ("candidate-b", candidate_root),
        ("base-b", base_root),
    )
    for label, root in order:
        documents[label] = _run(
            script,
            root,
            directory / f"{result_key}-{cycle}-{label}.json",
            "--label",
            f"{result_key}-{cycle}-{label}",
            *arguments,
        )
    base = [documents["base-a"], documents["base-b"]]
    candidate = [documents["candidate-a"], documents["candidate-b"]]
    base_rate = statistics.median(item[result_key]["rate_median"] for item in base)
    candidate_rate = statistics.median(
        item[result_key]["rate_median"] for item in candidate
    )
    return {
        "cycle": cycle,
        "base_rate": base_rate,
        "candidate_rate": candidate_rate,
        "ratio": candidate_rate / base_rate,
        "samples": documents,
    }


def _direct_ingress_pair(
    script: Path,
    base_root: Path,
    candidate_root: Path,
    directory: Path,
    *,
    cycle: int,
    messages: int,
) -> dict[str, Any]:
    documents: dict[str, dict[str, Any]] = {}
    order = (
        ("base-a", base_root),
        ("candidate-a", candidate_root),
        ("candidate-b", candidate_root),
        ("base-b", base_root),
    )
    for label, root in order:
        documents[label] = _run(
            script,
            root,
            directory / f"ingress-{cycle}-{label}.json",
            "--label",
            f"ingress-{cycle}-{label}",
            "--runs",
            "1",
            "--messages",
            str(messages),
        )
    base = [documents["base-a"], documents["base-b"]]
    candidate = [documents["candidate-a"], documents["candidate-b"]]
    base_rate = statistics.median(item["rate_median"] for item in base)
    candidate_rate = statistics.median(item["rate_median"] for item in candidate)
    return {
        "cycle": cycle,
        "base_rate": base_rate,
        "candidate_rate": candidate_rate,
        "ratio": candidate_rate / base_rate,
        "samples": documents,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--rtt-pairs", type=int, default=8_000)
    parser.add_argument("--ingress-messages", type=int, default=120_000)
    parser.add_argument("--minimum-ratio", type=float, default=0.97)
    args = parser.parse_args()

    script_root = Path(__file__).resolve().parent
    rtt_script = script_root / "native_rtt_ingress.py"
    ingress_script = script_root / "direct_ingress_hotpath.py"

    with tempfile.TemporaryDirectory(prefix="mqttium-native-paired-") as raw:
        directory = Path(raw)
        ingress_cycles = [
            _direct_ingress_pair(
                ingress_script,
                args.base_root,
                args.candidate_root,
                directory,
                cycle=cycle,
                messages=args.ingress_messages,
            )
            for cycle in range(1, args.repeat + 1)
        ]
        rtt_cycles = [
            _pair(
                rtt_script,
                args.base_root,
                args.candidate_root,
                directory,
                cycle=cycle,
                arguments=(
                    "--scenario",
                    "rtt",
                    "--runs",
                    "1",
                    "--rtt-pairs",
                    str(args.rtt_pairs),
                ),
                result_key="rtt",
            )
            for cycle in range(1, args.repeat + 1)
        ]

    result = {
        "schema_version": 1,
        "repeat": args.repeat,
        "minimum_ratio": args.minimum_ratio,
        "ingress": {
            "paired_ratio_median": statistics.median(
                cycle["ratio"] for cycle in ingress_cycles
            ),
            "cycles": ingress_cycles,
        },
        "rtt": {
            "paired_ratio_median": statistics.median(
                cycle["ratio"] for cycle in rtt_cycles
            ),
            "cycles": rtt_cycles,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))

    failures = [
        name
        for name in ("ingress", "rtt")
        if result[name]["paired_ratio_median"] < args.minimum_ratio
    ]
    if failures:
        joined = ", ".join(failures)
        raise SystemExit(f"native hotpath regression beyond tolerance: {joined}")


if __name__ == "__main__":
    main()
