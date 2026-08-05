from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path


def pin(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def worker(scenario: str, cpu: int | None) -> None:
    pin(cpu)
    from mqttium.compat.paho import CallbackAPIVersion, Client, _PendingPublish
    from mqttium.enums import ConnectionState, QoS

    topic = "bench/compat-boundary"
    payload = b"x"

    async def run() -> dict[str, float | int | str]:
        client = Client(
            CallbackAPIVersion.VERSION2,
            client_id="compat-boundary-ab",
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
        client._async._max_outbound_messages = 100_000
        client._async._max_outbound_bytes = 16 * 1024 * 1024
        client._async._engine.state = ConnectionState.CONNECTED
        client._loop = asyncio.get_running_loop()
        client._thread = threading.current_thread()
        request = _PendingPublish(topic, payload, False, QoS.AT_LEAST_ONCE, None)
        warmup = 1_000
        operations = 20_000

        for index in range(warmup + operations):
            if index == warmup:
                started = time.perf_counter()
            if scenario == "commit":
                client._commit_qosn_publish_on_loop(request)
            elif scenario == "publish":
                info = client.publish(topic, payload, qos=1)
                if info.mid is None:
                    raise RuntimeError("QoS 1 loop-thread publish returned no MID")
            else:
                raise ValueError(scenario)
        elapsed = time.perf_counter() - started
        return {
            "scenario": scenario,
            "operations": operations,
            "elapsed_s": elapsed,
            "ops_per_s": operations / elapsed,
        }

    print(json.dumps(asyncio.run(run())))


def run_worker(script: Path, root: Path, scenario: str, cpu: int | None) -> float:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [sys.executable, str(script), "--worker", "--scenario", scenario]
    if cpu is not None:
        command.extend(["--cpu", str(cpu)])
    completed = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
    return float(json.loads(completed.stdout.splitlines()[-1])["ops_per_s"])


def parent(base: Path, candidate: Path, repeat: int, cpu: int | None, output: Path) -> None:
    script = Path(__file__).resolve()
    result: dict[str, object] = {"repeat": repeat, "cpu": cpu, "scenarios": []}
    for scenario in ("commit", "publish"):
        pairs: list[dict[str, object]] = []
        ratios: list[float] = []
        for index in range(repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            rates: dict[str, float] = {}
            for variant in order:
                root = base if variant == "base" else candidate
                rates[variant] = run_worker(script, root, scenario, cpu)
            ratio = rates["candidate"] / rates["base"]
            ratios.append(ratio)
            pairs.append({"order": order, "base": rates["base"], "candidate": rates["candidate"], "ratio": ratio})
        item = {
            "name": scenario,
            "median_candidate_over_base": statistics.median(ratios),
            "min_candidate_over_base": min(ratios),
            "max_candidate_over_base": max(ratios),
            "pairs": pairs,
        }
        result["scenarios"].append(item)  # type: ignore[union-attr]
        print(
            f"{scenario}: candidate/base={item['median_candidate_over_base']:.4f} "
            f"range=[{item['min_candidate_over_base']:.4f}, {item['max_candidate_over_base']:.4f}]"
        )
    output.write_text(json.dumps(result, indent=2) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--scenario", choices=("commit", "publish"))
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--repeat", type=int, default=21)
    parser.add_argument("--cpu", type=int, default=0)
    parser.add_argument("--output", type=Path, default=Path("/tmp/compat-boundary-ab.json"))
    return parser.parse_args()


args = parse_args()
if args.worker:
    if args.scenario is None:
        raise SystemExit("--scenario is required")
    worker(args.scenario, args.cpu)
else:
    if args.base_root is None or args.candidate_root is None:
        raise SystemExit("--base-root and --candidate-root are required")
    parent(args.base_root, args.candidate_root, args.repeat, args.cpu, args.output)
