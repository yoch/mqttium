"""Paired same-runner microbenchmark for source-level performance changes.

The parent alternates a base checkout and a candidate checkout. Each sample runs
in a fresh interpreter with only that checkout on ``PYTHONPATH`` so imports and
module globals cannot leak across variants.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from collections.abc import Callable


@dataclass
class WorkerResult:
    scenario: str
    elapsed_s: float
    operations: int
    ops_per_s: float


SCENARIOS = (
    "encode_qos0",
    "encode_qos1",
    "ingress_engine_qos0",
    "effect_send_inline",
    "effect_batch_inline",
)


def _pin(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def _measure(fn: Callable[[], None], *, operations: int, warmup: int) -> WorkerResult:
    for _ in range(warmup):
        fn()
    started = time.perf_counter()
    for _ in range(operations):
        fn()
    elapsed = time.perf_counter() - started
    return WorkerResult("", elapsed, operations, operations / elapsed)


def _worker(args: argparse.Namespace) -> None:
    _pin(args.cpu)
    from mqttium.api.async_client import AsyncClient
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
    from mqttium.packets import PublishPacket
    from mqttium.protocol.effects import EffectKind
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine

    topic = "bench/sensors/temp"
    payload = b'{"t":21.5,"h":40}'
    scenario = args.scenario

    if scenario.startswith("encode_"):
        qos = QoS.AT_MOST_ONCE if scenario == "encode_qos0" else QoS.AT_LEAST_ONCE
        packet = PublishPacket(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=None if qos == QoS.AT_MOST_ONCE else 42,
        )
        result = _measure(
            lambda: packet.encode(MQTTProtocolVersion.MQTTv311),
            operations=120_000,
            warmup=2_000,
        )
    elif scenario == "ingress_engine_qos0":
        packet = PublishPacket(
            topic=topic,
            payload=payload,
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
        )
        wire = packet.encode(MQTTProtocolVersion.MQTTv311)
        decoder = IncrementalDecoder()
        decoder.feed(wire)
        raw = decoder.drain_packets()[0]
        engine = ProtocolEngine(
            EngineConfig(client_id="paired-ingress", protocol=MQTTProtocolVersion.MQTTv311)
        )
        engine.state = ConnectionState.CONNECTED

        def run_ingress() -> None:
            engine.handle_raw(raw)
            engine.take_effects()

        result = _measure(run_ingress, operations=80_000, warmup=2_000)
    elif scenario in ("effect_send_inline", "effect_batch_inline"):
        client = AsyncClient(client_id="paired-effects")
        client._engine.state = ConnectionState.CONNECTED
        batch_size = 1 if scenario == "effect_send_inline" else 8

        def run_effects() -> None:
            for _ in range(batch_size):
                client._engine._emit(EffectKind.SEND, b"x")
            client._collect_effects_locked()
            while True:
                try:
                    item = client._outbound.get_nowait()
                except asyncio.QueueEmpty:
                    break
                client._outbound_bytes -= len(item)
                client._outbound.task_done()

        result = _measure(
            run_effects,
            operations=100_000 if batch_size == 1 else 30_000,
            warmup=2_000,
        )
    else:
        raise ValueError(f"unknown scenario: {scenario}")

    result.scenario = scenario
    print(json.dumps(asdict(result)))


def _run_worker(
    script: Path,
    root: Path,
    scenario: str,
    *,
    cpu: int | None,
) -> WorkerResult:
    env = os.environ.copy()
    source = str(root.resolve() / "src")
    env["PYTHONPATH"] = source
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--scenario",
        scenario,
    ]
    if cpu is not None:
        command.extend(["--cpu", str(cpu)])
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return WorkerResult(**json.loads(lines[-1]))


def _median(values: list[float]) -> float:
    return statistics.median(values)


def _parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    base = args.base_root.resolve()
    candidate = args.candidate_root.resolve()
    payload: dict[str, object] = {
        "base_root": str(base),
        "candidate_root": str(candidate),
        "repeat": args.repeat,
        "cpu": args.cpu,
        "scenarios": [],
    }

    for scenario in SCENARIOS:
        pairs: list[dict[str, object]] = []
        ratios: list[float] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured: dict[str, WorkerResult] = {}
            for variant in order:
                root = base if variant == "base" else candidate
                measured[variant] = _run_worker(script, root, scenario, cpu=args.cpu)
            base_rate = measured["base"].ops_per_s
            candidate_rate = measured["candidate"].ops_per_s
            ratio = candidate_rate / base_rate
            ratios.append(ratio)
            pairs.append(
                {
                    "order": list(order),
                    "base_ops_per_s": base_rate,
                    "candidate_ops_per_s": candidate_rate,
                    "candidate_over_base": ratio,
                }
            )
        scenario_result = {
            "name": scenario,
            "median_candidate_over_base": _median(ratios),
            "min_candidate_over_base": min(ratios),
            "max_candidate_over_base": max(ratios),
            "pairs": pairs,
        }
        payload["scenarios"].append(scenario_result)  # type: ignore[index]
        print(
            f"{scenario:24s} candidate/base={_median(ratios):.4f} "
            f"range=[{min(ratios):.4f}, {max(ratios):.4f}]"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--repeat", type=int, default=9)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-regression.json"))
    args = parser.parse_args()
    if args.worker and args.scenario is None:
        parser.error("--scenario is required with --worker")
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        _worker(arguments)
    else:
        _parent(arguments)
