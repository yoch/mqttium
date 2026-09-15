"""Paired delivery micro/profiling probe across RC14 and the current source tree.

The existing ``delivery_*`` microbenchmarks are component probes, not network
benchmarks. This script decomposes them further and measures iterator delivery
with the actual production consumption shape. Wall timings are collected without
a profiler; cProfile is used only for call attribution and call counts.
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import json
import os
import pstats
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from benchmark_support import client_options


TOPIC = "bench/sensors/temp"
MESSAGE_PAYLOAD = b"x"
SCENARIOS = (
    # Direct delivery seam. On RC14 this is the fallback callback queue path,
    # whereas the real steady-state EffectPump can dispatch sync callbacks inline.
    "handoff_callback_direct",
    # Adds the EffectPump / delivery-lane layer and therefore better approximates
    # the application-delivery slice of the reader path.
    "effect_callback",
    # Public iterator consumption. Unbounded mode should use each source's
    # unaccounted queue representation; bounded mode keeps exact byte accounting.
    "handoff_iterator_unbounded_production",
    "handoff_iterator_bounded_production",
    # Effect-level iterator probes with the same production-like consumption.
    "effect_iterator_unbounded_production",
    "effect_iterator_bounded_production",
)


def _pin(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def _scenario_shape(scenario: str) -> tuple[str, bool, bool]:
    mode = "callback" if "callback" in scenario else "iterator"
    bounded = "_bounded_" in scenario
    use_engine_effect = scenario.startswith("effect_")
    return mode, bounded, use_engine_effect


def _make_client(*, mode: str, bounded: bool) -> Any:
    from mqttium.api import AsyncClient

    wanted: dict[str, Any] = {
        "message_delivery": mode,
        "max_pending_callbacks": 100_000,
    }
    if mode == "iterator":
        wanted["max_iterator_messages"] = 100_000
        wanted["max_iterator_bytes"] = 64 * 1024 * 1024 if bounded else None
    client = AsyncClient(**client_options(AsyncClient, **wanted))
    if mode == "callback":
        client.on_message = lambda _message: None
    return client


async def _finish_callbacks(client: Any) -> None:
    delivery = client._delivery
    queue = getattr(delivery, "callback_queue", None)
    if queue is not None:
        await queue.join()
    shutdown = getattr(delivery, "shutdown_callbacks", None)
    if shutdown is not None:
        await shutdown(drain=False)


async def _apply_delivery_effect_compat(client: Any, effect: Any) -> None:
    apply_delivery = getattr(client, "_apply_delivery_effect", None)
    if apply_delivery is not None:
        pending = apply_delivery(effect, client._connection_epoch)
        if pending is not None:
            await pending
        return
    await client._apply_effect(effect, nowait=False, epoch=client._connection_epoch)


def _consume_iterator_message(client: Any) -> None:
    """Consume one iterator item with the source's production representation."""
    delivery = client._delivery
    item = delivery.messages_queue.get_nowait()

    release_nowait = getattr(delivery, "release_nowait", None)
    if release_nowait is not None:
        # RC14: unaccounted mode stores a bare Message; accounted mode stores
        # (Message, token). Its _DeliveryQueue deliberately has no join/task_done
        # bookkeeping.
        if isinstance(item, tuple):
            _message, token = item
            release_nowait(token)
        return

    # Current source follows the same shape after restoring the unaccounted
    # fast path: a bare Message when max_iterator_bytes=None, otherwise
    # (Message, logical_size). Neither public messages() implementation calls
    # task_done().
    if isinstance(item, tuple):
        _message, size = item
        delivery.release(size)


async def _drain_effect_compat(client: Any) -> None:
    collect = getattr(client, "_collect_effects_locked", None)
    if collect is not None:
        collect()
    else:
        client._effect_pump.collect_from_engine()
    drain = getattr(client, "_drain_effects", None)
    if drain is not None:
        await drain()
    else:
        await client._effect_pump.drain()
    lane = getattr(client, "_delivery_lane", None)
    if lane is not None:
        await lane.drain()


async def _exercise(scenario: str, operations: int) -> None:
    from mqttium.protocol.effects import EffectKind, EngineEffect
    from mqttium.types import Message

    mode, bounded, use_engine_effect = _scenario_shape(scenario)
    client = _make_client(mode=mode, bounded=bounded)
    message = Message(topic=TOPIC, payload=MESSAGE_PAYLOAD)
    effect = EngineEffect(EffectKind.MESSAGE, message)

    try:
        for _ in range(operations):
            if use_engine_effect:
                client._engine._emit(EffectKind.MESSAGE, message, requires_delivery_mark=False)
                await _drain_effect_compat(client)
            else:
                await _apply_delivery_effect_compat(client, effect)
            if mode == "iterator":
                _consume_iterator_message(client)
        if mode == "callback":
            await _finish_callbacks(client)
    finally:
        if mode != "callback":
            await _finish_callbacks(client)


def _timed(scenario: str, *, warmup: int, operations: int) -> dict[str, float | int]:
    asyncio.run(_exercise(scenario, warmup))
    started = time.perf_counter()
    asyncio.run(_exercise(scenario, operations))
    elapsed = time.perf_counter() - started
    return {
        "elapsed_s": elapsed,
        "operations": operations,
        "ops_per_s": operations / elapsed,
        "us_per_op": elapsed * 1_000_000 / operations,
    }


def _profile(scenario: str, operations: int) -> dict[str, Any]:
    profiler = cProfile.Profile()
    started = time.perf_counter()
    profiler.enable()
    asyncio.run(_exercise(scenario, operations))
    profiler.disable()
    elapsed = time.perf_counter() - started
    stats = pstats.Stats(profiler)
    rows: list[dict[str, Any]] = []
    for (filename, line, name), (
        primitive,
        calls,
        self_s,
        cumulative_s,
        _callers,
    ) in stats.stats.items():
        if (
            "mqttium" not in filename
            and "asyncio" not in filename
            and "paired_delivery_profile.py" not in filename
        ):
            continue
        rows.append(
            {
                "function": f"{Path(filename).name}:{line}:{name}",
                "primitive_calls": primitive,
                "calls": calls,
                "calls_per_op": calls / operations,
                "self_s": self_s,
                "cumulative_s": cumulative_s,
                "self_us_per_op": self_s * 1_000_000 / operations,
                "cumulative_us_per_op": cumulative_s * 1_000_000 / operations,
            }
        )
    rows.sort(key=lambda row: row["cumulative_s"], reverse=True)
    return {
        "operations": operations,
        "profile_wall_s": elapsed,
        "top_cumulative": rows[:50],
    }


def _worker(args: argparse.Namespace) -> None:
    _pin(args.cpu)
    payload = {
        "scenario": args.scenario,
        "timing": _timed(args.scenario, warmup=args.warmup, operations=args.operations),
        "profile": _profile(args.scenario, args.profile_operations),
    }
    print(json.dumps(payload))


def _run_worker(
    script: Path, root: Path, scenario: str, args: argparse.Namespace
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--scenario",
        scenario,
        "--warmup",
        str(args.warmup),
        "--operations",
        str(args.operations),
        "--profile-operations",
        str(args.profile_operations),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    completed = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    output: dict[str, Any] = {
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "repeat": args.repeat,
        "cpu": args.cpu,
        "scenarios": [],
    }
    for scenario in SCENARIOS:
        pairs: list[dict[str, Any]] = []
        ratios: list[float] = []
        last_profile: dict[str, Any] = {}
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured = {
                variant: _run_worker(script, roots[variant], scenario, args) for variant in order
            }
            base_rate = measured["base"]["timing"]["ops_per_s"]
            candidate_rate = measured["candidate"]["timing"]["ops_per_s"]
            ratio = candidate_rate / base_rate
            ratios.append(ratio)
            pairs.append(
                {
                    "order": list(order),
                    "base": measured["base"]["timing"],
                    "candidate": measured["candidate"]["timing"],
                    "candidate_over_base": ratio,
                }
            )
            last_profile = {
                "base": measured["base"]["profile"],
                "candidate": measured["candidate"]["profile"],
            }
        median_ratio = statistics.median(ratios)
        output["scenarios"].append(
            {
                "name": scenario,
                "median_candidate_over_base": median_ratio,
                "min_candidate_over_base": min(ratios),
                "max_candidate_over_base": max(ratios),
                "pairs": pairs,
                "profile": last_profile,
            }
        )
        base_us = statistics.median(pair["base"]["us_per_op"] for pair in pairs)
        cand_us = statistics.median(pair["candidate"]["us_per_op"] for pair in pairs)
        print(
            f"{scenario:42s} candidate/base={median_ratio:.4f} "
            f"base={base_us:.3f}us candidate={cand_us:.3f}us"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=2_000)
    parser.add_argument("--operations", type=int, default=30_000)
    parser.add_argument("--profile-operations", type=int, default=5_000)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-delivery-profile.json"))
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
