from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any


async def _measure(operations: int, warmup: int) -> dict[str, float]:
    from mqttium.api._effects import EffectPump
    from mqttium.protocol.effects import EffectKind, EngineEffect

    class Engine:
        def __init__(self) -> None:
            self.effects: list[EngineEffect] = []

        def take_effects(self) -> list[EngineEffect]:
            effects = self.effects
            self.effects = []
            return effects

    class Owner:
        def __init__(self) -> None:
            self._connection_epoch = 1
            self._disconnect_exc: BaseException | None = None
            self._engine = Engine()
            self._connack_fut = None

        def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool:
            del effect, epoch
            return False

        def _apply_message_effect_batch_inline(
            self, effects: deque[EngineEffect], epoch: int
        ) -> int:
            del effects, epoch
            return 0

        async def _apply_effect(
            self,
            effect: EngineEffect,
            *,
            nowait: bool,
            epoch: int | None = None,
        ) -> None:
            del effect, nowait, epoch
            # Force the scheduled flusher to suspend so drain() must actually
            # wait for progress rather than winning an inline/recheck race.
            await asyncio.sleep(0)

        async def _close_transport_after_connection_failure(self) -> None:
            return None

        def _settle_terminal_effect(self, effect: EngineEffect) -> None:
            del effect

    owner = Owner()
    pump = EffectPump(owner)  # type: ignore[arg-type]
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    created_tasks = 0

    def counting_factory(
        loop: asyncio.AbstractEventLoop,
        coro: Any,
        context: Any = None,
    ) -> asyncio.Task[Any]:
        nonlocal created_tasks
        created_tasks += 1
        if context is None:
            return asyncio.Task(coro, loop=loop)
        return asyncio.Task(coro, loop=loop, context=context)

    async def one() -> None:
        owner._engine.effects = [EngineEffect(EffectKind.SEND, b"x")]
        pump.collect_from_engine()
        await pump.drain()

    loop.set_task_factory(counting_factory)
    try:
        for _ in range(warmup):
            await one()
        await asyncio.sleep(0)
        created_tasks = 0
        started = time.perf_counter()
        for _ in range(operations):
            await one()
        elapsed = time.perf_counter() - started
        measured_tasks = created_tasks
    finally:
        loop.set_task_factory(previous_factory)

    await asyncio.sleep(0)
    assert not pump.pending
    assert pump.applied == pump.enqueued
    assert pump.waiters == 0
    return {
        "elapsed_s": elapsed,
        "ops_s": operations / elapsed,
        "tasks_per_op": measured_tasks / operations,
        "apply_suspensions": float(pump.apply_suspensions),
    }


def _worker(args: argparse.Namespace) -> None:
    print(json.dumps(asyncio.run(_measure(args.operations, args.warmup)), sort_keys=True))


def _parent(args: argparse.Namespace) -> None:
    labels = {"A1": args.base_root, "A2": args.base_root, "B": args.candidate_root}
    samples: dict[str, list[dict[str, float]]] = {label: [] for label in labels}
    script = str(Path(__file__).resolve())

    for repeat in range(args.repeat):
        order = ["A1", "A2", "B"]
        order = order[repeat % 3 :] + order[: repeat % 3]
        for label in order:
            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path(labels[label]) / "src")
            proc = subprocess.run(
                [
                    sys.executable,
                    script,
                    "--worker",
                    "--operations",
                    str(args.operations),
                    "--warmup",
                    str(args.warmup),
                ],
                env=env,
                text=True,
                capture_output=True,
                check=True,
            )
            sample = json.loads(proc.stdout.strip().splitlines()[-1])
            samples[label].append(sample)
            print(f"repeat={repeat + 1} label={label} {sample}", flush=True)

    a1 = [sample["ops_s"] for sample in samples["A1"]]
    a2 = [sample["ops_s"] for sample in samples["A2"]]
    b = [sample["ops_s"] for sample in samples["B"]]
    aa = [100.0 * (y / x - 1.0) for x, y in zip(a1, a2, strict=True)]
    paired = [
        100.0 * (candidate / ((x + y) / 2.0) - 1.0)
        for x, y, candidate in zip(a1, a2, b, strict=True)
    ]
    task_medians = {
        label: statistics.median(sample["tasks_per_op"] for sample in values)
        for label, values in samples.items()
    }
    summary = {
        "base_sha": args.base_sha,
        "candidate_sha": args.candidate_sha,
        "repeat": args.repeat,
        "operations": args.operations,
        "warmup": args.warmup,
        "aa_control_delta_pct_median": statistics.median(aa),
        "candidate_delta_pct_median_paired": statistics.median(paired),
        "base_ops_s_median": statistics.median(a1 + a2),
        "candidate_ops_s_median": statistics.median(b),
        "tasks_per_op": task_medians,
        "samples": samples,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    Path(args.output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--base-root")
    parser.add_argument("--candidate-root")
    parser.add_argument("--base-sha")
    parser.add_argument("--candidate-sha")
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--operations", type=int, default=20_000)
    parser.add_argument("--warmup", type=int, default=1_000)
    parser.add_argument("--output", default="pr440-effect-drain-benchmark.json")
    args = parser.parse_args()
    if args.worker:
        _worker(args)
    else:
        _parent(args)
