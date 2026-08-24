"""Bounded exploration over cooperative scheduler decisions.

This is the campaign counterpart of the compact replay tests. It does not
mutate production code. A failing unique schedule can be copied into a
focused pytest.

Run a small campaign::

    PYTHONPATH=src:. python tests/concurrency/explore.py --seed 1 --max-schedules 40
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from mqttium.api._effects import StaleConnectionEffect
from mqttium.errors import MQTTError

from tests.concurrency.scheduler import PrefixChooser, RandomChooser, RunResult
from tests.concurrency.scenarios import WRITE_BOUNDARIES, publish_one, run_connected_scenario

_EXPECTED_EXCEPTIONS = (
    MQTTError,
    StaleConnectionEffect,
    ConnectionError,
    OSError,
    asyncio.CancelledError,
    TimeoutError,
)


@dataclass
class ExploreStats:
    runs: int = 0
    unique_schedules: int = 0
    ok: int = 0
    timeouts: int = 0
    deadlocks: int = 0
    errors: int = 0
    max_steps: int = 0
    branching: list[int] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)

    def format(self) -> str:
        mean_branch = sum(self.branching) / len(self.branching) if self.branching else 0.0
        lines = [
            f"runs={self.runs}",
            f"unique_schedules={self.unique_schedules}",
            f"ok={self.ok} timeouts={self.timeouts} deadlocks={self.deadlocks} "
            f"errors={self.errors}",
            f"max_steps={self.max_steps} mean_branching={mean_branch:.2f}",
        ]
        if self.failures:
            lines.append("failures:")
            for kind, schedule in self.failures[:8]:
                lines.append(f"  [{kind}]\n{schedule}")
        return "\n".join(lines) + "\n"


def _classify(result: RunResult) -> str:
    if result.timed_out:
        return "timeout"
    if result.deadlock:
        return "deadlock"
    if result.error is None:
        return "ok"
    if isinstance(result.error, AssertionError):
        return "error"
    if isinstance(result.error, _EXPECTED_EXCEPTIONS):
        return "ok"
    return "error"
    if result.timed_out:
        return "timeout"
    if result.deadlock:
        return "deadlock"
    if result.error is not None:
        return "error"
    return "ok"


async def explore_dfs(
    *,
    max_schedules: int = 40,
    max_depth: int = 6,
    enabled: frozenset[str] = WRITE_BOUNDARIES,
    actors: Callable[..., Sequence[tuple[str, Any]]] = publish_one,
    auto_ack: bool = True,
    **client_kwargs: Any,
) -> ExploreStats:
    stats = ExploreStats()
    seen: set[str] = set()
    queue: list[list[int]] = [[]]
    while queue and stats.runs < max_schedules:
        prefix = queue.pop()
        if len(prefix) > max_depth:
            continue
        chooser = PrefixChooser(prefix)
        result, _harness = await run_connected_scenario(
            actors,
            enabled=enabled,
            chooser=chooser,
            auto_ack=auto_ack,
            **client_kwargs,
        )
        stats.runs += 1
        rendered = result.schedule.format()
        if rendered not in seen:
            seen.add(rendered)
            stats.unique_schedules += 1
        stats.max_steps = max(stats.max_steps, result.steps)
        stats.branching.extend(chooser.branching)
        kind = _classify(result)
        if kind == "ok":
            stats.ok += 1
        elif kind == "timeout":
            stats.timeouts += 1
            stats.failures.append((kind, rendered))
        elif kind == "deadlock":
            stats.deadlocks += 1
            stats.failures.append((kind, rendered))
        else:
            stats.errors += 1
            stats.failures.append((kind, rendered))
        if len(prefix) >= max_depth:
            continue
        if len(chooser.branching) <= len(prefix):
            continue
        n_options = chooser.branching[len(prefix)]
        for index in range(n_options - 1, -1, -1):
            queue.append([*prefix, index])
    return stats


async def explore_random(
    *,
    seed: int = 1,
    max_schedules: int = 40,
    enabled: frozenset[str] = WRITE_BOUNDARIES,
    actors: Callable[..., Sequence[tuple[str, Any]]] = publish_one,
    auto_ack: bool = True,
    **client_kwargs: Any,
) -> ExploreStats:
    stats = ExploreStats()
    seen: set[str] = set()
    rng = random.Random(seed)
    for _ in range(max_schedules):
        chooser = RandomChooser(rng)
        result, _harness = await run_connected_scenario(
            actors,
            enabled=enabled,
            chooser=chooser,
            auto_ack=auto_ack,
            **client_kwargs,
        )
        stats.runs += 1
        rendered = result.schedule.format()
        if rendered not in seen:
            seen.add(rendered)
            stats.unique_schedules += 1
        stats.max_steps = max(stats.max_steps, result.steps)
        kind = _classify(result)
        if kind == "ok":
            stats.ok += 1
        elif kind == "timeout":
            stats.timeouts += 1
            stats.failures.append((kind, rendered))
        elif kind == "deadlock":
            stats.deadlocks += 1
            stats.failures.append((kind, rendered))
        else:
            stats.errors += 1
            stats.failures.append((kind, rendered))
    return stats


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MQTTium cooperative concurrency explorer")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--max-schedules", type=int, default=40)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--policy", choices=("dfs", "random"), default="dfs")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.policy == "dfs":
        stats = asyncio.run(explore_dfs(max_schedules=args.max_schedules, max_depth=args.max_depth))
    else:
        stats = asyncio.run(explore_random(seed=args.seed, max_schedules=args.max_schedules))
    sys.stderr.write(stats.format())
    return 1 if stats.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
