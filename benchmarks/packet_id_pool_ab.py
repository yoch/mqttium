"""A/B benchmark for the PacketIdPool representation.

The benchmark isolates allocator CPU cost and retained Python memory. Results
are build artefacts: CI uploads JSON but no generated measurement is committed.
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
import sys
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from mqttium.errors import FlowControlError
from mqttium.protocol.packet_ids import PacketIdPool


class LegacyPacketIdPool:
    """The set-plus-free-list allocator replaced by PacketIdPool."""

    __slots__ = ("_used", "_free", "_next", "_size")

    def __init__(self) -> None:
        self._used: set[int] = set()
        self._free: list[int] = []
        self._next = 1
        self._size = 65_535

    def allocate(self) -> int:
        if len(self._used) >= self._size:
            raise FlowControlError("No free MQTT packet identifiers")
        if self._free:
            mid = self._free.pop()
            self._used.add(mid)
            return mid
        while self._next <= self._size and self._next in self._used:
            self._next += 1
        if self._next <= self._size:
            mid = self._next
            self._next += 1
            self._used.add(mid)
            return mid
        for candidate in range(1, self._size + 1):
            if candidate not in self._used:
                self._used.add(candidate)
                return candidate
        raise FlowControlError("No free MQTT packet identifiers")

    def reserve(self, mid: int) -> None:
        self._used.add(mid)
        with suppress(ValueError):
            self._free.remove(mid)

    def release(self, mid: int) -> None:
        if mid not in self._used:
            return
        self._used.remove(mid)
        if not self._used:
            self._used = set()
            self._free = []
            self._next = 1
            return
        self._free.append(mid)


@dataclass(frozen=True, slots=True)
class Timing:
    operation: str
    operations: int
    legacy_ns_per_op: float
    frontier_ns_per_op: float
    frontier_over_legacy: float


@dataclass(frozen=True, slots=True)
class Memory:
    state: str
    live_ids: int
    legacy_bytes: int
    frontier_bytes: int
    frontier_over_legacy: float


def _median_ns(operation: Callable[[], int], operations: int, repeat: int) -> float:
    samples: list[float] = []
    for _ in range(repeat):
        gc.collect()
        started = time.perf_counter_ns()
        completed = operation()
        elapsed = time.perf_counter_ns() - started
        if completed != operations:
            raise AssertionError(f"expected {operations} operations, got {completed}")
        samples.append(elapsed / operations)
    return statistics.median(samples)


def _sequential_allocate(pool_type: type, count: int) -> Callable[[], int]:
    def run() -> int:
        pool = pool_type()
        for _ in range(count):
            pool.allocate()
        return count

    return run


def _sequential_reserve(pool_type: type, count: int) -> Callable[[], int]:
    def run() -> int:
        pool = pool_type()
        for mid in range(1, count + 1):
            pool.reserve(mid)
        return count

    return run


def _steady_reuse(pool_type: type, window: int, count: int) -> Callable[[], int]:
    def run() -> int:
        pool = pool_type()
        mids = [pool.allocate() for _ in range(window)]
        for index in range(count):
            slot = index % window
            pool.release(mids[slot])
            mids[slot] = pool.allocate()
        return count

    return run


def _sequential_drain(pool_type: type, count: int) -> Callable[[], int]:
    def run() -> int:
        pool = pool_type()
        mids = [pool.allocate() for _ in range(count)]
        for mid in mids:
            pool.release(mid)
        return count

    return run


def _deep_size(value: Any, seen: set[int] | None = None) -> int:
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        size += sum(_deep_size(key, seen) + _deep_size(item, seen) for key, item in value.items())
    elif isinstance(value, (list, tuple, set, frozenset)):
        size += sum(_deep_size(item, seen) for item in value)
    else:
        for slot in getattr(type(value), "__slots__", ()):
            if hasattr(value, slot):
                size += _deep_size(getattr(value, slot), seen)
    return size


def _memory_case(state: str, count: int, released: int) -> Memory:
    legacy = LegacyPacketIdPool()
    frontier = PacketIdPool()
    mids: list[int] = []
    for _ in range(count):
        left = legacy.allocate()
        right = frontier.allocate()
        if left != right:
            raise AssertionError((left, right))
        mids.append(left)
    for mid in mids[:released]:
        legacy.release(mid)
        frontier.release(mid)
    legacy_bytes = _deep_size(legacy)
    frontier_bytes = _deep_size(frontier)
    return Memory(
        state=state,
        live_ids=count - released,
        legacy_bytes=legacy_bytes,
        frontier_bytes=frontier_bytes,
        frontier_over_legacy=frontier_bytes / legacy_bytes,
    )


def run(count: int, repeat: int) -> dict[str, Any]:
    operations: list[tuple[str, int, Callable[[], int], Callable[[], int]]] = [
        (
            "sequential_allocate",
            count,
            _sequential_allocate(LegacyPacketIdPool, count),
            _sequential_allocate(PacketIdPool, count),
        ),
        (
            "sequential_reserve",
            count,
            _sequential_reserve(LegacyPacketIdPool, count),
            _sequential_reserve(PacketIdPool, count),
        ),
        (
            "steady_reuse_window_1",
            count,
            _steady_reuse(LegacyPacketIdPool, 1, count),
            _steady_reuse(PacketIdPool, 1, count),
        ),
        (
            "steady_reuse_window_16",
            count,
            _steady_reuse(LegacyPacketIdPool, 16, count),
            _steady_reuse(PacketIdPool, 16, count),
        ),
        (
            "steady_reuse_window_1000",
            count,
            _steady_reuse(LegacyPacketIdPool, 1_000, count),
            _steady_reuse(PacketIdPool, 1_000, count),
        ),
        (
            "sequential_drain",
            count,
            _sequential_drain(LegacyPacketIdPool, count),
            _sequential_drain(PacketIdPool, count),
        ),
    ]
    timings: list[Timing] = []
    for name, operation_count, legacy, frontier in operations:
        legacy_ns = _median_ns(legacy, operation_count, repeat)
        frontier_ns = _median_ns(frontier, operation_count, repeat)
        timings.append(
            Timing(
                operation=name,
                operations=operation_count,
                legacy_ns_per_op=legacy_ns,
                frontier_ns_per_op=frontier_ns,
                frontier_over_legacy=frontier_ns / legacy_ns,
            )
        )

    memory = [
        _memory_case("idle", 0, 0),
        _memory_case("loaded", count, 0),
        _memory_case("half_released", count, count // 2),
    ]
    return {
        "python": sys.version,
        "count": count,
        "repeat": repeat,
        "timings": [asdict(item) for item in timings],
        "memory": [asdict(item) for item in memory],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=30_000)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = run(args.count, args.repeat)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n")


if __name__ == "__main__":
    main()
