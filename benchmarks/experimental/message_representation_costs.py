from __future__ import annotations

import argparse
import dataclasses
import gc
import json
import os
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections import namedtuple
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

from mqttium.enums import QoS
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message

TOPIC = "bench/message/telemetry"
PAYLOAD = b"x" * 256
COUNT_BATCH = 256


@dataclasses.dataclass(slots=True)
class MutableSlotsMessage:
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    dup: bool
    mid: int | None
    properties: None
    delivery_logical_bytes: int = 0
    delivery_references: int = 0


@dataclasses.dataclass(slots=True, frozen=True)
class FrozenSevenMessage:
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    dup: bool
    mid: int | None
    properties: None


class PlainSlotsMessage:
    __slots__ = (
        "topic",
        "payload",
        "qos",
        "retain",
        "dup",
        "mid",
        "properties",
        "delivery_logical_bytes",
        "delivery_references",
    )

    def __init__(self) -> None:
        self.topic = TOPIC
        self.payload = PAYLOAD
        self.qos = QoS.AT_MOST_ONCE
        self.retain = False
        self.dup = False
        self.mid = None
        self.properties = None
        self.delivery_logical_bytes = 0
        self.delivery_references = 0


class MessageNamedTuple(NamedTuple):
    topic: str
    payload: bytes
    qos: QoS
    retain: bool
    dup: bool
    mid: int | None
    properties: None


LegacyNamedTuple = namedtuple(
    "LegacyNamedTuple",
    "topic payload qos retain dup mid properties",
)


def _current(_index: int) -> Message:
    return Message(
        topic=TOPIC,
        payload=PAYLOAD,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        mid=None,
        properties=None,
    )


def _current_effect(index: int) -> EngineEffect:
    return EngineEffect(EffectKind.MESSAGE, _current(index))


def _mutable_slots(_index: int) -> MutableSlotsMessage:
    return MutableSlotsMessage(
        TOPIC,
        PAYLOAD,
        QoS.AT_MOST_ONCE,
        False,
        False,
        None,
        None,
    )


def _frozen_seven(_index: int) -> FrozenSevenMessage:
    return FrozenSevenMessage(
        TOPIC,
        PAYLOAD,
        QoS.AT_MOST_ONCE,
        False,
        False,
        None,
        None,
    )


def _plain_slots(_index: int) -> PlainSlotsMessage:
    return PlainSlotsMessage()


def _named_tuple(_index: int) -> MessageNamedTuple:
    return MessageNamedTuple(
        TOPIC,
        PAYLOAD,
        QoS.AT_MOST_ONCE,
        False,
        False,
        None,
        None,
    )


def _legacy_named_tuple(_index: int) -> Any:
    return LegacyNamedTuple(
        TOPIC,
        PAYLOAD,
        QoS.AT_MOST_ONCE,
        False,
        False,
        None,
        None,
    )


def _plain_tuple(_index: int) -> tuple[Any, ...]:
    return (
        TOPIC,
        PAYLOAD,
        QoS.AT_MOST_ONCE,
        False,
        False,
        None,
        None,
    )


def _minimal_descriptor(_index: int) -> tuple[str, bytes, QoS, bool]:
    return TOPIC, PAYLOAD, QoS.AT_MOST_ONCE, False


FACTORIES: dict[str, Callable[[int], Any]] = {
    "current_message": _current,
    "current_message_effect": _current_effect,
    "mutable_slots_dataclass": _mutable_slots,
    "frozen_seven_dataclass": _frozen_seven,
    "plain_slots_class": _plain_slots,
    "typing_namedtuple": _named_tuple,
    "collections_namedtuple": _legacy_named_tuple,
    "plain_tuple": _plain_tuple,
    "minimal_descriptor_tuple": _minimal_descriptor,
}
STAGES = tuple(FACTORIES)


def _worker(stage: str, count: int, trace: bool) -> dict[str, Any]:
    factory = FACTORIES[stage]
    gc.collect()
    before_gc = [dict(item) for item in gc.get_stats()]
    if trace:
        tracemalloc.start()
    started = time.perf_counter_ns()
    sink: list[Any] = []
    produced = 0
    while produced < count:
        take = min(COUNT_BATCH, count - produced)
        for index in range(take):
            sink.append(factory(produced + index))
        produced += take
        sink.clear()
    elapsed_ns = time.perf_counter_ns() - started
    peak = None
    if trace:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    after_gc = gc.get_stats()
    sample = factory(0)
    return {
        "stage": stage,
        "count": count,
        "elapsed_ns": elapsed_ns,
        "ns_per_item": elapsed_ns / count,
        "rate": count / (elapsed_ns / 1_000_000_000),
        "trace_peak_bytes": peak,
        "shallow_size_bytes": sys.getsizeof(sample),
        "gc_collections": [
            after_gc[generation]["collections"] - before_gc[generation]["collections"]
            for generation in range(len(after_gc))
        ],
    }


def _sample(root: Path, stage: str, count: int, trace: bool) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--stage",
        stage,
        "--count",
        str(count),
    ]
    if trace:
        command.append("--trace")
    process = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    line = next(
        item.removeprefix("RESULT ")
        for item in reversed(process.stdout.splitlines())
        if item.startswith("RESULT ")
    )
    return json.loads(line)


def _driver(root: Path, cycles: int, count: int, output: Path) -> None:
    rows: list[dict[str, Any]] = []
    for cycle in range(cycles):
        shift = cycle % len(STAGES)
        rotated = STAGES[shift:] + STAGES[:shift]
        order = rotated if cycle % 2 == 0 else tuple(reversed(rotated))
        samples = {stage: _sample(root, stage, count, False) for stage in order}
        rows.append({"cycle": cycle, "order": order, "samples": samples})
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"rows": rows}, indent=2) + "\n")
    traces = {stage: _sample(root, stage, min(count, 50_000), True) for stage in STAGES}
    medians = {
        stage: {
            "ns_per_item": statistics.median(row["samples"][stage]["ns_per_item"] for row in rows),
            "rate": statistics.median(row["samples"][stage]["rate"] for row in rows),
            "shallow_size_bytes": traces[stage]["shallow_size_bytes"],
            "trace_peak_bytes": traces[stage]["trace_peak_bytes"],
            "gc_collections": [
                statistics.median(
                    row["samples"][stage]["gc_collections"][generation] for row in rows
                )
                for generation in range(3)
            ],
        }
        for stage in STAGES
    }
    output.write_text(
        json.dumps({"rows": rows, "traces": traces, "medians": medians}, indent=2) + "\n"
    )
    print(json.dumps(medians, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--stage", choices=STAGES)
    parser.add_argument("--count", type=int, default=1_000_000)
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--cycles", type=int, default=7)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker:
        if args.stage is None:
            parser.error("--worker requires --stage")
        print("RESULT " + json.dumps(_worker(args.stage, args.count, args.trace)))
        return
    if args.root is None or args.output is None:
        parser.error("driver mode requires --root and --output")
    _driver(args.root.resolve(), args.cycles, args.count, args.output)


if __name__ == "__main__":
    main()
