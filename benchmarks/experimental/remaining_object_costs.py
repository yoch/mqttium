from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message

TOPIC = "bench/remaining-costs/telemetry"
PAYLOAD = b"x" * 256
BODY = b"prefix:" + PAYLOAD + b":suffix"
BATCH = 256


def _repeat_batched(count: int, factory: Callable[[int], Any]) -> int:
    sink: list[Any] = []
    produced = 0
    while produced < count:
        take = min(BATCH, count - produced)
        for offset in range(take):
            sink.append(factory(produced + offset))
        produced += take
        sink.clear()
    return produced


def _event(index: int) -> asyncio.Event:
    _ = index
    return asyncio.Event()


def _qos0_receipt(index: int) -> PublishReceipt:
    _ = index
    return PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE, _event=None)


def _qos1_receipt(index: int) -> PublishReceipt:
    return PublishReceipt(
        mid=(index % 65_535) + 1,
        qos=QoS.AT_LEAST_ONCE,
        _event=asyncio.Event(),
    )


def _message(index: int) -> Message:
    _ = index
    return Message(
        topic=TOPIC,
        payload=PAYLOAD,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        mid=None,
        properties=None,
    )


def _effect(index: int) -> EngineEffect:
    return EngineEffect(EffectKind.MESSAGE, _message(index))


def _callback_tuple(index: int) -> tuple[str, bytes, int, None]:
    _ = index
    return TOPIC, PAYLOAD, 0, None


def _bytes_slice(index: int) -> bytes:
    _ = index
    return BODY[7:-7]


def _memoryview_slice(index: int) -> memoryview:
    _ = index
    return memoryview(BODY)[7:-7]


def _register_pop(count: int) -> int:
    client = AsyncClient(client_id="remaining-cost-register-pop")
    for index in range(count):
        mid = (index % 65_535) + 1
        receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
        client._register_publish_receipt(mid, receipt)
        popped = client._pop_publish_receipt(mid)
        if popped is not receipt:
            raise AssertionError("receipt FIFO mismatch")
    return count


def _register_settle(count: int) -> int:
    client = AsyncClient(client_id="remaining-cost-register-settle")
    for index in range(count):
        mid = (index % 65_535) + 1
        receipt = PublishReceipt(mid=mid, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
        client._register_publish_receipt(mid, receipt)
        client._settle_publish(mid, None)
        if receipt._event is None or not receipt._event.is_set():
            raise AssertionError("receipt was not settled")
    return count


def _same_mid_fifo(count: int) -> int:
    client = AsyncClient(client_id="remaining-cost-same-mid")
    produced = 0
    while produced < count:
        take = min(64, count - produced)
        receipts = [
            PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE, _event=asyncio.Event())
            for _ in range(take)
        ]
        for receipt in receipts:
            client._register_publish_receipt(1, receipt)
        for expected in receipts:
            if client._pop_publish_receipt(1) is not expected:
                raise AssertionError("same-mid FIFO mismatch")
        produced += take
    return produced


FACTORIES: dict[str, Callable[[int], Any]] = {
    "event": _event,
    "qos0_receipt": _qos0_receipt,
    "qos1_receipt": _qos1_receipt,
    "message_reused_fields": _message,
    "effect_reused_fields": _effect,
    "callback_tuple": _callback_tuple,
    "bytes_slice": _bytes_slice,
    "memoryview_slice": _memoryview_slice,
}
SPECIAL: dict[str, Callable[[int], int]] = {
    "receipt_register_pop": _register_pop,
    "receipt_register_settle": _register_settle,
    "receipt_same_mid_fifo": _same_mid_fifo,
}
STAGES = tuple(FACTORIES) + tuple(SPECIAL)


def _worker(stage: str, count: int, trace: bool) -> dict[str, Any]:
    gc.collect()
    before = [dict(item) for item in gc.get_stats()]
    if trace:
        tracemalloc.start()
    started = time.perf_counter_ns()
    if stage in FACTORIES:
        produced = _repeat_batched(count, FACTORIES[stage])
    else:
        produced = SPECIAL[stage](count)
    elapsed_ns = time.perf_counter_ns() - started
    peak = None
    if trace:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    after = gc.get_stats()
    return {
        "stage": stage,
        "count": produced,
        "elapsed_ns": elapsed_ns,
        "ns_per_item": elapsed_ns / produced,
        "rate": produced / (elapsed_ns / 1_000_000_000),
        "trace_peak_bytes": peak,
        "gc_collections": [
            after[generation]["collections"] - before[generation]["collections"]
            for generation in range(len(after))
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
    traces = {
        stage: _sample(root, stage, min(count, 50_000), True) for stage in STAGES
    }
    medians = {
        stage: {
            "ns_per_item": statistics.median(
                row["samples"][stage]["ns_per_item"] for row in rows
            ),
            "rate": statistics.median(row["samples"][stage]["rate"] for row in rows),
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
    parser.add_argument("--count", type=int, default=500_000)
    parser.add_argument("--trace", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--cycles", type=int, default=7)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker:
        if args.stage is None:
            parser.error("--worker requires --stage")
        result = asyncio.run(asyncio.to_thread(_worker, args.stage, args.count, args.trace))
        print("RESULT " + json.dumps(result))
        return
    if args.root is None or args.output is None:
        parser.error("driver mode requires --root and --output")
    _driver(args.root.resolve(), args.cycles, args.count, args.output)


if __name__ == "__main__":
    main()
