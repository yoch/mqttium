from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import statistics
import struct
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import unpack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine
from mqttium.topics import validate_received_publish_topic
from mqttium.types import Message

TOPIC = "bench/native-hotpath/telemetry"
PAYLOAD = b"t" * 256
PROTOCOL = MQTTProtocolVersion.MQTTv311


def _vbi(value: int) -> bytes:
    out = bytearray()
    while True:
        value, digit = divmod(value, 128)
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


_TOPIC_BYTES = TOPIC.encode()
_BODY = struct.pack("!H", len(_TOPIC_BYTES)) + _TOPIC_BYTES + PAYLOAD
_FRAME = b"\x30" + _vbi(len(_BODY)) + _BODY
_PER_CHUNK = max(1, (256 * 1024) // len(_FRAME))
_FULL_CHUNK = _FRAME * _PER_CHUNK


class Stage:
    def handle(self, raw: RawPacket) -> None:
        raise NotImplementedError

    def after_batch(self) -> None:
        return


class RawStage(Stage):
    def handle(self, raw: RawPacket) -> None:
        _ = raw.flags


class ParseStage(Stage):
    def handle(self, raw: RawPacket) -> None:
        topic, payload_pos = unpack_utf8(raw.remaining)
        _ = topic, raw.remaining[payload_pos:]


class ValidatedStage(Stage):
    def handle(self, raw: RawPacket) -> None:
        topic, payload_pos = unpack_utf8(raw.remaining)
        validate_received_publish_topic(topic, utf8_validated=True)
        _ = raw.remaining[payload_pos:]


def _message(raw: RawPacket) -> Message:
    topic, payload_pos = unpack_utf8(raw.remaining)
    validate_received_publish_topic(topic, utf8_validated=True)
    return Message(
        topic=topic,
        payload=raw.remaining[payload_pos:],
        qos=QoS.AT_MOST_ONCE,
        retain=bool(raw.flags & 0x01),
        dup=False,
        mid=None,
        properties=None,
    )


class MessageStage(Stage):
    def __init__(self) -> None:
        self.sink: list[Message] = []

    def handle(self, raw: RawPacket) -> None:
        self.sink.append(_message(raw))

    def after_batch(self) -> None:
        self.sink.clear()


class EffectStage(Stage):
    def __init__(self) -> None:
        self.sink: list[EngineEffect] = []

    def handle(self, raw: RawPacket) -> None:
        self.sink.append(EngineEffect(EffectKind.MESSAGE, _message(raw)))

    def after_batch(self) -> None:
        self.sink.clear()


class DirectDescriptorStage(Stage):
    def __init__(self) -> None:
        self.sink: list[Message] = []

    def handle(self, raw: RawPacket) -> None:
        self.sink.append(_message(raw))

    def after_batch(self) -> None:
        self.sink.clear()


class CallbackJobStage(Stage):
    def __init__(self) -> None:
        self.sink: list[tuple[Callable[[Message], None], tuple[Message], None]] = []

    @staticmethod
    def _callback(message: Message) -> None:
        _ = message

    def handle(self, raw: RawPacket) -> None:
        message = _message(raw)
        self.sink.append((self._callback, (message,), None))

    def after_batch(self) -> None:
        self.sink.clear()


class QueueMessageStage(Stage):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[Message] = asyncio.Queue()

    def handle(self, raw: RawPacket) -> None:
        self.queue.put_nowait(_message(raw))
        self.queue.get_nowait()
        self.queue.task_done()


class QueueCallbackJobStage(Stage):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[Callable[[Message], None], tuple[Message], None]] = (
            asyncio.Queue()
        )

    @staticmethod
    def _callback(message: Message) -> None:
        _ = message

    def handle(self, raw: RawPacket) -> None:
        message = _message(raw)
        self.queue.put_nowait((self._callback, (message,), None))
        self.queue.get_nowait()
        self.queue.task_done()


class InboundStage(Stage):
    def __init__(self) -> None:
        self.engine = ProtocolEngine(EngineConfig(protocol=PROTOCOL))
        self.engine.state = ConnectionState.CONNECTED

    def handle(self, raw: RawPacket) -> None:
        self.engine.inbound.on_publish(raw)

    def after_batch(self) -> None:
        self.engine.take_effects()


class EngineStage(Stage):
    def __init__(self) -> None:
        self.engine = ProtocolEngine(EngineConfig(protocol=PROTOCOL))
        self.engine.state = ConnectionState.CONNECTED

    def handle(self, raw: RawPacket) -> None:
        self.engine.handle_raw(raw)

    def after_batch(self) -> None:
        self.engine.take_effects()


STAGES: dict[str, Callable[[], Stage]] = {
    "decoder_raw": RawStage,
    "parse": ParseStage,
    "parse_validated": ValidatedStage,
    "message": MessageStage,
    "effect": EffectStage,
    "direct_descriptor": DirectDescriptorStage,
    "callback_job": CallbackJobStage,
    "queue_message": QueueMessageStage,
    "queue_callback_job": QueueCallbackJobStage,
    "inbound": InboundStage,
    "engine": EngineStage,
}


def _worker(stage_name: str, count: int, trace: bool) -> dict[str, Any]:
    decoder = IncrementalDecoder()
    stage = STAGES[stage_name]()
    remaining = count
    seen = 0
    gc.collect()
    before_gc = [dict(item) for item in gc.get_stats()]
    if trace:
        tracemalloc.start()
    started = time.perf_counter_ns()
    while remaining:
        take = min(_PER_CHUNK, remaining)
        decoder.feed(_FULL_CHUNK if take == _PER_CHUNK else _FRAME * take)
        remaining -= take
        while decoder.buffered:
            handled, _ = decoder.process_packets_bounded(
                stage.handle,
                limit=256,
                max_bytes=1 << 20,
            )
            if not handled:
                break
            seen += handled
            stage.after_batch()
    elapsed_ns = time.perf_counter_ns() - started
    if seen != count:
        raise AssertionError(f"expected {count} packets, observed {seen}")
    peak = None
    if trace:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    after_gc = gc.get_stats()
    collections = [
        after_gc[index]["collections"] - before_gc[index]["collections"]
        for index in range(len(after_gc))
    ]
    return {
        "stage": stage_name,
        "count": count,
        "elapsed_ns": elapsed_ns,
        "rate": count / (elapsed_ns / 1_000_000_000),
        "ns_per_message": elapsed_ns / count,
        "trace_peak_bytes": peak,
        "gc_collections": collections,
        "decoder_high_water": decoder.high_water,
    }


def _sample(
    *,
    root: Path,
    stage: str,
    count: int,
    trace: bool,
) -> dict[str, Any]:
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
    proc = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    line = next(
        item.removeprefix("RESULT ")
        for item in reversed(proc.stdout.splitlines())
        if item.startswith("RESULT ")
    )
    return json.loads(line)


def _driver(root: Path, cycles: int, count: int, output: Path) -> None:
    labels = tuple(STAGES)
    rows: list[dict[str, Any]] = []
    for cycle in range(cycles):
        shift = cycle % len(labels)
        rotated = labels[shift:] + labels[:shift]
        order = rotated if cycle % 2 == 0 else tuple(reversed(rotated))
        samples = {
            stage: _sample(root=root, stage=stage, count=count, trace=False) for stage in order
        }
        rows.append({"cycle": cycle, "order": order, "samples": samples})
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({"rows": rows}, indent=2) + "\n")
    traces = {
        stage: _sample(
            root=root,
            stage=stage,
            count=min(count, 50_000),
            trace=True,
        )
        for stage in labels
    }
    medians = {
        stage: {
            "rate": statistics.median(row["samples"][stage]["rate"] for row in rows),
            "ns_per_message": statistics.median(
                row["samples"][stage]["ns_per_message"] for row in rows
            ),
            "gc_collections": [
                statistics.median(
                    row["samples"][stage]["gc_collections"][generation] for row in rows
                )
                for generation in range(3)
            ],
            "trace_peak_bytes": traces[stage]["trace_peak_bytes"],
        }
        for stage in labels
    }
    result = {"rows": rows, "traces": traces, "medians": medians}
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--stage", choices=tuple(STAGES))
    parser.add_argument("--count", type=int, default=300_000)
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
