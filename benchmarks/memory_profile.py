"""Isolated memory profiling for MQTTium retention paths.

Each scenario runs in a fresh child process. This makes process peak RSS useful
and prevents one scenario from inheriting another scenario's allocator state.
The output combines operating-system memory, Python allocation tracing, and
logical MQTTium queue/store counters.
"""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import gc
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Coroutine

try:
    import resource
except ImportError:  # pragma: no cover - benchmark runs on Unix in CI
    resource = None  # type: ignore[assignment]

try:
    import psutil
except ImportError as exc:  # pragma: no cover - actionable local error
    raise SystemExit("memory_profile.py requires psutil>=6") from exc

from mqttium.api import AsyncClient
from mqttium.enums import OutboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EffectKind, EngineConfig, EngineEffect, ProtocolEngine
from mqttium.types import Message, OutboundMessage

_MIB = 1024 * 1024
_TOPIC = "bench/memory"


@dataclass(frozen=True, slots=True)
class ScenarioSpec:
    name: str
    runner: str
    count: int
    payload_size: int
    notes: str


SCENARIOS = (
    ScenarioSpec(
        name="protocol_qos_queue_64b",
        runner="protocol_qos_queue",
        count=30_000,
        payload_size=64,
        notes="Disconnected QoS 1 queue; emphasizes Python object/container overhead.",
    ),
    ScenarioSpec(
        name="protocol_qos_queue_4k",
        runner="protocol_qos_queue",
        count=12_000,
        payload_size=4_096,
        notes="Disconnected QoS 1 queue; emphasizes retained payload bytes.",
    ),
    ScenarioSpec(
        name="iterator_delivery_queue_4k",
        runner="iterator_delivery_queue",
        count=6_000,
        payload_size=4_096,
        notes="Unconsumed public iterator delivery queue at its configured capacity.",
    ),
    ScenarioSpec(
        name="memory_store_4k",
        runner="memory_store",
        count=12_000,
        payload_size=4_096,
        notes="Unique payloads retained by the in-memory inflight store.",
    ),
    ScenarioSpec(
        name="sqlite_hydration_4k",
        runner="sqlite_hydration",
        count=6_000,
        payload_size=4_096,
        notes="ProtocolEngine startup hydration from a durable queued session.",
    ),
)


class MemoryProbe:
    def __init__(self) -> None:
        self._process = psutil.Process()
        tracemalloc.start(25)

    def reset_python_peak(self) -> None:
        tracemalloc.reset_peak()

    def snapshot(self, phase: str, **logical: int | float | str | bool | None) -> dict[str, Any]:
        gc.collect()
        memory = self._process.memory_info()
        full = self._process.memory_full_info()
        traced_current, traced_peak = tracemalloc.get_traced_memory()
        return {
            "phase": phase,
            "rss_mib": memory.rss / _MIB,
            "uss_mib": _optional_mib(full, "uss"),
            "pss_mib": _optional_mib(full, "pss"),
            "max_rss_mib": _max_rss_mib(),
            "traced_current_mib": traced_current / _MIB,
            "traced_peak_mib": traced_peak / _MIB,
            "logical": logical,
        }


def _optional_mib(info: Any, name: str) -> float | None:
    value = getattr(info, name, None)
    return None if value is None else value / _MIB


def _max_rss_mib() -> float | None:
    if resource is None:
        return None
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return value / _MIB
    return value / 1024


def _malloc_trim() -> bool:
    if not sys.platform.startswith("linux"):
        return False
    try:
        trim = ctypes.CDLL(None).malloc_trim
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        return bool(trim(0))
    except (AttributeError, OSError):
        return False


def _payload(index: int, size: int) -> bytes:
    if size <= 0:
        return b""
    marker = index.to_bytes(8, "little", signed=False)
    if size <= len(marker):
        return marker[:size]
    return marker + bytes([index & 0xFF]) * (size - len(marker))


def _logical_message_bytes(payload_size: int) -> int:
    return payload_size + len(_TOPIC.encode("utf-8"))


def _outbound_message(mid: int, payload_size: int) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic=_TOPIC,
        payload=_payload(mid, payload_size),
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.QUEUED,
    )


def _message_effect(sequence: int, payload_size: int) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic=_TOPIC, payload=_payload(sequence, payload_size)),
    )


def _finalize(
    spec: ScenarioSpec,
    started: float,
    snapshots: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline = snapshots[0]
    for snapshot in snapshots:
        for field in ("rss_mib", "uss_mib", "pss_mib", "traced_current_mib"):
            current = snapshot[field]
            initial = baseline[field]
            snapshot[f"{field.removesuffix('_mib')}_delta_mib"] = (
                None if current is None or initial is None else current - initial
            )
    return {
        "name": spec.name,
        "runner": spec.runner,
        "count": spec.count,
        "payload_size": spec.payload_size,
        "logical_message_bytes": _logical_message_bytes(spec.payload_size),
        "logical_total_mib": (
            spec.count * _logical_message_bytes(spec.payload_size) / _MIB
        ),
        "seconds": time.perf_counter() - started,
        "notes": spec.notes,
        "snapshots": snapshots,
    }


def run_protocol_qos_queue(spec: ScenarioSpec) -> dict[str, Any]:
    probe = MemoryProbe()
    snapshots = [probe.snapshot("baseline")]
    probe.reset_python_peak()
    started = time.perf_counter()
    engine = ProtocolEngine(EngineConfig(max_queued=0))
    for index in range(spec.count):
        engine.queue_publish(
            _TOPIC,
            _payload(index, spec.payload_size),
            qos=QoS.AT_LEAST_ONCE,
        )
    snapshots.append(
        probe.snapshot(
            "loaded",
            queued_messages=len(engine._queued),
            queued_logical_bytes=(
                len(engine._queued) * _logical_message_bytes(spec.payload_size)
            ),
            packet_ids=len(engine.packet_ids),
            store_records=sum(1 for _ in engine.store.out_items()),
        )
    )
    engine._queued.clear()
    engine.store.clear_out()
    engine.packet_ids.clear()
    engine.take_effects()
    del engine
    snapshots.append(probe.snapshot("released"))
    trimmed = _malloc_trim()
    snapshots.append(probe.snapshot("released_after_malloc_trim", malloc_trim=trimmed))
    return _finalize(spec, started, snapshots)


async def _run_iterator_delivery_queue(spec: ScenarioSpec) -> dict[str, Any]:
    probe = MemoryProbe()
    snapshots = [probe.snapshot("baseline")]
    probe.reset_python_peak()
    started = time.perf_counter()
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=spec.count,
        delivery_timeout=30.0,
    )
    for index in range(spec.count):
        await client._apply_effect(_message_effect(index, spec.payload_size), nowait=False)
    snapshots.append(
        probe.snapshot(
            "loaded",
            iterator_messages=client._messages.qsize(),
            iterator_logical_bytes=(
                client._messages.qsize() * _logical_message_bytes(spec.payload_size)
            ),
        )
    )
    while not client._messages.empty():
        client._messages.get_nowait()
        client._messages.task_done()
    del client
    snapshots.append(probe.snapshot("released"))
    trimmed = _malloc_trim()
    snapshots.append(probe.snapshot("released_after_malloc_trim", malloc_trim=trimmed))
    return _finalize(spec, started, snapshots)


def run_iterator_delivery_queue(spec: ScenarioSpec) -> dict[str, Any]:
    return asyncio.run(_run_iterator_delivery_queue(spec))


def run_memory_store(spec: ScenarioSpec) -> dict[str, Any]:
    probe = MemoryProbe()
    snapshots = [probe.snapshot("baseline")]
    probe.reset_python_peak()
    started = time.perf_counter()
    store = MemoryInflightStore()
    for mid in range(1, spec.count + 1):
        store.put_out(_outbound_message(mid, spec.payload_size))
    snapshots.append(
        probe.snapshot(
            "loaded",
            store_records=sum(1 for _ in store.out_items()),
            store_logical_bytes=(
                spec.count * _logical_message_bytes(spec.payload_size)
            ),
        )
    )
    store.clear_out()
    del store
    snapshots.append(probe.snapshot("released"))
    trimmed = _malloc_trim()
    snapshots.append(probe.snapshot("released_after_malloc_trim", malloc_trim=trimmed))
    return _finalize(spec, started, snapshots)


def run_sqlite_hydration(spec: ScenarioSpec) -> dict[str, Any]:
    probe = MemoryProbe()
    with tempfile.TemporaryDirectory(prefix="mqttium-memory-profile-") as directory:
        path = Path(directory) / "session.db"
        writer = SqliteInflightStore(path)
        with writer.batch():
            for mid in range(1, spec.count + 1):
                writer.put_out(_outbound_message(mid, spec.payload_size))
        writer.close()
        gc.collect()
        _malloc_trim()
        snapshots = [
            probe.snapshot(
                "baseline_before_hydration",
                sqlite_bytes=path.stat().st_size,
            )
        ]
        probe.reset_python_peak()
        started = time.perf_counter()
        store = SqliteInflightStore(path)
        engine = ProtocolEngine(store=store)
        snapshots.append(
            probe.snapshot(
                "loaded",
                queued_messages=len(engine._queued),
                packet_ids=len(engine.packet_ids),
                store_records=sum(1 for _ in store.out_items()),
                store_logical_bytes=(
                    spec.count * _logical_message_bytes(spec.payload_size)
                ),
            )
        )
        engine._queued.clear()
        engine.packet_ids.clear()
        store.clear_out()
        store.close()
        del engine
        del store
        snapshots.append(probe.snapshot("released"))
        trimmed = _malloc_trim()
        snapshots.append(probe.snapshot("released_after_malloc_trim", malloc_trim=trimmed))
    return _finalize(spec, started, snapshots)


RUNNERS: dict[str, Callable[[ScenarioSpec], dict[str, Any]]] = {
    "protocol_qos_queue": run_protocol_qos_queue,
    "iterator_delivery_queue": run_iterator_delivery_queue,
    "memory_store": run_memory_store,
    "sqlite_hydration": run_sqlite_hydration,
}


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def metadata(label: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "label": label,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "mqttium": package_version("mqttium"),
        "psutil": package_version("psutil"),
        "git_sha": os.environ.get("GITHUB_SHA"),
    }


def _scaled_spec(spec: ScenarioSpec, scale: float) -> ScenarioSpec:
    return ScenarioSpec(
        name=spec.name,
        runner=spec.runner,
        count=max(1, round(spec.count * scale)),
        payload_size=spec.payload_size,
        notes=spec.notes,
    )


def _run_child(spec: ScenarioSpec, output: Path) -> None:
    result = RUNNERS[spec.runner](spec)
    output.write_text(json.dumps(result, indent=2))
    loaded = result["snapshots"][1]
    print(
        f"{spec.name}: rss_delta={loaded['rss_delta_mib']:.2f} MiB "
        f"uss_delta={loaded['uss_delta_mib']:.2f} MiB "
        f"traced_peak={loaded['traced_peak_mib']:.2f} MiB"
    )


def _run_parent(output: Path, label: str, scale: float) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    scenarios: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="mqttium-memory-children-") as directory:
        for index, base_spec in enumerate(SCENARIOS):
            spec = _scaled_spec(base_spec, scale)
            child_output = Path(directory) / f"{index:02d}-{spec.name}.json"
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--child",
                spec.name,
                "--count",
                str(spec.count),
                "--payload-size",
                str(spec.payload_size),
                "--output",
                str(child_output),
            ]
            subprocess.run(command, check=True, env={**os.environ, "PYTHONHASHSEED": "0"})
            scenarios.append(json.loads(child_output.read_text()))
    payload = {"metadata": metadata(label), "scenarios": scenarios}
    output.write_text(json.dumps(payload, indent=2))
    print(f"Wrote isolated memory profile to {output}")


def _find_child_spec(name: str, count: int, payload_size: int) -> ScenarioSpec:
    for spec in SCENARIOS:
        if spec.name == name:
            return ScenarioSpec(
                name=spec.name,
                runner=spec.runner,
                count=count,
                payload_size=payload_size,
                notes=spec.notes,
            )
    raise ValueError(f"Unknown child scenario {name!r}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="manual")
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--child", choices=[spec.name for spec in SCENARIOS])
    parser.add_argument("--count", type=int)
    parser.add_argument("--payload-size", type=int)
    args = parser.parse_args()
    if args.scale <= 0:
        parser.error("--scale must be positive")
    if args.child is None:
        _run_parent(args.output, args.label, args.scale)
        return
    if args.count is None or args.payload_size is None:
        parser.error("child mode requires --count and --payload-size")
    if args.count <= 0 or args.payload_size < 0:
        parser.error("child count must be positive and payload size non-negative")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    _run_child(_find_child_spec(args.child, args.count, args.payload_size), args.output)


if __name__ == "__main__":
    main()
