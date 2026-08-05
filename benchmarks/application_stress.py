"""Application-side delivery and persistence stress benchmarks.

These measurements exercise the paths that codec-only benchmarks miss:
ordered callback delivery, iterator backpressure, and inflight persistence.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
import platform
import resource
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mqttium.api import AsyncClient
from mqttium.enums import OutboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message, OutboundMessage


@dataclass
class Sample:
    name: str
    operations: int
    seconds: float
    ops_per_s: float
    cpu_seconds: float
    max_rss_mib: float
    notes: str = ""


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def metadata() -> dict[str, object]:
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "mqttium": package_version("mqttium"),
    }


def sample(
    name: str,
    operations: int,
    started: float,
    cpu_started: float,
    *,
    notes: str = "",
) -> Sample:
    seconds = time.perf_counter() - started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return Sample(
        name=name,
        operations=operations,
        seconds=seconds,
        ops_per_s=operations / seconds,
        cpu_seconds=time.process_time() - cpu_started,
        max_rss_mib=usage.ru_maxrss / 1024,
        notes=notes,
    )


def message_effect(sequence: int) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic="bench/delivery", payload=str(sequence).encode()),
    )


async def callback_delivery(count: int, delay: float) -> Sample:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=128,
        delivery_timeout=10.0,
        callback_shutdown_timeout=10.0,
    )
    seen: list[int] = []

    async def callback(message: Message) -> None:
        if delay:
            await asyncio.sleep(delay)
        seen.append(int(message.payload))

    client.on_message = callback
    started = time.perf_counter()
    cpu_started = time.process_time()
    for sequence in range(count):
        await client._apply_effect(message_effect(sequence), nowait=False)
    await client._callback_queue.join()
    result = sample(
        f"callback_delay_{delay * 1000:g}ms",
        count,
        started,
        cpu_started,
        notes="bounded queue=128; single ordered worker",
    )
    if seen != list(range(count)):
        raise RuntimeError("callback ordering or completeness violation")
    await client._shutdown_callback_worker(drain=False)
    return result


async def iterator_delivery(count: int, delay: float) -> Sample:
    client = AsyncClient(
        message_delivery="iterator",
        max_pending_messages=128,
        delivery_timeout=10.0,
    )
    seen: list[int] = []

    async def consume() -> None:
        async for message in client.messages():
            if delay:
                await asyncio.sleep(delay)
            seen.append(int(message.payload))
            if len(seen) == count:
                return

    consumer = asyncio.create_task(consume())
    started = time.perf_counter()
    cpu_started = time.process_time()
    for sequence in range(count):
        await client._apply_effect(message_effect(sequence), nowait=False)
    await consumer
    result = sample(
        f"iterator_delay_{delay * 1000:g}ms",
        count,
        started,
        cpu_started,
        notes="bounded queue=128; public async iterator consumer",
    )
    if seen != list(range(count)):
        raise RuntimeError("iterator ordering or completeness violation")
    return result


def messages(count: int) -> list[OutboundMessage]:
    payload = b"x" * 64
    return [
        OutboundMessage(
            mid=mid,
            topic="bench/persistence",
            payload=payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
        for mid in range(1, count + 1)
    ]


def persistence_cycle(name: str, store, count: int, *, batched: bool) -> Sample:
    records = messages(count)
    started = time.perf_counter()
    cpu_started = time.process_time()
    context = store.batch() if batched else _NoopContext()
    with context:
        for record in records:
            store.put_out(record)
        for record in records:
            record.dup = True
            store.update_out(record)
        for record in records:
            if not store.delete_out(record.mid):
                raise RuntimeError(f"missing persistence record mid={record.mid}")
    return sample(
        name,
        count * 3,
        started,
        cpu_started,
        notes=f"{count} put + update + pop operations",
    )


class _NoopContext:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False


async def run(args: argparse.Namespace) -> list[Sample]:
    samples = [
        await callback_delivery(args.count, 0.0),
        await callback_delivery(min(500, args.count), 0.001),
        await callback_delivery(min(100, args.count), 0.010),
        await callback_delivery(min(20, args.count), 0.100),
        await iterator_delivery(args.count, 0.0),
        await iterator_delivery(min(500, args.count), 0.001),
    ]

    persistence_count = min(args.sqlite_count, 20_000)
    samples.append(
        persistence_cycle(
            "memory_store_cycle",
            MemoryInflightStore(),
            persistence_count,
            batched=True,
        )
    )
    with tempfile.TemporaryDirectory(prefix="mqttium-bench-") as directory:
        store = SqliteInflightStore(Path(directory) / "autocommit.db")
        samples.append(
            persistence_cycle(
                "sqlite_autocommit_cycle",
                store,
                min(persistence_count, 2_000),
                batched=False,
            )
        )
        store.close()

        store = SqliteInflightStore(Path(directory) / "batched.db")
        samples.append(
            persistence_cycle(
                "sqlite_batched_cycle",
                store,
                persistence_count,
                batched=True,
            )
        )
        store.close()
    return samples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20_000)
    parser.add_argument("--sqlite-count", type=int, default=5_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.count <= 0 or args.sqlite_count <= 0:
        parser.error("counts must be positive")

    samples = asyncio.run(run(args))
    payload = {
        "metadata": metadata(),
        "samples": [asdict(item) for item in samples],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    for item in samples:
        print(f"{item.name:28s} {item.ops_per_s:12.1f} ops/s")


if __name__ == "__main__":
    main()
