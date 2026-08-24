"""Reconnect, delivery and backpressure soak against a real MQTT broker."""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import gc
import json
import sys
import time
import tracemalloc
import uuid
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient, PublishMessage
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.reconnect import ReconnectPolicy

try:
    import psutil
except ImportError:  # pragma: no cover - checked by the command entry point
    psutil = None  # type: ignore[assignment]


_MIB = 1024 * 1024


def _resource_snapshot(process: Any, phase: str) -> dict[str, Any]:
    gc.collect()
    memory = process.memory_info()
    full = process.memory_full_info()
    uss = getattr(full, "uss", None)
    pss = getattr(full, "pss", None)
    current, peak = tracemalloc.get_traced_memory()
    num_fds = getattr(process, "num_fds", None)
    return {
        "phase": phase,
        "rss_mib": memory.rss / _MIB,
        "uss_mib": None if uss is None else uss / _MIB,
        "pss_mib": None if pss is None else pss / _MIB,
        "traced_current_mib": current / _MIB,
        "traced_peak_mib": peak / _MIB,
        "threads": process.num_threads(),
        "fds": num_fds() if num_fds is not None else None,
        "asyncio_tasks": sum(not task.done() for task in asyncio.all_tasks()),
    }


def _malloc_trim() -> bool:
    if not sys.platform.startswith("linux"):
        return False
    try:
        return bool(ctypes.CDLL(None).malloc_trim(0))
    except (AttributeError, OSError):
        return False


def _resource_assessment(snapshots: list[dict[str, Any]]) -> dict[str, object]:
    baseline, _drained, trimmed = snapshots
    violations = [
        field
        for field in ("threads", "fds", "asyncio_tasks")
        if baseline[field] is not None
        and trimmed[field] is not None
        and trimmed[field] > baseline[field]
    ]
    deltas: dict[str, float | None] = {}
    for field in ("rss_mib", "uss_mib", "pss_mib", "traced_current_mib"):
        before, after = baseline[field], trimmed[field]
        deltas[field] = None if before is None or after is None else float(after) - float(before)
    return {
        "status": "resource_leak" if violations else "stable",
        "discrete_violations": violations,
        "post_trim_delta_mib": deltas,
        "memory_is_diagnostic": True,
    }


def _protocol(value: str) -> MQTTProtocolVersion:
    if value == "311":
        return MQTTProtocolVersion.MQTTv311
    if value == "5":
        return MQTTProtocolVersion.MQTTv5
    raise argparse.ArgumentTypeError("protocol must be 311 or 5")


def _idle_violations(client: AsyncClient) -> list[str]:
    stats = client.stats()
    checks = {
        "outbound.pending_messages": stats.outbound.pending_messages,
        "outbound.pending_bytes": stats.outbound.pending_bytes,
        "outbound.queued_messages": stats.outbound.queued_messages,
        "outbound.flow_inflight": stats.outbound.flow_inflight,
        "outbound.packet_ids_in_use": stats.outbound.packet_ids_in_use,
        "inbound.inflight": stats.inbound.inflight,
        "inbound.pending_bytes": stats.inbound.pending_bytes,
        "inbound.replay_pending": stats.inbound.replay_pending,
        "delivery.iterator_queued": stats.delivery.iterator_queued,
        "delivery.callback_queued": stats.delivery.callback_queued,
        "delivery.pending_bytes": stats.delivery.pending_bytes,
        "delivery.waiters": stats.delivery.waiters,
        "writer.queued_messages": stats.writer.queued_messages,
        "writer.queued_bytes": stats.writer.queued_bytes,
        "writer.waiters": stats.writer.waiters,
        "effects.pending": stats.effects.pending,
        "effects.waiters": stats.effects.waiters,
        "receipts.publish": stats.receipts.publish,
        "receipts.publish_batches": stats.receipts.publish_batches,
        "receipts.subscribe": stats.receipts.subscribe,
        "receipts.unsubscribe": stats.receipts.unsubscribe,
        "receipts.publish_waiters": stats.receipts.publish_waiters,
    }
    return [f"{name}={value}" for name, value in checks.items() if value]


async def _wait_until(
    predicate: Any,
    *,
    timeout: float,
    description: str,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting for {description}")
        await asyncio.sleep(0.01)


async def _wait_idle(client: AsyncClient, *, timeout: float) -> None:
    await _wait_until(
        lambda: not _idle_violations(client),
        timeout=timeout,
        description="client queues and receipts to drain",
    )


async def _force_reconnect(client: AsyncClient, *, cycle: int, timeout: float) -> None:
    previous_epoch = client.stats().connection_epoch
    transport = client._transport
    if transport is None:
        raise RuntimeError("publisher transport disappeared before forced reconnect")
    await transport.close()
    await _wait_until(
        lambda: (
            client.is_connected
            and (stats := client.stats()).connection_epoch > previous_epoch
            and not stats.tasks.reconnect
        ),
        timeout=timeout,
        description=f"settled automatic reconnect after cycle {cycle}",
    )


def _measurement_done(
    args: argparse.Namespace,
    measured_cycles: int,
    measurement_started: float | None,
) -> bool:
    if measurement_started is None or measured_cycles == 0:
        return False
    if args.duration_seconds > 0:
        return time.perf_counter() - measurement_started >= args.duration_seconds
    return measured_cycles >= args.cycles


async def _disconnect_clients(
    publisher: AsyncClient,
    subscriber: AsyncClient,
    consumer_task: asyncio.Task[None] | None,
) -> None:
    for client in (publisher, subscriber):
        try:
            await client.disconnect()
        except Exception:
            pass
    if consumer_task is None:
        return
    try:
        await asyncio.wait_for(consumer_task, timeout=2.0)
    except TimeoutError:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass


async def run(args: argparse.Namespace) -> dict[str, object]:
    run_id = uuid.uuid4().hex[:12]
    topic = f"mqttium/soak/{run_id}"
    subscriber = AsyncClient(
        f"mqttium-soak-sub-{run_id}",
        protocol=args.protocol,
        message_delivery="iterator",
        max_pending_messages=max(args.messages_per_cycle * 2, 1024),
        max_pending_delivery_bytes=max(args.payload_size * args.messages_per_cycle * 4, 1 << 20),
    )
    publisher = AsyncClient(
        f"mqttium-soak-pub-{run_id}",
        protocol=args.protocol,
        clean_start=True,
        reconnect=ReconnectPolicy(
            enabled=True,
            initial_delay=0.05,
            multiplier=1.5,
            max_delay=0.5,
            max_retries=100,
            stable_after=0.05,
            connect_timeout=args.timeout,
        ),
        max_pending_outbound_messages=max(args.messages_per_cycle * 2, 1024),
        max_pending_outbound_bytes=max(args.payload_size * args.messages_per_cycle * 4, 1 << 20),
        max_outbound_messages=max(args.messages_per_cycle * 2, 1024),
        max_outbound_bytes=max(args.payload_size * args.messages_per_cycle * 2, 1 << 20),
    )

    received = 0

    async def consume() -> None:
        nonlocal received
        async for message in subscriber.messages():
            if message.topic == topic:
                received += 1

    consumer_task: asyncio.Task[None] | None = None
    started = time.perf_counter()
    reconnects = 0
    try:
        await subscriber.connect(args.host, args.port, timeout=args.timeout)
        await subscriber.subscribe(topic, qos=1, timeout=args.timeout)
        consumer_task = asyncio.create_task(consume(), name="mqttium-soak-consumer")
        await publisher.connect(args.host, args.port, timeout=args.timeout)

        if psutil is None:
            raise RuntimeError("soak.py resource sampling requires the benchmark extra")
        process = psutil.Process()
        expected = 0
        cycle = 0
        measured_cycles = 0
        measurement_started: float | None = None
        snapshots: list[dict[str, Any]] = []
        payload = b"x" * args.payload_size
        if args.warmup_cycles == 0:
            tracemalloc.start(25)
            snapshots.append(_resource_snapshot(process, "post_warmup"))
            tracemalloc.reset_peak()
            measurement_started = time.perf_counter()
        while not _measurement_done(args, measured_cycles, measurement_started):
            batch = await publisher.publish_many(
                (
                    PublishMessage(
                        topic,
                        cycle.to_bytes(4, "big") + index.to_bytes(4, "big") + payload,
                        qos=1,
                    )
                    for index in range(args.messages_per_cycle)
                ),
                chunk_size=min(256, args.messages_per_cycle),
            )
            await asyncio.wait_for(batch.wait(), timeout=args.timeout)
            expected += args.messages_per_cycle
            await _wait_until(
                lambda expected=expected: received >= expected,
                timeout=args.timeout,
                description=f"delivery of cycle {cycle}",
            )
            await _wait_idle(publisher, timeout=args.timeout)

            cycle += 1
            if measurement_started is None and cycle == args.warmup_cycles:
                tracemalloc.start(25)
                snapshots.append(_resource_snapshot(process, "post_warmup"))
                tracemalloc.reset_peak()
                measurement_started = time.perf_counter()
            elif measurement_started is not None:
                measured_cycles += 1

            should_reconnect = (
                args.force_reconnect_every > 0
                and cycle % args.force_reconnect_every == 0
                and not _measurement_done(args, measured_cycles, measurement_started)
            )
            if should_reconnect:
                await _force_reconnect(publisher, cycle=cycle, timeout=args.timeout)
                reconnects += 1

        await _wait_idle(publisher, timeout=args.timeout)
        assert measurement_started is not None
        measurement_elapsed = time.perf_counter() - measurement_started
        snapshots.append(_resource_snapshot(process, "drained"))
        trimmed = _malloc_trim()
        snapshots.append(_resource_snapshot(process, "post_malloc_trim"))
        snapshots[-1]["malloc_trim"] = trimmed
        tracemalloc.stop()
        assessment = _resource_assessment(snapshots)
        publisher_stats = publisher.stats()
        subscriber_stats = subscriber.stats()
        return {
            "protocol": args.protocol.name,
            "cycles": measured_cycles,
            "warmup_cycles": args.warmup_cycles,
            "requested_duration_s": args.duration_seconds or None,
            "messages_per_cycle": args.messages_per_cycle,
            "payload_size": args.payload_size,
            "published": cycle * args.messages_per_cycle,
            "measured_published": measured_cycles * args.messages_per_cycle,
            "received": received,
            "forced_reconnects": reconnects,
            "elapsed_s": time.perf_counter() - started,
            "measurement_elapsed_s": measurement_elapsed,
            "resource_snapshots": snapshots,
            "resource_assessment": assessment,
            "publisher_high_water": {
                "pending_messages": publisher_stats.outbound.pending_high_water_messages,
                "pending_bytes": publisher_stats.outbound.pending_high_water_bytes,
                "writer_messages": publisher_stats.writer.high_water_messages,
                "writer_bytes": publisher_stats.writer.high_water_bytes,
                "decoder_bytes": publisher_stats.decoder.high_water_bytes,
            },
            "subscriber_high_water": {
                "delivery_bytes": subscriber_stats.delivery.pending_high_water_bytes,
                "decoder_bytes": subscriber_stats.decoder.high_water_bytes,
            },
            "publisher_idle_violations": _idle_violations(publisher),
        }
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        await _disconnect_clients(publisher, subscriber, consumer_task)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--protocol", type=_protocol, default=_protocol("5"))
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--duration-seconds", type=float, default=0.0)
    parser.add_argument("--warmup-cycles", type=int, default=2)
    parser.add_argument("--messages-per-cycle", type=int, default=500)
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--force-reconnect-every", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.cycles <= 0:
        parser.error("--cycles must be positive")
    if args.duration_seconds < 0:
        parser.error("--duration-seconds must be non-negative")
    if args.warmup_cycles < 0:
        parser.error("--warmup-cycles must be non-negative")
    if args.messages_per_cycle <= 0:
        parser.error("--messages-per-cycle must be positive")
    if args.payload_size < 0:
        parser.error("--payload-size must be non-negative")
    if args.force_reconnect_every < 0:
        parser.error("--force-reconnect-every must be non-negative")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main() -> None:
    args = parse_args()
    result = asyncio.run(run(args))
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    print(rendered, end="")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    assessment = result["resource_assessment"]
    if isinstance(assessment, dict) and assessment.get("status") == "resource_leak":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
