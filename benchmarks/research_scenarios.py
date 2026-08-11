"""Realistic MQTT usage scenarios for profiling and paired research runs.

The worker validates delivery and ordering itself.  Profilers may wrap this
process, but their output is diagnostic only: clean worker runs are the only
inputs accepted by the paired research orchestrator.
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import importlib
import json
import os
import resource
import ssl
import statistics
import subprocess
import sys
import time
import uuid
from collections import deque
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol, cast

from mqttium.api import AsyncClient, MessageDelivery, Properties, PublishMessage
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.reconnect import ReconnectPolicy


class SubscriberProbe(Protocol):
    def abort(self) -> None: ...
    def finish(self, timeout: float) -> tuple[list[float], list[int]]: ...


def _start_subscriber(
    host: str,
    port: int,
    topic: str,
    count: int,
    cafile: str | None,
) -> SubscriberProbe:
    module_name = "benchmarks.paired_network" if __package__ else "paired_network"
    module = importlib.import_module(module_name)
    if cafile is None:
        return cast(SubscriberProbe, module.start_subscriber(host, port, topic, count))
    command = [
        "mosquitto_sub",
        "-h",
        host,
        "-p",
        str(port),
        "-t",
        topic,
        "-q",
        "0",
        "-C",
        str(count),
        "-F",
        "%p",
        "--cafile",
        cafile,
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert process.stdout is not None
    module_file = module.__file__
    if module_file is None:  # pragma: no cover - imported source module contract
        raise RuntimeError("paired network observer has no source path")
    observer = subprocess.Popen(
        [sys.executable, str(Path(module_file).resolve()), "--observer-worker"],
        stdin=process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.stdout.close()
    time.sleep(0.2)
    if process.poll() is not None:
        observer.kill()
        observer.wait()
        assert process.stderr is not None
        raise RuntimeError(process.stderr.read().decode(errors="replace"))
    return cast(SubscriberProbe, module.SubscriberProbe(process, observer))


SCENARIOS = (
    "telemetry",
    "reliable",
    "duplex",
    "batch_delivery",
    "large_payload",
    "slow_consumer",
    "property_heavy",
)


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def _max_rss_mib() -> float:
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value / (1024 * 1024 if sys_platform_is_darwin() else 1024)


def sys_platform_is_darwin() -> bool:
    return os.uname().sysname == "Darwin" if hasattr(os, "uname") else False


def _payload(sequence: int, size: int) -> bytes:
    header = f"{sequence:016x}{time.monotonic_ns():016x}".encode("ascii")
    return header + b"x" * max(0, size - len(header))


def _decode_payload(payload: bytes) -> tuple[int, float]:
    if len(payload) < 32:
        raise RuntimeError(f"benchmark payload is too short: {len(payload)}")
    try:
        sequence = int(payload[:16], 16)
        sent_ns = int(payload[16:32], 16)
    except ValueError as exc:
        raise RuntimeError("benchmark payload header is malformed") from exc
    return sequence, (time.monotonic_ns() - sent_ns) / 1_000_000


def _protocol(value: str) -> MQTTProtocolVersion:
    return MQTTProtocolVersion.MQTTv5 if value == "5" else MQTTProtocolVersion.MQTTv311


def _pin(args: argparse.Namespace) -> None:
    if args.cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {args.cpu})


def _client(
    args: argparse.Namespace,
    suffix: str,
    *,
    delivery: MessageDelivery = "iterator",
) -> AsyncClient:
    budget = max(args.count * max(args.payload_size, 64) * 3, 2 * 1024 * 1024)
    return AsyncClient(
        client_id=f"research-{suffix}-{os.getpid()}-{uuid.uuid4().hex[:8]}",
        protocol=_protocol(args.protocol),
        message_delivery=delivery,
        max_pending_messages=max(args.count * 2, 1024),
        max_pending_delivery_bytes=budget,
        max_outbound_inflight=args.window,
        max_pending_outbound_messages=max(args.count * 2, 1024),
        max_pending_outbound_bytes=budget,
        max_outbound_messages=max(args.count * 2, 1024),
        max_outbound_bytes=budget,
        reconnect=ReconnectPolicy(enabled=False),
    )


async def _connect(client: AsyncClient, args: argparse.Namespace) -> None:
    if args.ws_url:
        context = ssl.create_default_context(cafile=args.cafile) if args.cafile else None
        await client.connect_ws(args.ws_url, ssl=context, timeout=args.timeout)
        return
    context = ssl.create_default_context(cafile=args.cafile) if args.cafile else None
    await client.connect(args.host, args.port, ssl=context, timeout=args.timeout)


async def _wait_event(event: asyncio.Event, args: argparse.Namespace, description: str) -> None:
    await asyncio.wait_for(event.wait(), timeout=args.timeout)
    if not event.is_set():  # pragma: no cover - wait_for contract guard
        raise TimeoutError(description)


def _latency_summary(values: list[float]) -> dict[str, float]:
    return {
        "p50_ms": statistics.median(values) if values else 0.0,
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
    }


async def _one_way(  # noqa: C901 - scenario lifecycle is kept auditable in one owner
    args: argparse.Namespace,
    *,
    qos: int,
    batch: bool = False,
    callback_delay: float = 0.0,
    properties: Properties | None = None,
) -> dict[str, Any]:
    topic = f"mqttium/research/{args.scenario}/{uuid.uuid4().hex}"
    delivery: MessageDelivery = "callback" if callback_delay else "iterator"
    external_observer = not callback_delay and not args.ws_url
    subscriber = None if external_observer else _client(args, "subscriber", delivery=delivery)
    publisher = _client(args, "publisher")
    observer: SubscriberProbe | None = None
    received: list[int] = []
    delivery_latencies: list[float] = []
    completed = asyncio.Event()
    consumer: asyncio.Task[None] | None = None

    def record(payload: bytes) -> None:
        sequence, latency = _decode_payload(payload)
        received.append(sequence)
        delivery_latencies.append(latency)
        if len(received) == args.count:
            completed.set()

    if callback_delay:

        async def on_message(message: Any) -> None:
            if callback_delay:
                await asyncio.sleep(callback_delay)
            record(message.payload)

        assert subscriber is not None
        subscriber.on_message = on_message

    try:
        if external_observer:
            observer = _start_subscriber(args.host, args.port, topic, args.count, args.cafile)
            # ``mosquitto_sub`` has no machine-readable readiness handshake.
            # Give the SUBACK time to arrive before a QoS 0 publisher can emit
            # an unrecoverable first sample.
            await asyncio.sleep(0.5)
        else:
            assert subscriber is not None
            await _connect(subscriber, args)
            result = await subscriber.subscribe(topic, qos=qos, timeout=args.timeout)
            if any(code >= 0x80 for code in result.reason_codes):
                raise RuntimeError(f"subscription rejected: {result.reason_codes}")
        _pin(args)
        if not callback_delay and not external_observer:
            assert subscriber is not None

            async def consume() -> None:
                async for message in subscriber.messages():
                    if message.topic == topic:
                        record(message.payload)
                        if len(received) == args.count:
                            return

            consumer = asyncio.create_task(consume(), name="research-consumer")

        await _connect(publisher, args)
        loop = asyncio.get_running_loop()
        cpu_started = time.process_time()
        started = loop.time()
        schedule_lag: list[float] = []
        ack_latencies: list[float] = []
        pending: deque[Any] = deque()
        ack_tasks: list[asyncio.Task[float]] = []

        async def observe_ack(receipt: Any, sent_ns: int) -> float:
            await receipt.wait()
            return (time.monotonic_ns() - sent_ns) / 1_000_000

        if batch:
            batch_receipt = await publisher.publish_many(
                (
                    PublishMessage(
                        topic,
                        _payload(sequence, args.payload_size),
                        qos=qos,
                        properties=properties,
                    )
                    for sequence in range(args.count)
                ),
                chunk_size=min(256, args.count),
            )
            await batch_receipt.wait()
        else:
            for sequence in range(args.count):
                if args.target_rate > 0:
                    deadline = started + sequence / args.target_rate
                    delay = deadline - loop.time()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    schedule_lag.append(max(0.0, loop.time() - deadline) * 1000)
                sent_ns = time.monotonic_ns()
                receipt = await publisher.publish(
                    topic,
                    _payload(sequence, args.payload_size),
                    qos=qos,
                    properties=properties,
                )
                if qos:
                    pending.append(receipt)
                    if not args.skip_ack_timestamps:
                        ack_tasks.append(asyncio.create_task(observe_ack(receipt, sent_ns)))
                    if len(pending) >= args.window:
                        oldest = pending.popleft()
                        await oldest.wait()
            while pending:
                receipt = pending.popleft()
                await receipt.wait()
            if ack_tasks:
                ack_latencies.extend(await asyncio.gather(*ack_tasks))

        # QoS 0 receipts are complete at admission.  Do not block the event
        # loop in the external observer while the single writer still owns
        # queued frames, otherwise a correct run looks like message loss.
        await publisher._write_pump.join()
        transport = publisher._transport
        drain = getattr(transport, "drain", None)
        if drain is not None:
            await drain()
        publish_elapsed = max(loop.time() - started, 1e-9)
        if observer is not None:
            observed_latencies, sequences = observer.finish(args.timeout)
            observer = None
            received.extend(sequences)
            delivery_latencies.extend(observed_latencies)
        else:
            try:
                await _wait_event(completed, args, "subscriber completion")
            except TimeoutError as exc:
                raise TimeoutError(
                    f"subscriber completion: received={len(received)}/{args.count} "
                    f"unique={len(set(received))}"
                ) from exc
        elapsed = max(loop.time() - started, 1e-9)
        cpu_seconds = time.process_time() - cpu_started
        if consumer is not None:
            await consumer
        if sorted(received) != list(range(args.count)):
            raise RuntimeError(f"delivery mismatch: received={len(received)}/{args.count}")
        return {
            "published": args.count,
            "received": len(received),
            "elapsed_s": elapsed,
            "publish_elapsed_s": publish_elapsed,
            "primary_rate": args.count / elapsed,
            "publisher_rate": args.count / publish_elapsed,
            "cpu_seconds": cpu_seconds,
            "delivery_latency": _latency_summary(delivery_latencies),
            "ack_latency": _latency_summary(ack_latencies),
            "loop_lag": _latency_summary(schedule_lag),
            "publisher_stats": asdict(publisher.stats()),
            "subscriber_stats": None if subscriber is None else asdict(subscriber.stats()),
        }
    finally:
        if observer is not None:
            observer.abort()
        for client in (publisher, subscriber):
            if client is None:
                continue
            try:
                await client.disconnect()
            except Exception:
                pass
        if consumer is not None and not consumer.done():
            consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass


async def _duplex(args: argparse.Namespace) -> dict[str, Any]:
    run_id = uuid.uuid4().hex
    topics = (f"mqttium/research/duplex/a/{run_id}", f"mqttium/research/duplex/b/{run_id}")
    clients = (_client(args, "duplex-a"), _client(args, "duplex-b"))
    received: list[list[int]] = [[], []]
    latencies: list[float] = []
    complete = asyncio.Event()

    async def consume(client: AsyncClient, index: int) -> None:
        async for message in client.messages():
            sequence, latency = _decode_payload(message.payload)
            received[index].append(sequence)
            latencies.append(latency)
            if all(len(items) == args.count for items in received):
                complete.set()
                return

    consumers: list[asyncio.Task[None]] = []
    try:
        for client in clients:
            await _connect(client, args)
        await clients[0].subscribe(topics[1], qos=1, timeout=args.timeout)
        await clients[1].subscribe(topics[0], qos=1, timeout=args.timeout)
        consumers = [
            asyncio.create_task(consume(clients[0], 0), name="research-duplex-a"),
            asyncio.create_task(consume(clients[1], 1), name="research-duplex-b"),
        ]
        _pin(args)

        async def send(client: AsyncClient, topic: str) -> None:
            receipt = await client.publish_many(
                (
                    PublishMessage(topic, _payload(sequence, args.payload_size), qos=1)
                    for sequence in range(args.count)
                ),
                chunk_size=min(args.window, 256),
            )
            await receipt.wait()

        started = time.perf_counter()
        cpu_started = time.process_time()
        await asyncio.gather(send(clients[0], topics[0]), send(clients[1], topics[1]))
        await _wait_event(complete, args, "duplex completion")
        elapsed = max(time.perf_counter() - started, 1e-9)
        if any(sorted(items) != list(range(args.count)) for items in received):
            raise RuntimeError("duplex delivery ordering or completeness violation")
        return {
            "published": args.count * 2,
            "received": sum(map(len, received)),
            "elapsed_s": elapsed,
            "publish_elapsed_s": elapsed,
            "primary_rate": args.count * 2 / elapsed,
            "publisher_rate": args.count * 2 / elapsed,
            "cpu_seconds": time.process_time() - cpu_started,
            "delivery_latency": _latency_summary(latencies),
            "ack_latency": _latency_summary([]),
            "loop_lag": _latency_summary([]),
            "client_stats": [asdict(client.stats()) for client in clients],
        }
    finally:
        for client in clients:
            try:
                await client.disconnect()
            except Exception:
                pass
        for consumer in consumers:
            if not consumer.done():
                consumer.cancel()
        if consumers:
            await asyncio.gather(*consumers, return_exceptions=True)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.scenario == "duplex":
        measurement = await _duplex(args)
    elif args.scenario == "telemetry":
        measurement = await _one_way(args, qos=0)
    elif args.scenario == "reliable":
        measurement = await _one_way(args, qos=1)
    elif args.scenario == "batch_delivery":
        measurement = await _one_way(args, qos=1, batch=True)
    elif args.scenario == "large_payload":
        measurement = await _one_way(args, qos=1)
    elif args.scenario == "slow_consumer":
        measurement = await _one_way(args, qos=0, callback_delay=args.callback_delay)
    else:
        properties = Properties()
        properties.set("content_type", "application/octet-stream")
        for index in range(16):
            properties.add_user_property(f"key-{index}", "value" * 4)
        measurement = await _one_way(args, qos=2, properties=properties)
    return {
        "scenario": args.scenario,
        "protocol": args.protocol,
        "transport": "websocket" if args.ws_url else ("tls" if args.cafile else "tcp"),
        "payload_size": args.payload_size,
        "count": args.count,
        "window": args.window,
        "target_rate": args.target_rate,
        "callback_delay_s": args.callback_delay,
        "max_rss_mib": _max_rss_mib(),
        **measurement,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--ws-url")
    parser.add_argument("--cafile")
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--window", type=int, default=64)
    parser.add_argument("--target-rate", type=float, default=0.0)
    parser.add_argument("--callback-delay", type=float, default=0.001)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cprofile-output", type=Path)
    parser.add_argument("--skip-ack-timestamps", action="store_true")
    args = parser.parse_args()
    if args.count <= 0 or args.payload_size < 32 or args.window <= 0:
        parser.error("count/window must be positive and payload-size must be at least 32")
    if args.target_rate < 0 or args.callback_delay < 0 or args.timeout <= 0:
        parser.error("rates/delays must be non-negative and timeout must be positive")
    return args


def main() -> None:
    args = parse_args()
    if args.cprofile_output is None:
        result = asyncio.run(run(args))
    else:
        args.cprofile_output.parent.mkdir(parents=True, exist_ok=True)
        profiler = cProfile.Profile()
        result = profiler.runcall(asyncio.run, run(args))
        profiler.dump_stats(args.cprofile_output)
    rendered = json.dumps(result, indent=2, default=str) + "\n"
    print(rendered, end="")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
