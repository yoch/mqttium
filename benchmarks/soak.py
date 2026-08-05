"""Reconnect, delivery and backpressure soak against a real MQTT broker."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient, PublishMessage
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Properties


def _protocol(value: str) -> MQTTProtocolVersion:
    if value == "311":
        return MQTTProtocolVersion.MQTTv311
    if value == "5":
        return MQTTProtocolVersion.MQTTv5
    raise argparse.ArgumentTypeError("protocol must be 311 or 5")


def _idle_violations(client: AsyncClient) -> list[str]:
    stats = client.stats()
    checks = {
        "protocol.pending_outbound_messages": stats.protocol.pending_outbound_messages,
        "protocol.pending_outbound_bytes": stats.protocol.pending_outbound_bytes,
        "protocol.queued_outbound_messages": stats.protocol.queued_outbound_messages,
        "protocol.flow_inflight": stats.protocol.flow_inflight,
        "writer.queued_messages": stats.writer.queued_messages,
        "writer.queued_bytes": stats.writer.queued_bytes,
        "effects.pending": stats.effects.pending,
        "receipts.publish": stats.receipts.publish,
        "receipts.publish_batches": stats.receipts.publish_batches,
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


async def run(args: argparse.Namespace) -> dict[str, object]:
    run_id = uuid.uuid4().hex[:12]
    topic = f"mqttium/soak/{run_id}"
    connect_properties = None
    if args.protocol is MQTTProtocolVersion.MQTTv5:
        connect_properties = Properties(values={"session_expiry_interval": 60})

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
        clean_start=False,
        connect_properties=connect_properties,
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

        expected = 0
        payload = b"x" * args.payload_size
        for cycle in range(args.cycles):
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
                lambda: received >= expected,
                timeout=args.timeout,
                description=f"delivery of cycle {cycle}",
            )
            await _wait_idle(publisher, timeout=args.timeout)

            should_reconnect = (
                args.force_reconnect_every > 0
                and cycle + 1 < args.cycles
                and (cycle + 1) % args.force_reconnect_every == 0
            )
            if should_reconnect:
                previous_epoch = publisher.stats().connection_epoch
                transport = publisher._transport
                if transport is None:
                    raise RuntimeError("publisher transport disappeared before forced reconnect")
                await transport.close()
                await _wait_until(
                    lambda: (
                        publisher.is_connected
                        and publisher.stats().connection_epoch > previous_epoch
                    ),
                    timeout=args.timeout,
                    description=f"automatic reconnect after cycle {cycle}",
                )
                reconnects += 1

        await _wait_idle(publisher, timeout=args.timeout)
        publisher_stats = publisher.stats()
        subscriber_stats = subscriber.stats()
        return {
            "protocol": args.protocol.name,
            "cycles": args.cycles,
            "messages_per_cycle": args.messages_per_cycle,
            "payload_size": args.payload_size,
            "published": args.cycles * args.messages_per_cycle,
            "received": received,
            "forced_reconnects": reconnects,
            "elapsed_s": time.perf_counter() - started,
            "publisher_high_water": {
                "pending_messages": (publisher_stats.protocol.pending_outbound_high_water_messages),
                "pending_bytes": publisher_stats.protocol.pending_outbound_high_water_bytes,
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
        for client in (publisher, subscriber):
            try:
                await client.disconnect()
            except Exception:
                pass
        if consumer_task is not None:
            try:
                await asyncio.wait_for(consumer_task, timeout=2.0)
            except TimeoutError:
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--protocol", type=_protocol, default=_protocol("5"))
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--messages-per-cycle", type=int, default=500)
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--force-reconnect-every", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.cycles <= 0:
        parser.error("--cycles must be positive")
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


if __name__ == "__main__":
    main()
