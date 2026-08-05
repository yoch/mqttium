"""Targeted native AsyncClient RTT and ingress benchmark.

The benchmark intentionally stays inside MQTTium's public AsyncClient surface for
both measured clients. Ingress load is produced by raw MQTT 3.1.1 publishers in
worker threads so sender scheduling does not consume the subscriber event loop.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import queue
import socket
import statistics
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 11883
RTT_PAYLOAD_SIZE = 64
TELEMETRY_PAYLOAD = b"x" * 256


@dataclass(frozen=True)
class Sample:
    name: str
    rate: float
    p50_ms: float | None
    p95_ms: float | None
    counters: dict[str, Any]


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


async def _wait_receipts_empty(*clients: AsyncClient, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(client.stats().receipts.publish == 0 for client in clients):
            return
        await asyncio.sleep(0)
    remaining = [client.stats().receipts.publish for client in clients]
    raise TimeoutError(f"publish receipts did not drain: {remaining}")


async def _rtt_once(count: int, window: int, run_id: str) -> Sample:
    request_topic = f"bench/native-rtt/{run_id}/request"
    response_topic = f"bench/native-rtt/{run_id}/response"
    responder = AsyncClient(
        client_id=f"mqttium-rtt-responder-{run_id}",
        message_delivery="callback",
        max_pending_callbacks=4_096,
    )
    initiator = AsyncClient(
        client_id=f"mqttium-rtt-initiator-{run_id}",
        message_delivery="callback",
        max_pending_callbacks=4_096,
    )
    completed: asyncio.Queue[int] = asyncio.Queue()
    starts = [0] * count
    latencies_ms: list[float] = []

    async def respond(message: Any) -> None:
        await responder.publish(response_topic, message.payload, qos=1)

    def receive(message: Any) -> None:
        sequence = int.from_bytes(message.payload[:8], "big")
        latencies_ms.append((time.perf_counter_ns() - starts[sequence]) / 1_000_000)
        completed.put_nowait(sequence)

    responder.on_message = respond
    initiator.on_message = receive
    await responder.connect(BROKER_HOST, BROKER_PORT)
    await initiator.connect(BROKER_HOST, BROKER_PORT)
    try:
        await responder.subscribe(request_topic, qos=1)
        await initiator.subscribe(response_topic, qos=1)
        await asyncio.sleep(0.05)

        async def submit(sequence: int) -> None:
            payload = sequence.to_bytes(8, "big") + b"r" * (RTT_PAYLOAD_SIZE - 8)
            starts[sequence] = time.perf_counter_ns()
            await initiator.publish(request_topic, payload, qos=1)

        next_sequence = 0
        started = time.perf_counter()
        for _ in range(min(window, count)):
            await submit(next_sequence)
            next_sequence += 1
        for _ in range(count):
            await completed.get()
            if next_sequence < count:
                await submit(next_sequence)
                next_sequence += 1
        elapsed = time.perf_counter() - started
        await _wait_receipts_empty(initiator, responder)
        initiator_stats = initiator.stats()
        responder_stats = responder.stats()
        return Sample(
            name="rtt_qos1_w32",
            rate=count / elapsed,
            p50_ms=statistics.median(latencies_ms),
            p95_ms=_percentile(latencies_ms, 0.95),
            counters={
                "pairs": count,
                "initiator_effect_batches": initiator_stats.effects.batches,
                "initiator_effect_inline": initiator_stats.effects.inline_effects,
                "initiator_effect_suspensions": initiator_stats.effects.apply_suspensions,
                "responder_effect_batches": responder_stats.effects.batches,
                "responder_effect_inline": responder_stats.effects.inline_effects,
                "responder_effect_suspensions": responder_stats.effects.apply_suspensions,
                "initiator_writer_batches": initiator_stats.writer.batches,
                "responder_writer_batches": responder_stats.writer.batches,
                "initiator_delivery_pending": initiator_stats.delivery.callback_queued,
                "responder_delivery_pending": responder_stats.delivery.callback_queued,
            },
        )
    finally:
        await initiator.disconnect()
        await responder.disconnect()


def _encode_vbi(value: int) -> bytes:
    encoded = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        encoded.append(digit)
        if not value:
            return bytes(encoded)


def _connect_packet(client_id: str) -> bytes:
    client = client_id.encode("utf-8")
    variable = b"\x00\x04MQTT\x04\x02\x00\x00"
    payload = struct.pack("!H", len(client)) + client
    remaining = variable + payload
    return b"\x10" + _encode_vbi(len(remaining)) + remaining


def _publish_frame(topic: str, payload: bytes) -> bytes:
    topic_bytes = topic.encode("utf-8")
    body = struct.pack("!H", len(topic_bytes)) + topic_bytes + payload
    return b"\x30" + _encode_vbi(len(body)) + body


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise ConnectionError("broker closed raw publisher connection")
        chunks.extend(chunk)
    return bytes(chunks)


def _raw_publish_worker(
    topic: str,
    count: int,
    worker_id: int,
    start_event: threading.Event,
    ready: queue.SimpleQueue[None],
    connections: int = 8,
) -> float:
    frame = _publish_frame(topic, TELEMETRY_PAYLOAD)
    sockets: list[socket.socket] = []
    try:
        for connection_id in range(connections):
            sock = socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=10.0)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client_id = f"mqttium-raw-{worker_id}-{connection_id}-{time.time_ns()}"
            sock.sendall(_connect_packet(client_id))
            if _recv_exact(sock, 4) != b"\x20\x02\x00\x00":
                raise ConnectionError("unexpected CONNACK for raw publisher")
            sockets.append(sock)
        ready.put(None)
        start_event.wait()
        started = time.perf_counter()
        remaining = count
        batch_frames = 64
        cursor = 0
        while remaining:
            take = min(batch_frames, remaining)
            sockets[cursor].sendall(frame * take)
            remaining -= take
            cursor = (cursor + 1) % len(sockets)
        return time.perf_counter() - started
    finally:
        for sock in sockets:
            try:
                sock.close()
            except OSError:
                pass


async def _ingress_once(count: int, run_id: str) -> Sample:
    topic = f"bench/native-ingress/{run_id}"
    subscriber = AsyncClient(
        client_id=f"mqttium-ingress-{run_id}",
        message_delivery="callback",
        max_pending_callbacks=8_192,
    )
    delivered = 0
    complete = asyncio.Event()
    finished_at = 0.0

    def receive(_message: Any) -> None:
        nonlocal delivered, finished_at
        delivered += 1
        if delivered == count:
            finished_at = time.perf_counter()
            complete.set()

    subscriber.on_message = receive
    await subscriber.connect(BROKER_HOST, BROKER_PORT)
    try:
        await subscriber.subscribe(topic, qos=0)
        await asyncio.sleep(0.05)
        workers = 4
        per_worker = [count // workers] * workers
        for index in range(count % workers):
            per_worker[index] += 1
        start_event = threading.Event()
        ready: queue.SimpleQueue[None] = queue.SimpleQueue()
        publisher_tasks = [
            asyncio.create_task(
                asyncio.to_thread(
                    _raw_publish_worker,
                    topic,
                    worker_count,
                    worker_id,
                    start_event,
                    ready,
                )
            )
            for worker_id, worker_count in enumerate(per_worker)
        ]
        for _ in publisher_tasks:
            await asyncio.to_thread(ready.get)
        started = time.perf_counter()
        start_event.set()
        worker_durations = await asyncio.gather(*publisher_tasks)
        offered_finished = time.perf_counter()
        await asyncio.wait_for(complete.wait(), timeout=30.0)
        elapsed = finished_at - started
        stats = subscriber.stats()
        return Sample(
            name="ingress_qos0_exact_telemetry256",
            rate=count / elapsed,
            p50_ms=None,
            p95_ms=None,
            counters={
                "delivered": delivered,
                "offered_rate": count / (offered_finished - started),
                "slowest_publisher_seconds": max(worker_durations),
                "post_offer_drain_seconds": max(0.0, finished_at - offered_finished),
                "effect_batches": stats.effects.batches,
                "effect_inline": stats.effects.inline_effects,
                "effect_suspensions": stats.effects.apply_suspensions,
                "effect_high_water": stats.effects.pending_high_water,
                "callback_pending": stats.delivery.callback_queued,
                "delivery_waiters": stats.delivery.waiters,
                "decoder_high_water": stats.decoder.high_water_bytes,
            },
        )
    finally:
        await subscriber.disconnect()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    rtt_samples: list[Sample] = []
    ingress_samples: list[Sample] = []
    for run in range(args.runs):
        rtt_samples.append(await _rtt_once(args.rtt_pairs, 32, f"{args.label}-{run}"))
    for run in range(args.runs):
        ingress_samples.append(await _ingress_once(args.ingress_messages, f"{args.label}-{run}"))

    def aggregate(name: str, samples: list[Sample]) -> dict[str, Any]:
        rates = [sample.rate for sample in samples]
        p50_values = [sample.p50_ms for sample in samples if sample.p50_ms is not None]
        p95_values = [sample.p95_ms for sample in samples if sample.p95_ms is not None]
        return {
            "name": name,
            "rate_median": statistics.median(rates),
            "rate_samples": rates,
            "p50_ms_median": statistics.median(p50_values) if p50_values else None,
            "p95_ms_median": statistics.median(p95_values) if p95_values else None,
            "counters": [sample.counters for sample in samples],
        }

    return {
        "label": args.label,
        "rtt": aggregate("rtt_qos1_w32", rtt_samples),
        "ingress": aggregate("ingress_qos0_exact_telemetry256", ingress_samples),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--rtt-pairs", type=int, default=8_000)
    parser.add_argument("--ingress-messages", type=int, default=120_000)
    args = parser.parse_args()
    result = asyncio.run(_run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
