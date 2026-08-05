"""Controlled decode-to-callback ingress benchmark.

A minimal in-memory MQTT 3.1.1 transport performs only CONNECT/SUBSCRIBE
handshakes and then feeds pre-encoded QoS 0 PUBLISH packets. This deliberately
removes publisher and broker backpressure so the measured ceiling belongs to the
AsyncClient reader, decoder, effect and callback-delivery path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import struct
import time
from collections import deque
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient

PAYLOAD = b"t" * 256
TOPIC = "bench/direct/telemetry"


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


def _publish_frame() -> bytes:
    topic = TOPIC.encode("utf-8")
    body = struct.pack("!H", len(topic)) + topic + PAYLOAD
    return b"\x30" + _encode_vbi(len(body)) + body


def _packet_body_offset(packet: bytes) -> int:
    offset = 1
    while packet[offset] & 0x80:
        offset += 1
    return offset + 1


class ProbeTransport:
    def __init__(self) -> None:
        self._incoming: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._pending: deque[bytes] = deque()
        self._closing = False

    async def write(self, data: bytes) -> None:
        packet_type = data[0] >> 4
        if packet_type == 1:  # CONNECT
            self._incoming.put_nowait(b"\x20\x02\x00\x00")
        elif packet_type == 8:  # SUBSCRIBE
            body = _packet_body_offset(data)
            mid = data[body : body + 2]
            self._incoming.put_nowait(b"\x90\x03" + mid + b"\x00")

    async def write_many(self, parts: list[bytes]) -> None:
        for part in parts:
            await self.write(part)

    async def read(self, n: int = 65_536) -> bytes:
        if self._pending:
            data = self._pending.popleft()
        else:
            item = await self._incoming.get()
            if item is None:
                return b""
            data = item
        if len(data) <= n:
            return data
        self._pending.appendleft(data[n:])
        return data[:n]

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._incoming.put_nowait(None)

    def is_closing(self) -> bool:
        return self._closing

    def feed(self, data: bytes) -> None:
        self._incoming.put_nowait(data)


async def _once(messages: int, run_id: str) -> dict[str, Any]:
    transport = ProbeTransport()
    client = AsyncClient(
        client_id=f"direct-ingress-{run_id}",
        message_delivery="callback",
        max_pending_callbacks=8_192,
    )

    async def factory(
        _host: str,
        _port: int,
        *,
        ssl: object | None = None,
    ) -> ProbeTransport:
        del ssl
        return transport

    client._transport_factory = factory
    delivered = 0
    completed = asyncio.Event()
    finished = 0.0

    def receive(_message: Any) -> None:
        nonlocal delivered, finished
        delivered += 1
        if delivered == messages:
            finished = time.perf_counter()
            completed.set()

    client.on_message = receive
    await client.connect("probe", 0)
    try:
        await client.subscribe(TOPIC, qos=0)
        frame = _publish_frame()
        frames_per_chunk = max(1, 65_536 // len(frame))
        chunk = frame * frames_per_chunk
        full_chunks, remainder = divmod(messages, frames_per_chunk)
        started = time.perf_counter()
        for _ in range(full_chunks):
            transport.feed(chunk)
        if remainder:
            transport.feed(frame * remainder)
        await asyncio.wait_for(completed.wait(), timeout=30.0)
        elapsed = finished - started
        stats = client.stats()
        return {
            "rate": messages / elapsed,
            "elapsed": elapsed,
            "delivered": delivered,
            "effect_batches": stats.effects.batches,
            "effect_inline": stats.effects.inline_effects,
            "effect_suspensions": stats.effects.apply_suspensions,
            "effect_high_water": stats.effects.pending_high_water,
            "decoder_high_water": stats.decoder.high_water_bytes,
            "callback_queued": stats.delivery.callback_queued,
            "delivery_waiters": stats.delivery.waiters,
        }
    finally:
        await client.disconnect()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    samples = [
        await _once(args.messages, f"{args.label}-{run}") for run in range(args.runs)
    ]
    return {
        "label": args.label,
        "messages": args.messages,
        "rate_median": statistics.median(sample["rate"] for sample in samples),
        "rate_samples": [sample["rate"] for sample in samples],
        "samples": samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--messages", type=int, default=160_000)
    args = parser.parse_args()
    result = asyncio.run(_run(args))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
