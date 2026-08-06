from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EngineEffect
from mqttium.types import Message

TOPIC = "bench/native-hotpath/telemetry"
EXACT_FILTER = TOPIC
WILDCARD_FILTER = "bench/+/telemetry"


def _encode_vbi(value: int) -> bytes:
    out = bytearray()
    while True:
        value, digit = divmod(value, 128)
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def _publish_frame(topic: str, payload: bytes) -> bytes:
    topic_bytes = topic.encode()
    body = struct.pack("!H", len(topic_bytes)) + topic_bytes + payload
    return b"\x30" + _encode_vbi(len(body)) + body


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    out = bytearray()
    while len(out) < size:
        chunk = sock.recv(size - len(out))
        if not chunk:
            raise ConnectionError("socket closed")
        out.extend(chunk)
    return bytes(out)


def _recv_packet(sock: socket.socket) -> tuple[int, bytes]:
    first = _recv_exact(sock, 1)[0]
    multiplier = 1
    remaining = 0
    while True:
        digit = _recv_exact(sock, 1)[0]
        remaining += (digit & 0x7F) * multiplier
        if not digit & 0x80:
            break
        multiplier *= 128
    return first, _recv_exact(sock, remaining)


class DirectBroker:
    def __init__(self, *, count: int, payload: bytes) -> None:
        self.count = count
        self.payload = payload
        self.started = threading.Event()
        self.subscribed = threading.Event()
        self.stop = threading.Event()
        self.offered_started = 0.0
        self.offered_finished = 0.0
        self.error: BaseException | None = None
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.port = self._listener.getsockname()[1]
        self._thread = threading.Thread(target=self._run, daemon=True)

    def launch(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        conn: socket.socket | None = None
        try:
            conn, _ = self._listener.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            conn.settimeout(60.0)
            packet_type, _body = _recv_packet(conn)
            if packet_type >> 4 != 1:
                raise RuntimeError("expected CONNECT")
            conn.sendall(b"\x20\x02\x00\x00")
            packet_type, body = _recv_packet(conn)
            if packet_type >> 4 != 8:
                raise RuntimeError("expected SUBSCRIBE")
            mid = body[:2]
            conn.sendall(b"\x90\x03" + mid + b"\x00")
            self.subscribed.set()
            if not self.started.wait(30.0):
                raise TimeoutError("direct broker start timeout")
            frame = _publish_frame(TOPIC, self.payload)
            frames_per_chunk = max(1, (256 * 1024) // len(frame))
            chunk = frame * frames_per_chunk
            full, tail = divmod(self.count, frames_per_chunk)
            self.offered_started = time.perf_counter()
            for _ in range(full):
                conn.sendall(chunk)
            if tail:
                conn.sendall(frame * tail)
            self.offered_finished = time.perf_counter()
            self.stop.wait(30.0)
        except BaseException as exc:
            self.error = exc
            self.subscribed.set()
        finally:
            if conn is not None:
                try:
                    conn.close()
                except OSError:
                    pass
            self._listener.close()

    def close(self) -> None:
        self.stop.set()
        self._thread.join(timeout=5.0)
        if self.error is not None:
            raise self.error


class LagProbe:
    def __init__(self, interval: float = 0.001) -> None:
        self.interval = interval
        self.samples: list[float] = []
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        expected = loop.time() + self.interval
        while not self.stop_event.is_set():
            await asyncio.sleep(self.interval)
            now = loop.time()
            self.samples.append(max(0.0, now - expected) * 1000.0)
            expected = now + self.interval

    async def stop(self) -> dict[str, float]:
        self.stop_event.set()
        if self.task is not None:
            await self.task
        return {
            "loop_lag_p50_ms": _percentile(self.samples, 0.50),
            "loop_lag_p95_ms": _percentile(self.samples, 0.95),
            "loop_lag_p99_ms": _percentile(self.samples, 0.99),
        }


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


async def _wait_thread_event(event: threading.Event, timeout: float = 30.0) -> None:
    if not await asyncio.to_thread(event.wait, timeout):
        raise TimeoutError("thread event timeout")


class MeasuredAsyncClient(AsyncClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.callback_hwm = 0
        self.iterator_hwm = 0

    def _sample_delivery_queues(self) -> None:
        self.callback_hwm = max(self.callback_hwm, self._callback_queue.qsize())
        self.iterator_hwm = max(self.iterator_hwm, self._messages.qsize())

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        await super()._apply_effect(effect, nowait=nowait, epoch=epoch)
        self._sample_delivery_queues()


def _client_counters(client: MeasuredAsyncClient) -> dict[str, Any]:
    stats = client.stats()
    return {
        "effect_batches": stats.effects.batches,
        "effect_multi_batches": stats.effects.multi_effect_batches,
        "effect_suspensions": stats.effects.apply_suspensions,
        "effect_hwm": stats.effects.pending_high_water,
        "callback_hwm": client.callback_hwm,
        "iterator_hwm": client.iterator_hwm,
        "delivery_pending_hwm_bytes": stats.delivery.pending_high_water_bytes,
        "writer_batches": stats.writer.batches,
        "writer_items": stats.writer.batched_items,
        "receipts": stats.receipts.publish,
    }


async def _mqttium_case(
    *,
    mode: str,
    count: int,
    payload: bytes,
    subscription: str,
) -> dict[str, Any]:
    delivery = "both" if mode == "both_sync" else ("iterator" if mode == "iterator" else "callback")
    broker = DirectBroker(count=count, payload=payload)
    broker.launch()
    client = MeasuredAsyncClient(
        client_id=f"ingress-{mode}-{os.getpid()}",
        message_delivery=delivery,
        max_pending_callbacks=8_192,
        max_pending_messages=8_192,
    )
    callback_count = 0
    iterator_count = 0
    finished = 0.0
    complete = asyncio.Event()

    def maybe_complete() -> None:
        nonlocal finished
        callback_done = mode == "iterator" or callback_count == count
        iterator_done = mode not in ("iterator", "both_sync") or iterator_count == count
        if callback_done and iterator_done and not complete.is_set():
            finished = time.perf_counter()
            complete.set()

    if mode in ("callback_sync", "both_sync"):

        def on_message(_message: Message) -> None:
            nonlocal callback_count
            callback_count += 1
            if callback_count == count:
                maybe_complete()

        client.on_message = on_message
    elif mode == "callback_async":

        async def on_message(_message: Message) -> None:
            nonlocal callback_count
            callback_count += 1
            if callback_count == count:
                maybe_complete()

        client.on_message = on_message

    iterator_task: asyncio.Task[None] | None = None
    if mode in ("iterator", "both_sync"):

        async def consume(messages: AsyncIterator[Message]) -> None:
            nonlocal iterator_count
            async for _message in messages:
                iterator_count += 1
                if iterator_count == count:
                    maybe_complete()
                    return

        iterator_task = asyncio.create_task(consume(client.messages()))

    lag = LagProbe()
    await client.connect("127.0.0.1", broker.port)
    try:
        await client.subscribe(subscription, qos=0)
        await _wait_thread_event(broker.subscribed)
        lag.start()
        started = time.perf_counter()
        broker.started.set()
        await asyncio.wait_for(complete.wait(), timeout=90.0)
        elapsed = finished - started
        counters = _client_counters(client)
        counters.update(await lag.stop())
        counters.update(
            {
                "callback_delivered": callback_count,
                "iterator_delivered": iterator_count,
                "completion_ratio": min(
                    callback_count if mode != "iterator" else count,
                    iterator_count if mode in ("iterator", "both_sync") else count,
                )
                / count,
                "offered_rate": count / max(1e-9, broker.offered_finished - broker.offered_started),
                "post_offer_drain_ms": max(0.0, finished - broker.offered_finished) * 1000.0,
            }
        )
        return {
            "library": "mqttium",
            "mode": mode,
            "count": count,
            "payload_size": len(payload),
            "subscription": subscription,
            "elapsed": elapsed,
            "rate": count / elapsed,
            "counters": counters,
        }
    finally:
        if iterator_task is not None and not iterator_task.done():
            iterator_task.cancel()
            try:
                await iterator_task
            except asyncio.CancelledError:
                pass
        broker.stop.set()
        await client.disconnect()
        broker.close()


async def _gmqtt_case(
    *,
    mode: str,
    count: int,
    payload: bytes,
    subscription: str,
) -> dict[str, Any]:
    from gmqtt import Client
    from gmqtt.mqtt.constants import MQTTv311

    broker = DirectBroker(count=count, payload=payload)
    broker.launch()
    client = Client(f"ingress-gmqtt-{mode}-{os.getpid()}", clean_session=True)
    delivered = 0
    finished = 0.0
    complete = asyncio.Event()
    subscribed = asyncio.Event()

    if mode == "callback_sync":

        def on_message(*_args: Any) -> None:
            nonlocal delivered, finished
            delivered += 1
            if delivered == count:
                finished = time.perf_counter()
                complete.set()

    elif mode == "callback_async":

        async def on_message(*_args: Any) -> None:
            nonlocal delivered, finished
            delivered += 1
            if delivered == count:
                finished = time.perf_counter()
                complete.set()

    else:
        raise ValueError(f"unsupported gmqtt mode {mode}")

    client.on_message = on_message
    client.on_subscribe = lambda *_args: subscribed.set()
    lag = LagProbe()
    await client.connect("127.0.0.1", port=broker.port, keepalive=60, version=MQTTv311)
    try:
        transport = client._connection._transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client.subscribe(subscription, qos=0)
        await asyncio.wait_for(subscribed.wait(), timeout=10.0)
        await _wait_thread_event(broker.subscribed)
        lag.start()
        started = time.perf_counter()
        broker.started.set()
        await asyncio.wait_for(complete.wait(), timeout=90.0)
        elapsed = finished - started
        counters = await lag.stop()
        counters.update(
            {
                "delivered": delivered,
                "completion_ratio": delivered / count,
                "offered_rate": count / max(1e-9, broker.offered_finished - broker.offered_started),
                "post_offer_drain_ms": max(0.0, finished - broker.offered_finished) * 1000.0,
            }
        )
        return {
            "library": "gmqtt",
            "mode": mode,
            "count": count,
            "payload_size": len(payload),
            "subscription": subscription,
            "elapsed": elapsed,
            "rate": count / elapsed,
            "counters": counters,
        }
    finally:
        broker.stop.set()
        try:
            await client.disconnect()
        except Exception:
            pass
        broker.close()


async def _worker(args: argparse.Namespace) -> dict[str, Any]:
    payload = b"p" * args.payload_size
    if args.library == "mqttium":
        return await _mqttium_case(
            mode=args.mode,
            count=args.count,
            payload=payload,
            subscription=args.subscription,
        )
    return await _gmqtt_case(
        mode=args.mode,
        count=args.count,
        payload=payload,
        subscription=args.subscription,
    )


def _sample(
    *,
    root: Path,
    library: str,
    mode: str,
    payload_size: int,
    count: int,
    subscription: str,
) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--library",
        library,
        "--mode",
        mode,
        "--payload-size",
        str(payload_size),
        "--count",
        str(count),
        "--subscription",
        subscription,
    ]
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


def _driver(args: argparse.Namespace) -> None:
    root = args.root.resolve()
    cases: list[tuple[str, str, int, int, str]] = []
    payload_counts = {0: 160_000, 256: 160_000, 4096: 20_000, 65_536: 2_000}
    modes = (
        ("mqttium", "callback_sync"),
        ("mqttium", "callback_async"),
        ("mqttium", "iterator"),
        ("mqttium", "both_sync"),
        ("gmqtt", "callback_sync"),
        ("gmqtt", "callback_async"),
    )
    for payload_size, count in payload_counts.items():
        for library, mode in modes:
            cases.append((library, mode, payload_size, count, EXACT_FILTER))
    for library, mode in modes:
        cases.append((library, mode, 256, 160_000, WILDCARD_FILTER))

    rows: list[dict[str, Any]] = []
    for cycle in range(args.cycles):
        shift = cycle % len(cases)
        rotated = cases[shift:] + cases[:shift]
        order = rotated if cycle % 2 == 0 else list(reversed(rotated))
        samples: list[dict[str, Any]] = []
        for library, mode, payload_size, count, subscription in order:
            samples.append(
                _sample(
                    root=root,
                    library=library,
                    mode=mode,
                    payload_size=payload_size,
                    count=count,
                    subscription=subscription,
                )
            )
        rows.append({"cycle": cycle, "samples": samples})
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"rows": rows}, indent=2) + "\n")

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        for sample in row["samples"]:
            key = "|".join(
                (
                    sample["library"],
                    sample["mode"],
                    str(sample["payload_size"]),
                    sample["subscription"],
                )
            )
            grouped.setdefault(key, []).append(sample)
    summary = {
        key: {
            "rate_median": statistics.median(item["rate"] for item in samples),
            "rate_min": min(item["rate"] for item in samples),
            "rate_max": max(item["rate"] for item in samples),
            "loop_lag_p95_ms_median": statistics.median(
                item["counters"]["loop_lag_p95_ms"] for item in samples
            ),
            "post_offer_drain_ms_median": statistics.median(
                item["counters"]["post_offer_drain_ms"] for item in samples
            ),
            "completion_ratio_min": min(item["counters"]["completion_ratio"] for item in samples),
        }
        for key, samples in grouped.items()
    }
    args.output.write_text(json.dumps({"rows": rows, "summary": summary}, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--library", choices=("mqttium", "gmqtt"))
    parser.add_argument(
        "--mode",
        choices=("callback_sync", "callback_async", "iterator", "both_sync"),
    )
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--count", type=int, default=160_000)
    parser.add_argument("--subscription", default=EXACT_FILTER)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker:
        if args.library is None or args.mode is None:
            parser.error("worker mode requires --library and --mode")
        print("RESULT " + json.dumps(asyncio.run(_worker(args))))
        return
    if args.root is None or args.output is None:
        parser.error("driver mode requires --root and --output")
    _driver(args)


if __name__ == "__main__":
    main()
