"""End-to-end compatibility publish regression tests."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from mqttium.api import AsyncClient
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.protocol.reconnect import ReconnectPolicy


@pytest.mark.asyncio
async def test_compat_qos0_callbacks_and_delivery_are_exactly_once() -> None:
    count = 1_000
    topic = f"integration/compat-qos0/{time.time_ns()}"
    subscriber = AsyncClient(
        client_id=f"sub-{time.time_ns()}",
        reconnect=ReconnectPolicy(enabled=False),
    )
    await subscriber.connect("127.0.0.1", 11883, timeout=5.0)
    await subscriber.subscribe(topic, timeout=5.0)

    received: list[bytes] = []

    async def collect() -> None:
        async for message in subscriber.messages():
            if message.topic != topic:
                continue
            received.append(message.payload)
            if len(received) == count:
                return

    collector = asyncio.create_task(collect())
    publisher = Client(
        CallbackAPIVersion.VERSION2,
        client_id=f"pub-{time.time_ns()}",
    )
    completed = 0
    completed_event = threading.Event()
    completed_lock = threading.Lock()

    def on_publish(*_args: object) -> None:
        nonlocal completed
        with completed_lock:
            completed += 1
            if completed == count:
                completed_event.set()

    publisher.on_publish = on_publish
    await asyncio.to_thread(publisher.connect, "127.0.0.1", 11883)
    try:
        started = time.perf_counter()

        def submit() -> float:
            for index in range(count):
                publisher.publish(topic, str(index).encode(), qos=0)
            return count / (time.perf_counter() - started)

        submit_rate = await asyncio.to_thread(submit)
        assert await asyncio.to_thread(completed_event.wait, 10.0)
        await asyncio.wait_for(collector, timeout=10.0)
        assert completed == count
        assert len(received) == count
        assert submit_rate > 5_000, f"submit_rate={submit_rate:.0f} too low"
    finally:
        await asyncio.to_thread(publisher.disconnect)
        publisher.loop_stop()
        if not collector.done():
            collector.cancel()
            with pytest.raises(asyncio.CancelledError):
                await collector
        await subscriber.disconnect()
