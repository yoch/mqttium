"""End-to-end compatibility publish regression tests."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from mqttium.api import AsyncClient
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.protocol.reconnect import ReconnectPolicy


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

        def submit() -> None:
            for index in range(count):
                publisher.publish(topic, str(index).encode(), qos=0)

        await asyncio.to_thread(submit)
        assert await asyncio.to_thread(completed_event.wait, 10.0)
        await asyncio.wait_for(collector, timeout=10.0)
        assert completed == count
        assert len(received) == count
    finally:
        await asyncio.to_thread(publisher.disconnect)
        publisher.loop_stop()
        if not collector.done():
            collector.cancel()
            with pytest.raises(asyncio.CancelledError):
                await collector
        await subscriber.disconnect()


async def test_compat_qos1_concurrent_submission_completes_exactly_once() -> None:  # noqa: C901
    count = 400
    workers = 8
    topic = f"integration/compat-qos1/{time.time_ns()}"
    subscriber = AsyncClient(
        client_id=f"sub-qos1-{time.time_ns()}",
        reconnect=ReconnectPolicy(enabled=False),
    )
    await subscriber.connect("127.0.0.1", 11883, timeout=5.0)
    await subscriber.subscribe(topic, qos=1, timeout=5.0)

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
        client_id=f"pub-qos1-{time.time_ns()}",
        max_pending_outbound_messages=count,
        max_pending_outbound_bytes=None,
    )
    completed = 0
    completed_event = threading.Event()
    completed_lock = threading.Lock()
    callback_errors: list[int] = []

    def on_publish(_client, _userdata, _mid, reason_code, _properties) -> None:
        nonlocal completed
        with completed_lock:
            if reason_code != 0:
                callback_errors.append(reason_code)
            completed += 1
            if completed == count:
                completed_event.set()

    publisher.on_publish = on_publish
    await asyncio.to_thread(publisher.connect, "127.0.0.1", 11883)
    try:

        def submit_concurrently():
            per_worker = count // workers
            barrier = threading.Barrier(workers)
            infos = []
            errors: list[BaseException] = []
            result_lock = threading.Lock()

            def worker(worker_id: int) -> None:
                local = []
                try:
                    barrier.wait()
                    base = worker_id * per_worker
                    for index in range(per_worker):
                        local.append(
                            publisher.publish(
                                topic,
                                str(base + index).encode(),
                                qos=1,
                            )
                        )
                except BaseException as exc:
                    with result_lock:
                        errors.append(exc)
                finally:
                    with result_lock:
                        infos.extend(local)

            threads = [threading.Thread(target=worker, args=(index,)) for index in range(workers)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=15.0)
            if any(thread.is_alive() for thread in threads):
                raise RuntimeError("QoS 1 publisher thread did not finish")
            if errors:
                raise errors[0]
            return infos

        infos = await asyncio.to_thread(submit_concurrently)
        assert len(infos) == count
        assert all(info.rc == 0 and info.mid is not None for info in infos)

        for info in infos:
            await asyncio.to_thread(info.wait_for_publish, 10.0)
        assert await asyncio.to_thread(completed_event.wait, 10.0)
        await asyncio.wait_for(collector, timeout=15.0)

        assert callback_errors == []
        assert completed == count
        assert len(received) == count
    finally:
        await asyncio.to_thread(publisher.disconnect)
        publisher.loop_stop()
        if not collector.done():
            collector.cancel()
            with pytest.raises(asyncio.CancelledError):
                await collector
        await subscriber.disconnect()
