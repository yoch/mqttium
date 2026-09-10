"""Bounded callback work gives other ready tasks a turn without losing jobs."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api._delivery import ApplicationDelivery
from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Message
from tests.unit.test_callback_lifecycle_regressions import task_factory as task_factory


def _delivery():
    return ApplicationDelivery(
        mode="callback",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_pending_messages=2048,
        max_pending_callbacks=2048,
        max_pending_delivery_bytes=65536,
        delivery_timeout=1,
        callback_shutdown_timeout=1,
    )


@pytest.mark.parametrize("jobs", [63, 64, 65, 1025])
@pytest.mark.parametrize("kind", ["sync", "async", "error", "cancelled"])
async def test_ready_heartbeat_runs_within_one_quantum(task_factory, jobs, kind):
    delivery = _delivery()
    gate, entered, ready = (asyncio.Event() for _ in range(3))
    seen, heartbeat_counts, errors = [], [], []
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context["exception"]))

    async def first():
        entered.set()
        await gate.wait()

    def callback(message):
        seen.append(int(message.payload))
        if len(seen) == 63 and kind == "error":
            raise ValueError("isolated callback fault")
        if len(seen) == 63 and kind == "cancelled":
            raise asyncio.CancelledError("user callback cancellation")

    async def async_callback(message):
        callback(message)

    async def heartbeat():
        await ready.wait()
        heartbeat_counts.append(len(seen))

    task = None
    try:
        await delivery.enqueue_callback(first)
        await asyncio.wait_for(entered.wait(), 1)
        for index in range(jobs - 1):
            await delivery.accept(
                Message("t", str(index).encode()), async_callback if kind == "async" else callback
            )
        task = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)  # Both task factories have parked the heartbeat.
        gate.set()  # Queue the worker before the heartbeat.
        ready.set()
        await asyncio.wait_for(task, 1)
        assert heartbeat_counts == [min(jobs - 1, 63)]
        await asyncio.wait_for(delivery.callback_queue.join(), 1)
        assert seen == list(range(jobs - 1))
        assert delivery.pending_bytes == 0
        assert len(errors) == int(jobs >= 64 and kind in ("error", "cancelled"))
    finally:
        gate.set()
        ready.set()
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await delivery.shutdown_callbacks(drain=False)
        loop.set_exception_handler(old_handler)
    assert delivery.callback_task is None


async def test_worker_cancelled_at_quantum_releases_remaining_jobs(task_factory):
    delivery = _delivery()
    gate, entered, ready = (asyncio.Event() for _ in range(3))
    seen = []

    async def first():
        entered.set()
        await gate.wait()

    async def cancel_at_yield():
        await ready.wait()
        assert len(seen) == 63
        delivery.callback_task.cancel()

    await delivery.enqueue_callback(first)
    await asyncio.wait_for(entered.wait(), 1)
    for index in range(128):
        await delivery.accept(Message("t", bytes([index])), lambda message: seen.append(message))
    canceller = asyncio.create_task(cancel_at_yield())
    await asyncio.sleep(0)
    gate.set()
    ready.set()
    await asyncio.wait_for(canceller, 1)
    await delivery.shutdown_callbacks(drain=False)
    await asyncio.wait_for(delivery.callback_queue.join(), 1)
    assert len(seen) == 63
    assert delivery.pending_bytes == 0
    assert delivery.callback_task is None


@pytest.mark.parametrize("reopen", [False, True])
async def test_self_stop_at_quantum_preserves_reopen_ownership(task_factory, reopen):
    delivery = _delivery()
    entered, gate = asyncio.Event(), asyncio.Event()
    seen = []

    async def callback(message):
        index = int(message.payload)
        seen.append(index)
        if index == 0:
            entered.set()
            await gate.wait()
        if index == 63:
            worker = asyncio.current_task()
            await delivery.shutdown_callbacks(drain=False)
            if reopen:
                delivery.reopen()
                assert delivery.callback_task is worker
                await delivery.accept(Message("t", b"999"), callback)

    await delivery.accept(Message("t", b"0"), callback)
    await asyncio.wait_for(entered.wait(), 1)
    for index in range(1, 129):
        await delivery.accept(Message("t", str(index).encode()), callback)
    gate.set()
    await asyncio.wait_for(delivery.callback_queue.join(), 1)
    await delivery.shutdown_callbacks(drain=False)
    assert seen == list(range(64)) + ([999] if reopen else [])
    assert delivery.pending_bytes == 0
    assert delivery.callback_task is None
