"""Actual callback invocations, including route fan-out, share a bounded quantum."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.api._delivery import ApplicationDelivery, _CALLBACK_QUANTUM
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
@pytest.mark.parametrize("kind", ["sync", "error", "cancelled"])
async def test_ready_heartbeat_runs_within_one_quantum(task_factory, jobs, kind):
    delivery = _delivery()
    seen, errors = [], []
    loop = asyncio.get_running_loop()
    heartbeat = loop.create_future()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context["exception"]))

    def callback(message):
        seen.append(int(message.payload))
        if len(seen) == 1:
            loop.call_soon(lambda: heartbeat.set_result(len(seen)))
        if len(seen) == 63 and kind == "error":
            raise ValueError("isolated callback fault")
        if len(seen) == 63 and kind == "cancelled":
            raise asyncio.CancelledError("user callback cancellation")

    try:
        for index in range(jobs):
            assert delivery.try_accept(Message("t", str(index).encode()), callback)
        assert seen == []  # Also true with eager task creation.
        assert await asyncio.wait_for(heartbeat, 1) == min(jobs, _CALLBACK_QUANTUM)
        await asyncio.wait_for(delivery.callback_queue.join(), 1)
        assert seen == list(range(jobs))
        assert delivery.pending_bytes == 0
        assert len(errors) == int(kind in ("error", "cancelled"))
    finally:
        await delivery.shutdown_callbacks(drain=False)
        loop.set_exception_handler(previous)


@pytest.mark.parametrize("fanout", [_CALLBACK_QUANTUM + 1, 500])
async def test_route_fanout_yields_and_retains_message_bytes(task_factory, fanout):
    client = AsyncClient(message_delivery="callback")
    loop = asyncio.get_running_loop()
    heartbeat = loop.create_future()
    seen, charges = [], []
    message = Message("/".join(["t"] * 9), b"owned")
    logical_size = client._delivery.logical_size(message)

    def callback(_message):
        seen.append(len(seen))
        charges.append(client._delivery.pending_bytes)
        if len(seen) == 1:
            loop.call_soon(
                lambda: heartbeat.set_result((len(seen), client._delivery.pending_bytes))
            )

    for index in range(fanout):
        topic_filter = "/".join("+" if index & (1 << bit) else "t" for bit in range(9))
        client.message_callback_add(topic_filter, callback)
    client._freeze_message_routes()
    assert client._delivery.try_accept(message, client._message_callback)
    assert await asyncio.wait_for(heartbeat, 1) == (_CALLBACK_QUANTUM, logical_size)
    await asyncio.wait_for(client._delivery.callback_queue.join(), 1)
    assert seen == list(range(fanout))
    assert charges == [logical_size] * fanout
    assert client._delivery.pending_bytes == 0
    await client._delivery.shutdown_callbacks(drain=False)


@pytest.mark.parametrize("routed", [False, True])
async def test_worker_cancelled_at_quantum_releases_current_and_queued_work(task_factory, routed):
    client = AsyncClient(message_delivery="callback")
    delivery = client._delivery
    loop = asyncio.get_running_loop()
    cancelled = loop.create_future()
    seen = []

    def cancel_worker():
        assert len(seen) == _CALLBACK_QUANTUM
        delivery.callback_task.cancel()
        cancelled.set_result(None)

    def callback(_message):
        seen.append(len(seen))
        if len(seen) == 1:
            loop.call_soon(cancel_worker)

    if routed:
        for index in range(_CALLBACK_QUANTUM * 2):
            topic_filter = "/".join("+" if index & (1 << bit) else "t" for bit in range(9))
            client.message_callback_add(topic_filter, callback)
        message = Message("/".join(["t"] * 9), b"payload")
        assert delivery.try_accept(message, client._message_callback)
        assert delivery.try_accept(message, client._message_callback)
    else:
        for index in range(_CALLBACK_QUANTUM * 2):
            assert delivery.try_accept(Message("t", str(index).encode()), callback)
    await asyncio.wait_for(cancelled, 1)
    await delivery.shutdown_callbacks(drain=False)
    await asyncio.wait_for(delivery.callback_queue.join(), 1)
    assert len(seen) == _CALLBACK_QUANTUM
    assert delivery.pending_bytes == 0
    assert delivery.callback_task is None
    delivery.reopen()
    replacement = []
    assert delivery.try_accept(Message("t", b"new"), lambda message: replacement.append(message))
    await asyncio.wait_for(delivery.callback_queue.join(), 1)
    assert [message.payload for message in replacement] == [b"new"]
    assert delivery.pending_bytes == 0
    await delivery.shutdown_callbacks(drain=False)
