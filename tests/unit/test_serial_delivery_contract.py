"""One bounded callback worker and one byte charge per message."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.types import Message


@pytest.mark.parametrize("burst", [1, 2, 8])
async def test_each_message_is_one_job_in_fifo_order(burst) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    delivery = client._delivery
    started = asyncio.Event()
    release = asyncio.Event()
    seen = []

    async def active():
        started.set()
        await release.wait()
        seen.append("connect")

    await delivery.enqueue_callback(active)
    await started.wait()

    async def produce():
        for index in range(burst):
            await delivery.accept(
                Message(topic="t", payload=bytes([index])), lambda m: seen.append(m.payload[0])
            )
        await delivery.enqueue_callback(lambda: seen.append("publish"))

    producer = asyncio.create_task(produce())
    await asyncio.sleep(0)
    assert delivery.callback_queue.qsize() == min(2, burst + 1)
    # At most one additional producer-owned reservation waits for queue capacity.
    assert delivery.pending_bytes == min(3, burst) * 2
    assert seen == []
    release.set()
    await asyncio.wait_for(producer, 1)
    await delivery.callback_queue.join()
    assert seen == ["connect", *range(burst), "publish"]
    assert delivery.pending_bytes == 0
    await delivery.shutdown_callbacks(drain=False)


async def test_timeout_is_one_deadline_for_bytes_then_queue() -> None:
    client = AsyncClient(max_pending_messages=1, max_pending_delivery_bytes=5, delivery_timeout=0.2)
    delivery = client._delivery
    await delivery.accept(Message(topic="a", payload=b""), None)
    # This producer owns four bytes while it waits for the occupied queue slot.
    queued = asyncio.create_task(delivery.accept(Message(topic="bbbb", payload=b""), None))
    await asyncio.sleep(0)
    assert delivery.pending_bytes == 5
    loop = asyncio.get_running_loop()
    start = loop.time()
    waiting = asyncio.create_task(delivery.accept(Message(topic="ccc", payload=b""), None))
    await asyncio.sleep(0.12)
    assert delivery.waiters == 1
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    with pytest.raises(MessageDeliveryError):
        await waiting
    elapsed = loop.time() - start
    assert 0.18 <= elapsed < 0.29, "queue waiting must not start a second timeout"
    assert delivery.pending_bytes == 1
    assert delivery.waiters == 0
    assert (await anext(client.messages())).topic == "a"
    assert delivery.pending_bytes == 0


async def test_default_delivery_wait_has_no_deadline_and_cancels_cleanly() -> None:
    client = AsyncClient(max_pending_messages=1, max_pending_delivery_bytes=2)
    delivery = client._delivery
    assert delivery.delivery_timeout is None
    await delivery.accept(Message(topic="a", payload=b""), None)
    waiting = asyncio.create_task(delivery.accept(Message(topic="b", payload=b""), None))
    await asyncio.sleep(0)
    assert delivery.pending_bytes == 2
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert delivery.pending_bytes == 1
    assert (await anext(client.messages())).topic == "a"
    assert delivery.pending_bytes == 0


@pytest.mark.parametrize("kind", ["coroutine", "future", "error", "cancelled"])
async def test_worker_isolates_invalid_sync_callback_and_continues(kind) -> None:
    client = AsyncClient(message_delivery="callback")
    delivery = client._delivery
    loop = asyncio.get_running_loop()
    reported = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: reported.append(context))
    seen = []
    future = loop.create_future()
    returned = []

    async def coroutine():
        seen.append("must not run")

    def invalid(_message):
        if kind == "coroutine":
            result = coroutine()
            returned.append(result)
            return result
        if kind == "future":
            return future
        if kind == "cancelled":
            raise asyncio.CancelledError("callback raised")
        raise RuntimeError("callback raised")

    try:
        await delivery.accept(Message(topic="t", payload=b"x"), invalid)
        await delivery.accept(Message(topic="t", payload=b"y"), lambda m: seen.append(m.payload))
        await delivery.callback_queue.join()
        assert seen == [b"y"]
        assert len(reported) == 1
        assert delivery.pending_bytes == 0
        assert not future.done(), "returned futures are not owned by the worker"
        if returned:
            assert returned[0].cr_frame is None
    finally:
        loop.set_exception_handler(previous)
        await delivery.shutdown_callbacks(drain=False)


async def test_worker_cancellation_releases_active_and_queued_charges() -> None:
    client = AsyncClient(message_delivery="callback")
    delivery = client._delivery
    started = asyncio.Event()

    async def active(_message):
        started.set()
        await asyncio.Event().wait()

    await delivery.accept(Message(topic="a", payload=b"x"), active)
    await started.wait()
    await delivery.accept(Message(topic="b", payload=b"y"), active)
    assert delivery.pending_bytes == 4
    await delivery.shutdown_callbacks(drain=False)
    assert delivery.pending_bytes == 0
    assert delivery.callback_queue.empty()
    await delivery.callback_queue.join()


async def test_reentrant_worker_shutdown_does_not_wait_on_itself() -> None:
    client = AsyncClient(message_delivery="callback")
    delivery = client._delivery
    seen = []

    async def stop(_message):
        await client.disconnect()
        seen.append("stopped")

    await delivery.accept(Message(topic="a", payload=b"x"), stop)
    await delivery.accept(Message(topic="b", payload=b"y"), lambda _: seen.append("discarded"))
    await asyncio.wait_for(delivery.callback_queue.join(), 1)
    assert seen == ["stopped"]
    assert delivery.pending_bytes == 0
    await delivery.shutdown_callbacks(drain=False)
