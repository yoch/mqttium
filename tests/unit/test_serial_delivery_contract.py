"""Inline synchronous callbacks and one shared deadline for iterator waits."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.errors import MessageDeliveryError
from mqttium.types import Message
from tests.support import accept_message


@pytest.mark.parametrize("burst", [1, 2, 8])
async def test_callback_mode_runs_each_message_inline_in_fifo_order(burst) -> None:
    client = AsyncClient(message_delivery="callback")
    delivery = client._delivery
    seen = []

    for index in range(burst):
        message = Message(topic="t", payload=bytes([index]))
        assert delivery.accept(message, lambda m: seen.append(m.payload[0])) is None
        # The callback has already run when accept returns.
        assert seen[-1] == index

    assert seen == list(range(burst))
    assert delivery.callback_invocations == burst
    assert delivery.stats().callback_invocations == burst
    assert delivery.pending_bytes == 0
    assert delivery.messages_queue.empty()


async def test_timeout_is_one_deadline_across_queue_and_byte_waits() -> None:
    client = AsyncClient(
        max_iterator_messages=1, max_iterator_bytes=5, iterator_admission_timeout=0.2
    )
    delivery = client._delivery
    await accept_message(delivery, Message(topic="a", payload=b""))
    loop = asyncio.get_running_loop()
    start = loop.time()
    queued = asyncio.create_task(accept_message(delivery, Message(topic="bbbb", payload=b"")))
    waiting = asyncio.create_task(accept_message(delivery, Message(topic="ccc", payload=b"")))
    await asyncio.sleep(0)
    # Waiting producers own no bytes until they enqueue.
    assert delivery.pending_bytes == 1
    assert delivery.waiters == 2
    await asyncio.sleep(0.12)

    stream = client.messages()
    assert (await anext(stream)).topic == "a"
    await asyncio.wait_for(queued, 1)
    assert delivery.pending_bytes == 4
    # The remaining producer got the queue slot wakeup but still lacks bytes;
    # its original deadline must not restart.
    with pytest.raises(MessageDeliveryError, match="Application delivery capacity"):
        await waiting
    elapsed = loop.time() - start
    assert 0.18 <= elapsed < 0.29, "re-waiting after a wakeup must not start a second timeout"
    assert delivery.waiters == 0
    assert (await anext(stream)).topic == "bbbb"
    assert delivery.pending_bytes == 0
    await stream.aclose()


async def test_default_delivery_wait_has_no_deadline_and_cancels_cleanly() -> None:
    client = AsyncClient(max_iterator_messages=1, max_iterator_bytes=2)
    delivery = client._delivery
    assert delivery.iterator_admission_timeout is None
    await accept_message(delivery, Message(topic="a", payload=b""))
    waiting = asyncio.create_task(accept_message(delivery, Message(topic="b", payload=b"")))
    await asyncio.sleep(0)
    assert delivery.pending_bytes == 1
    assert delivery.waiters == 1
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert delivery.waiters == 0
    assert delivery.pending_bytes == 1
    assert (await anext(client.messages())).topic == "a"
    assert delivery.pending_bytes == 0


@pytest.mark.parametrize("kind", ["coroutine", "future", "error", "cancelled"])
async def test_inline_callback_errors_are_isolated_and_delivery_continues(kind) -> None:
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
        await accept_message(delivery, Message(topic="t", payload=b"x"), invalid)
        await accept_message(
            delivery, Message(topic="t", payload=b"y"), lambda m: seen.append(m.payload)
        )
        assert seen == [b"y"]
        assert len(reported) == 1
        assert reported[0]["message"] == "mqttium user callback failed"
        assert reported[0]["callback"] is invalid
        if kind in ("coroutine", "future"):
            assert isinstance(reported[0]["exception"], TypeError)
        assert delivery.callback_invocations == 2
        assert delivery.pending_bytes == 0
        assert not future.done(), "returned futures are not owned by the delivery"
        if returned:
            assert returned[0].cr_frame is None
    finally:
        loop.set_exception_handler(previous)


async def test_sync_callback_can_schedule_explicit_application_owned_shutdown() -> None:
    client = AsyncClient(message_delivery="callback")
    delivery = client._delivery
    seen = []
    tasks = []

    async def shutdown():
        await client.disconnect()
        seen.append("stopped")

    def callback(_message):
        tasks.append(asyncio.create_task(shutdown()))

    await accept_message(delivery, Message(topic="a", payload=b"x"), callback)
    assert len(tasks) == 1
    await asyncio.wait_for(tasks[0], 1)
    assert seen == ["stopped"]
    assert delivery.pending_bytes == 0
