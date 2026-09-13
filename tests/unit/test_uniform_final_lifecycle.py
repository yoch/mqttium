"""Final adverse check for uniform delivery generation ownership."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.types import Message


@pytest.fixture(params=[False, True], ids=["normal", "eager"])
async def scheduler(request):
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    handler = loop.get_exception_handler()
    errors: list[dict[str, object]] = []
    if request.param:
        if not hasattr(asyncio, "eager_task_factory"):
            pytest.skip("eager tasks require Python 3.12+")
        loop.set_task_factory(asyncio.eager_task_factory)
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        yield
        await asyncio.sleep(0)
        assert not errors, errors
    finally:
        loop.set_task_factory(factory)
        loop.set_exception_handler(handler)


async def finish(client: AsyncClient) -> None:
    await asyncio.wait_for(client._shutdown_callback_worker(drain=False), 1)
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert client._callback_queue.empty()
    assert client._callback_worker_task is None
    assert not client._delivery._callback_active


async def test_explicit_stream_reset_wakes_old_iterator_without_fresh_message(scheduler) -> None:
    client = AsyncClient(message_delivery="iterator", max_pending_messages=1)
    delivery = client._delivery
    queue = delivery.messages_queue
    ready = delivery.message_ready
    stream = client.messages()
    waiting = asyncio.create_task(anext(stream))
    try:
        for _ in range(3):
            await asyncio.sleep(0)
        assert not waiting.done()

        await client._reset_message_stream()

        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(waiting, 1)
        assert delivery.messages_queue is queue
        assert delivery.message_ready is ready

        fresh = Message(topic="uniform/x", payload=b"fresh")
        await delivery.accept(fresh, None)
        assert await asyncio.wait_for(anext(client.messages()), 1) is fresh
    finally:
        if not waiting.done():
            waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        await stream.aclose()
        await finish(client)
