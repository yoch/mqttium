"""Final adverse checks for uniform delivery ownership."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory


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


async def test_qos1_reply_progresses_while_message_callback_queue_is_saturated(scheduler) -> None:
    client = AsyncClient(
        "uniform-saturated-reply",
        message_delivery="callback",
        protocol=MQTTProtocolVersion.MQTTv311,
        keepalive=0,
        max_pending_callbacks=1,
    )
    transport = ScriptedBrokerTransport(protocol=MQTTProtocolVersion.MQTTv311)
    client._transport_factory = transport_factory(transport)
    seen: list[bytes] = []
    reply_done = asyncio.Event()
    all_done = asyncio.Event()

    async def callback(message: Message) -> None:
        seen.append(message.payload)
        if message.payload == b"0":
            # Allow delivery admission to occupy the only waiting slot before
            # this callback waits for a QoS1 PUBACK. Network receive must still
            # progress while later message admission is backpressured.
            for _ in range(3):
                await asyncio.sleep(0)
            assert client._callback_queue.full()
            receipt = client.publish_nowait("uniform/reply", b"reply", qos=1)
            await asyncio.wait_for(receipt.wait(), 1)
            assert receipt.is_done()
            reply_done.set()
        if message.payload == b"2":
            all_done.set()

    client.on_message = callback
    try:
        await client.connect("memory", timeout=1)
        transport.push_rx(
            b"".join(
                PublishPacket(
                    topic="uniform/x",
                    payload=str(i).encode(),
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    dup=False,
                    mid=i + 1,
                ).encode(MQTTProtocolVersion.MQTTv311)
                for i in range(3)
            )
        )
        await asyncio.wait_for(reply_done.wait(), 2)
        await asyncio.wait_for(all_done.wait(), 2)
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen == [b"0", b"1", b"2"]
        assert client.is_connected
    finally:
        await asyncio.wait_for(client.disconnect(), 2)
        await finish(client)
