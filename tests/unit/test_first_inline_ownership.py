from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, PublishReceipt
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory
from tests.unit.test_first_inline_bursts import assert_bound, clean, effects
from tests.unit.test_first_inline_bursts import task_factory as task_factory


# Share only the task-factory fixture; failures inside callbacks must not be
# swallowed by MQTTium's application-error isolation.
@pytest.fixture(autouse=True)
async def no_loop_errors():
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        yield
        await asyncio.sleep(0)
        assert not errors, errors
    finally:
        loop.set_exception_handler(previous)


async def until(predicate) -> None:
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


@pytest.mark.parametrize("count", [2, 3, 8, 32])
@pytest.mark.parametrize("when", ["inside-first", "before-start", "active-tail"])
async def test_cancelled_worker_retires_queued_ownership(task_factory, count, when) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=64)
    seen = []
    worker = None

    def callback(message: Message) -> None:
        nonlocal worker
        seen.append(message.payload)
        worker = client._callback_worker_task
        assert worker is not None
        if (when == "inside-first" and message.payload == b"0") or (
            when == "active-tail" and message.payload == b"1"
        ):
            worker.cancel()
            if when == "active-tail":
                raise asyncio.CancelledError("stop worker")

    client.on_message = callback
    try:
        client._apply_message_effect_batch_inline(effects(count), client._connection_epoch)
        assert worker is not None
        if when == "before-start":
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await asyncio.sleep(0)
        assert worker.cancelled()
        assert seen == ([b"0", b"1"] if when == "active-tail" else [b"0"])
        assert client._callback_queue.empty()
        assert client._delivery._callback_batch_reserved == 0
        assert client._callback_queue.maxsize == 64
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert_bound(client)
        # A later ordinary delivery owns a new worker, not the cancelled one.
        client._delivery.spawn_callback(lambda: seen.append(b"new"))
        replacement = client._callback_worker_task
        assert replacement is not None and replacement is not worker
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen[-1] == b"new"
    finally:
        await clean(client)


async def test_delayed_done_callback_cannot_discard_new_worker_jobs(task_factory) -> None:
    client = AsyncClient(message_delivery="callback")
    seen = []
    try:
        client._delivery.spawn_callback(lambda: seen.append("old"))
        old = client._callback_worker_task
        assert old is not None
        old.cancel()
        await asyncio.gather(old, return_exceptions=True)
        client._delivery.spawn_callback(lambda: seen.append("new"))
        replacement = client._callback_worker_task
        client._delivery._callback_worker_done(old)
        assert client._callback_worker_task is replacement
        await client._callback_queue.join()
        assert seen == ["new"]
    finally:
        await clean(client)


@pytest.mark.parametrize("both", [False, True])
async def test_cancel_before_start_releases_only_callback_byte_reference(
    task_factory, both
) -> None:
    client = AsyncClient(
        message_delivery="both" if both else "callback",
        max_pending_delivery_bytes=4096,
        max_pending_callbacks=8,
    )
    client.on_message = lambda _message: None
    delivery = client._delivery
    message = Message(topic="x", payload=b"x" * 2048)
    try:
        await delivery.accept(message, client.on_message)
        assert delivery.pending_bytes > 0
        worker = delivery.callback_task
        assert worker is not None
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await asyncio.sleep(0)
        assert delivery.callback_queue.empty()
        if both:
            assert delivery.pending_bytes > 0
            assert await anext(client.messages()) is message
        assert delivery.pending_bytes == 0
        await asyncio.wait_for(delivery.callback_queue.join(), 1)
    finally:
        await clean(client)


async def test_waiting_producer_finishes_after_cancelled_worker(task_factory) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=1)
    seen = []
    delivery = client._delivery
    try:
        delivery.spawn_callback(lambda: seen.append("retired"))
        worker = delivery.callback_task
        assert worker is not None
        # Start the slow producer before the worker is allowed to take its job.
        # Its wait_for/Queue.put task may finish after worker retirement.
        producer = asyncio.create_task(delivery.enqueue_callback(lambda: seen.append("next")))
        worker.cancel()
        await asyncio.wait_for(producer, 1)
        await asyncio.gather(worker, return_exceptions=True)
        await asyncio.wait_for(delivery.callback_queue.join(), 1)
        assert seen == ["next"]
        assert_bound(client)
    finally:
        await clean(client)


@pytest.mark.parametrize("scheduled", [False, True])
@pytest.mark.parametrize("kind", [EffectKind.PINGRESP, EffectKind.PUBLISH_COMPLETE])
async def test_cancelled_batch_preserves_later_effect_ownership(
    task_factory, scheduled, kind
) -> None:
    client = AsyncClient(message_delivery="callback")
    pump = client._effect_pump
    seen = []
    receipt = PublishReceipt(41, QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(41, receipt)
    client._ping_pending = True
    original = asyncio.CancelledError("cancel A")

    def callback(message: Message) -> None:
        seen.append(message.payload)
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        raise original

    client.on_message = callback
    pump.pending = effects(3)
    pump.pending.append(EngineEffect(kind, 41 if kind is EffectKind.PUBLISH_COMPLETE else None))
    pump.pending_epoch = client._connection_epoch
    pump.enqueued = len(pump.pending)
    try:
        if scheduled:
            pump.schedule()
            task = pump.task
        else:

            async def inline():
                pump.drain_inline()

            task = asyncio.create_task(inline())
        assert task is not None
        await asyncio.gather(task, return_exceptions=True)
        await until(lambda: not pump.pending and pump.task is None)
        assert task.cancelled()
        assert seen == [b"0"]
        assert pump.applied == pump.enqueued == 4
        if kind is EffectKind.PUBLISH_COMPLETE:
            assert receipt.is_done()
            await receipt.wait()
            assert not client._receipts
        else:
            assert not client._ping_pending
        assert_bound(client)
    finally:
        await clean(client)


@pytest.mark.parametrize("qos", [0, 1])
@pytest.mark.parametrize("count", [1, 2, 3, 8])
async def test_real_reader_cancellation_propagates_and_tears_down(task_factory, qos, count) -> None:
    # Cancellation still targets the actual current task. Generalizing inline
    # execution generalizes the existing singleton/pair ownership; do not use
    # uncancel() to fabricate worker-like semantics on the reader.
    client = AsyncClient(message_delivery="callback", keepalive=0)
    transport = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(transport)
    seen = []
    owners = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        owner = asyncio.current_task()
        owners.append(owner)
        assert not client._engine_lock.locked()
        assert owner is not None
        owner.cancel()
        raise asyncio.CancelledError("cancel reader")

    client.on_message = callback
    try:
        await client.connect("memory-transport", timeout=1)
        reader = client._reader_task
        assert reader is not None
        transport.push_rx(
            b"".join(
                PublishPacket(
                    topic="x",
                    payload=str(i).encode(),
                    qos=QoS(qos),
                    retain=False,
                    dup=False,
                    mid=i + 1 if qos else None,
                ).encode(MQTTProtocolVersion.MQTTv311)
                for i in range(count)
            )
        )
        await asyncio.wait_for(asyncio.gather(reader, return_exceptions=True), 1)
        assert seen == [b"0"]
        assert owners == [reader]
        assert reader.cancelled()
        assert not client.is_connected
        assert not client._effect_pump.pending
        assert client._callback_queue.empty()
        assert client._delivery._callback_batch_reserved == 0
        assert client._delivery.pending_bytes == 0
    finally:
        await asyncio.wait_for(client.disconnect(), 1)
        await clean(client)


async def test_shutdown_cancellation_never_resurrects_a_pending_message(task_factory) -> None:
    client = AsyncClient(message_delivery="callback", keepalive=0)
    transport = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(transport)
    seen = []
    client.on_message = lambda m: seen.append(m.payload)
    try:
        await client.connect("memory-transport", timeout=1)
        pump = client._effect_pump
        pump.pending = effects(8)
        pump.pending_epoch = client._connection_epoch
        pump.enqueued += 8
        # Hold its serialization lock so even an eager flusher cannot deliver.
        await pump.lock.acquire()
        try:
            pump.schedule()
            flush = pump.task
            assert flush is not None
            await asyncio.wait_for(client.disconnect(), 1)
        finally:
            pump.lock.release()
        await asyncio.sleep(0)
        assert seen == []
        assert not client.is_connected
        assert flush.done()
        assert not pump.pending
        assert pump.task is None
        assert pump.applied == pump.enqueued
    finally:
        await client.disconnect()
        await clean(client)


async def test_topic_chain_and_publish_callback_keep_their_existing_scope(task_factory) -> None:
    # Deliberately preserve per-message filter grouping and on_publish. These
    # are NOT a universal one-user-function-per-event-loop-turn budget.
    client = AsyncClient(message_delivery="callback")
    seen = []
    owner = asyncio.current_task()
    client.on_publish = lambda _mid, _reason: seen.append(("publish", asyncio.current_task()))
    for topic in ("burst/#", "burst/+", "burst/x"):
        client.message_callback_add(
            topic, lambda m, topic=topic: seen.append((topic, asyncio.current_task()))
        )
    pump = client._effect_pump
    pump.pending = effects(1)
    pump.pending.appendleft(EngineEffect(EffectKind.PUBLISH_COMPLETE, None))
    pump.pending_epoch = client._connection_epoch
    pump.enqueued = len(pump.pending)
    try:
        pump.drain_inline()
        assert seen == [(label, owner) for label in ("publish", "burst/#", "burst/+", "burst/x")]
        assert pump.enqueued == pump.applied
        assert client._callback_worker_task is None
    finally:
        await clean(client)


@pytest.mark.parametrize("count", [2, 8])
@pytest.mark.parametrize("shutdown", [False, True])
async def test_active_batch_retirement_preserves_newly_awakened_producer(
    task_factory, count, shutdown
) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=count)
    delivery = client._delivery
    entered = asyncio.Event()
    block = asyncio.Event()
    seen = []

    async def active(message: Message) -> None:
        seen.append(message.payload)
        entered.set()
        await block.wait()

    producer = None
    try:
        delivery._enqueue_message_batch(
            active, [effect.data for effect in effects(count)], iterator_delivery=False
        )
        await entered.wait()
        old = delivery.callback_task
        assert old is not None
        delivery.spawn_callback(lambda: seen.append(b"retired"))
        producer = asyncio.create_task(delivery.enqueue_callback(lambda: seen.append(b"new")))
        # Queue.put is genuinely parked behind the active batch reservation.
        for _ in range(5):
            await asyncio.sleep(0)
        assert not producer.done()
        assert delivery.callback_queue.full()
        if shutdown:
            await delivery.shutdown_callbacks(drain=False)
        else:
            old.cancel()
        await asyncio.gather(old, return_exceptions=True)
        await asyncio.wait_for(producer, 1)
        await asyncio.wait_for(delivery.callback_queue.join(), 1)
        assert old.cancelled()
        assert seen == [b"0", b"new"]
        assert delivery.callback_task is not None
        assert delivery.callback_task is not old
        assert not delivery.callback_task.done()
        assert delivery._callback_batch_reserved == 0
        assert_bound(client)
    finally:
        if producer is not None and not producer.done():
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
        await clean(client)
