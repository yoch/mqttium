from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory


def effects(count: int, kind: EffectKind = EffectKind.MESSAGE) -> deque[EngineEffect]:
    return deque(
        EngineEffect(
            kind,
            Message(topic="burst/x", payload=str(i).encode()),
            requires_delivery_mark=False,
            decoded_property_wire_size=0 if kind is EffectKind.DECODED_MESSAGE else None,
        )
        for i in range(count)
    )


@pytest.fixture(autouse=True)
async def reject_unexpected_callback_errors():  # type: ignore[no-untyped-def]
    # Assertions inside user callbacks are isolated by the library, so also
    # fail the test if an unexpected callback assertion was reported to asyncio.
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    reported = []
    loop.set_exception_handler(lambda loop, context: reported.append(context))
    try:
        yield
        await asyncio.sleep(0)
        assert not reported, reported
    finally:
        loop.set_exception_handler(previous)


@pytest.fixture(params=[False, True], ids=["normal", "eager"])
async def task_factory(request):  # type: ignore[no-untyped-def]
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if request.param:
        if not hasattr(asyncio, "eager_task_factory"):
            pytest.skip("eager tasks require Python 3.12+")
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield bool(request.param)
    finally:
        loop.set_task_factory(previous)


def assert_bound(client: AsyncClient) -> None:
    delivery = client._delivery
    stats = client.stats().delivery
    assert 0 <= stats.callback_queued <= stats.callback_limit
    assert (
        stats.callback_queued == delivery.callback_queue.qsize() + delivery._callback_batch_reserved
    )
    assert (
        delivery.callback_queue.maxsize + delivery._callback_batch_reserved == stats.callback_limit
    )
    assert delivery.callback_queue.maxsize > 0


async def clean(client: AsyncClient) -> None:
    await asyncio.wait_for(client._shutdown_callback_worker(drain=False), 1)
    assert client._callback_queue.empty()
    assert client._delivery._callback_batch_reserved == 0
    assert not client._delivery._callback_active
    assert client._callback_queue.maxsize == client.stats().delivery.callback_limit
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert_bound(client)


@pytest.mark.parametrize("count", [1, 2, 3, 8, 32])
@pytest.mark.parametrize("path", ["message", "decoded", "direct"])
async def test_first_sync_only_and_tail_worker(task_factory, count, path) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=64)
    caller = asyncio.current_task()
    seen = []

    def callback(message: Message) -> None:
        seen.append((message.payload, asyncio.current_task() is caller))
        assert_bound(client)

    client.on_message = callback
    try:
        batch = effects(
            count, EffectKind.DECODED_MESSAGE if path == "decoded" else EffectKind.MESSAGE
        )
        if path == "direct":
            assert client._delivery.deliver_callback_messages_inline(
                [e.data for e in batch], callback
            )
        else:
            assert (
                client._apply_message_effect_batch_inline(batch, client._connection_epoch) == count
            )
        assert seen == [(b"0", True)]
        assert client.stats().delivery.callback_queued == count - 1
        if count == 1:
            assert client._callback_worker_task is None
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen == [(str(i).encode(), i == 0) for i in range(count)]
    finally:
        await clean(client)


@pytest.mark.parametrize("count", [2, 3, 8, 32])
@pytest.mark.parametrize("path", ["message", "direct"])
@pytest.mark.parametrize(
    "action", ["none", "exception", "self-cancel", "cancel-return", "cancel-raise", "replace"]
)
async def test_errors_cancellation_reentrance_and_capture(
    task_factory, count, path, action
) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=count)
    seen = []
    errors = []
    client._delivery.report_callback_error = lambda callback, exc: errors.append(exc)

    def replacement(message: Message) -> None:
        seen.append(("new", message.payload))

    def callback(message: Message) -> None:
        seen.append(("old", message.payload))
        assert_bound(client)
        if message.payload != b"0":
            return
        assert client.stats().delivery.callback_queued == count - 1
        if action == "replace":
            client.on_message = replacement
        pending = client._accept_message(
            Message(topic="burst/x", payload=b"X"), client._message_callback
        )
        assert pending is None
        assert client.stats().delivery.callback_queued == count
        assert not client._delivery.try_enqueue_callback(lambda: None)
        if action == "exception":
            raise ValueError("isolated")
        if action == "self-cancel":
            raise asyncio.CancelledError("self")
        if action.startswith("cancel-"):
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            if action == "cancel-raise":
                raise asyncio.CancelledError("interrupted")

    async def run() -> None:
        client.on_message = callback
        batch = effects(count)
        if path == "direct":
            assert client._delivery.deliver_callback_messages_inline(
                [e.data for e in batch], callback
            )
        else:
            assert (
                client._apply_message_effect_batch_inline(batch, client._connection_epoch) == count
            )

    try:
        task = asyncio.create_task(run())
        if action.startswith("cancel-"):
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
        await asyncio.wait_for(client._callback_queue.join(), 1)
        expected = (
            [("old", b"0")]
            if action == "cancel-raise"
            else [("old", str(i).encode()) for i in range(count)]
        )
        expected.append(("new" if action == "replace" else "old", b"X"))
        assert seen == expected
        assert len(errors) == int(action in ("exception", "self-cancel"))
    finally:
        await clean(client)


@pytest.mark.parametrize("limit", [1, 2, 7])
async def test_exact_tail_capacity_and_oversize_fallback(task_factory, limit) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=limit)
    seen = []
    refused = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        if message.payload == b"0":
            refused.append(not client._delivery.try_enqueue_callback(lambda: None))
        assert_bound(client)

    client.on_message = callback
    try:
        # Exactly one active callback plus a full, counted tail.
        assert (
            client._apply_message_effect_batch_inline(effects(limit + 1), client._connection_epoch)
            == limit + 1
        )
        assert seen == [b"0"]
        assert refused == [True]
        assert client.stats().delivery.callback_queued == limit
        await client._callback_queue.join()
        await clean(client)
        seen.clear()
        refused.clear()
        # A tail too large to pre-admit cannot take the inline fast path.
        assert (
            client._apply_message_effect_batch_inline(effects(limit + 2), client._connection_epoch)
            == limit
        )
        assert seen == []
        assert client.stats().delivery.callback_queued == limit
        await client._callback_queue.join()
    finally:
        await clean(client)


@pytest.mark.parametrize("mode", ["callback", "iterator", "both"])
@pytest.mark.parametrize("count", [1, 2, 8])
async def test_async_and_delivery_mode_controls(task_factory, mode, count) -> None:
    client = AsyncClient(message_delivery=mode, max_pending_messages=64, max_pending_callbacks=64)
    seen = []

    async def callback(message: Message) -> None:
        await asyncio.sleep(0)
        seen.append(message.payload)

    client.on_message = callback
    try:
        assert (
            client._apply_message_effect_batch_inline(effects(count), client._connection_epoch)
            == count
        )
        assert seen == []
        assert client._messages.qsize() == (count if mode in ("iterator", "both") else 0)
        await client._callback_queue.join()
        assert seen == ([] if mode == "iterator" else [str(i).encode() for i in range(count)])
    finally:
        await clean(client)


@pytest.mark.parametrize("scheduled", [False, True])
async def test_one_message_inline_across_separated_prefixes(task_factory, scheduled) -> None:
    client = AsyncClient(message_delivery="callback")
    pump = client._effect_pump
    seen = []

    def callback(message: Message) -> None:
        seen.append((message.payload, asyncio.current_task() is client._callback_worker_task))

    client.on_message = callback
    first, second, third = list(effects(3))
    pump.pending = deque(
        [first, EngineEffect(EffectKind.PINGRESP), second, EngineEffect(EffectKind.PINGRESP), third]
    )
    pump.enqueued = len(pump.pending)
    try:
        if scheduled:
            await pump._run_scheduled()
        else:
            pump.drain_inline()
        await client._callback_queue.join()
        assert seen == [(b"0", False), (b"1", True), (b"2", True)]
        assert pump.enqueued == pump.applied
    finally:
        await clean(client)


@pytest.mark.parametrize("count", [1, 2, 8])
@pytest.mark.parametrize("scheduled", [False, True])
async def test_cancelled_effect_prefix_never_replayed(task_factory, count, scheduled) -> None:
    client = AsyncClient(message_delivery="callback")
    pump = client._effect_pump
    seen = []
    original = asyncio.CancelledError("original cancellation")

    def callback(message: Message) -> None:
        seen.append(message.payload)
        if message.payload == b"0" and seen.count(b"0") == 1:
            # A reentrant request must not resurrect the abandoned prefix.
            pump.schedule()
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            raise original

    client.on_message = callback
    pump.pending = effects(count)
    pump.enqueued = count

    async def run() -> None:
        try:
            if scheduled:
                await pump._run_scheduled()
            else:
                pump.drain_inline()
        except asyncio.CancelledError as exc:
            assert exc is original
            raise

    try:
        task = asyncio.create_task(run())
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(5):
            await asyncio.sleep(0)
        assert seen == [b"0"]
        assert not pump.pending
        assert pump.enqueued == pump.applied == count
    finally:
        if pump.task is not None:
            await asyncio.gather(pump.task, return_exceptions=True)
        await clean(client)


@pytest.mark.parametrize("drain", [False, True])
@pytest.mark.parametrize("cancel_worker", [False, True])
async def test_shutdown_requested_from_first_callback(task_factory, drain, cancel_worker) -> None:
    client = AsyncClient(message_delivery="callback", callback_shutdown_timeout=0.01)
    seen = []
    tasks = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        if message.payload == b"0":
            if cancel_worker:
                assert client._callback_worker_task is not None
                client._callback_worker_task.cancel()
            tasks.append(asyncio.create_task(client._shutdown_callback_worker(drain=drain)))

    client.on_message = callback
    try:
        client._apply_message_effect_batch_inline(effects(8), client._connection_epoch)
        assert seen == [b"0"]
        await asyncio.wait_for(asyncio.gather(*tasks), 1)
        assert seen in ([b"0"], [str(i).encode() for i in range(8)])
    finally:
        await clean(client)


@pytest.mark.parametrize("mixed", [False, True])
async def test_topic_router_keeps_registration_order_and_per_message_snapshot(
    task_factory, mixed
) -> None:
    client = AsyncClient(message_delivery="callback")
    seen = []

    def replacement(message: Message) -> None:
        seen.append(("new", message.payload))

    def first(message: Message) -> None:
        seen.append(("first", message.payload))
        if message.payload == b"0":
            client.message_callback_add("burst/+", replacement)

    def second(message: Message) -> None:
        seen.append(("second", message.payload))

    async def second_async(message: Message) -> None:
        await asyncio.sleep(0)
        second(message)

    client.message_callback_add("burst/#", first)
    client.message_callback_add("burst/+", second_async if mixed else second)
    try:
        assert client._apply_message_effect_batch_inline(effects(3), client._connection_epoch) == 3
        assert seen == ([] if mixed else [("first", b"0"), ("second", b"0")])
        await client._callback_queue.join()
        assert seen == [
            ("first", b"0"),
            ("second", b"0"),
            ("first", b"1"),
            ("new", b"1"),
            ("first", b"2"),
            ("new", b"2"),
        ]
    finally:
        await clean(client)


@pytest.mark.parametrize("qos", [0, 1])
@pytest.mark.parametrize("count", [1, 2, 8])
@pytest.mark.parametrize("action", ["none", "publish", "disconnect", "cancel"])
async def test_real_reader_boundaries_and_lifecycle(task_factory, qos, count, action) -> None:
    client = AsyncClient(message_delivery="callback", keepalive=0)
    transport = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(transport)
    seen = []
    lock_states = []
    owners = []
    done = asyncio.Event()
    tasks = []
    receipts = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        lock_states.append(client._engine_lock.locked())
        owners.append(asyncio.current_task() is client._callback_worker_task)
        if message.payload == b"0":
            if action == "publish":
                receipts.append(client.publish_nowait("reply/x", b"reply", qos=QoS.AT_LEAST_ONCE))
            elif action == "disconnect":
                tasks.append(asyncio.create_task(client.disconnect()))
            elif action == "cancel":
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                raise asyncio.CancelledError
        if len(seen) == count:
            done.set()

    client.on_message = callback
    try:
        await client.connect("in-process", timeout=1)
        reader = client._reader_task
        wire = b"".join(
            PublishPacket(
                topic="burst/x",
                payload=str(i).encode(),
                qos=QoS(qos),
                retain=False,
                dup=False,
                mid=i + 1 if qos else None,
            ).encode(MQTTProtocolVersion.MQTTv311)
            for i in range(count)
        )
        transport.push_rx(wire)
        if action == "cancel":
            assert reader is not None
            await asyncio.wait_for(asyncio.gather(reader, return_exceptions=True), 1)
            assert seen == [b"0"]
        elif action == "disconnect":
            for _ in range(100):
                if tasks:
                    break
                await asyncio.sleep(0)
            assert tasks
            await asyncio.wait_for(asyncio.gather(*tasks), 1)
            assert seen in ([b"0"], [str(i).encode() for i in range(count)])
        else:
            await asyncio.wait_for(done.wait(), 1)
            assert seen == [str(i).encode() for i in range(count)]
            assert owners == [False] + [True] * (count - 1)
            for receipt in receipts:
                await asyncio.wait_for(receipt.wait(), 1)
        assert not any(lock_states)
    finally:
        await asyncio.wait_for(client.disconnect(), 1)
        await clean(client)


async def test_both_capacity_one_retains_its_existing_inline_fallback(task_factory) -> None:
    # This historical both-mode corner is deliberately outside the new policy.
    client = AsyncClient(message_delivery="both", max_pending_callbacks=1, max_pending_messages=8)
    seen = []
    client.on_message = lambda message: seen.append(message.payload)
    try:
        assert client._apply_message_effect_batch_inline(effects(8), client._connection_epoch) == 8
        assert seen == [str(i).encode() for i in range(8)]
        assert client._messages.qsize() == 8
        assert client._callback_worker_task is None
    finally:
        await clean(client)


@pytest.mark.parametrize("count", [2, 8])
async def test_cancellation_in_tail_worker_releases_batch(task_factory, count) -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=count)
    seen = []

    def callback(message: Message) -> None:
        seen.append(message.payload)
        if message.payload == b"1":
            task = asyncio.current_task()
            assert task is client._callback_worker_task
            task.cancel()
            raise asyncio.CancelledError

    client.on_message = callback
    try:
        assert (
            client._apply_message_effect_batch_inline(effects(count), client._connection_epoch)
            == count
        )
        assert seen == [b"0"]
        worker = client._callback_worker_task
        assert worker is not None
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert seen == [b"0", b"1"]
        assert client.stats().delivery.callback_queued == 0
        assert client._delivery._callback_batch_reserved == 0
        await asyncio.wait_for(client._callback_queue.join(), 1)
    finally:
        await clean(client)
