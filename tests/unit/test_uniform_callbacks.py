"""Uniform message callback ownership, bounded rounds, and lifecycle regressions."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MessageDeliveryError
from mqttium.packets import PubRelPacket, PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory


@pytest.fixture(params=[False, True], ids=["normal", "eager"])
async def scheduler(request):
    loop = asyncio.get_running_loop()
    factory, handler = loop.get_task_factory(), loop.get_exception_handler()
    errors = []
    if request.param:
        if not hasattr(asyncio, "eager_task_factory"):
            pytest.skip("eager tasks require Python 3.12+")
        loop.set_task_factory(asyncio.eager_task_factory)
    loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))
    try:
        yield
        await asyncio.sleep(0)
        assert not errors, errors
    finally:
        loop.set_task_factory(factory)
        loop.set_exception_handler(handler)


def batch(count):
    return deque(
        EngineEffect(
            EffectKind.MESSAGE,
            Message(topic="uniform/x", payload=str(i).encode()),
            requires_delivery_mark=False,
        )
        for i in range(count)
    )


async def finish(client):
    await asyncio.wait_for(client._shutdown_callback_worker(drain=False), 1)
    await asyncio.wait_for(client._callback_queue.join(), 1)
    assert client._callback_queue.empty()
    assert client._callback_worker_task is None
    assert not client._delivery._callback_active


def bound(client, limit):
    q = client._callback_queue
    assert client.stats().delivery.callback_queued == q.qsize()
    assert q.maxsize == client.stats().delivery.callback_limit == limit
    assert 0 <= q.qsize() <= limit
    assert not hasattr(client._delivery, "_callback_batch_reserved")


@pytest.mark.parametrize("count", [1, 2, 3, 8, 32])
@pytest.mark.parametrize("mode", ["callback", "auto", "both"])
@pytest.mark.parametrize("direct", [False, True])
async def test_all_message_callbacks_are_worker_owned(scheduler, count, mode, direct):
    client = AsyncClient(message_delivery=mode, max_pending_callbacks=64, max_pending_messages=64)
    seen, owners = [], []
    caller = asyncio.current_task()

    def callback(m):
        seen.append(m.payload)
        owners.append(asyncio.current_task())
        bound(client, 64)

    client.on_message = callback
    try:
        items = batch(count)
        if direct and mode != "both":
            assert client._delivery.deliver_callback_messages_inline(
                [e.data for e in items], callback
            )
        else:
            assert (
                client._apply_message_effect_batch_inline(items, client._connection_epoch) == count
            )
        assert seen == []
        bound(client, 64)
        assert client._callback_queue.qsize() == count
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen == [str(i).encode() for i in range(count)]
        assert all(t is client._callback_worker_task and t is not caller for t in owners)
        if mode == "both":
            assert client._messages.qsize() == count
    finally:
        await finish(client)


@pytest.mark.parametrize("limit", [1, 2, 8])
async def test_reentrant_notifications_cannot_extend_the_current_round(scheduler, limit):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=limit)
    order = []
    loop = asyncio.get_running_loop()

    def first(m):
        order.append(m.payload)
        if m.payload == b"0":
            assert client._delivery.try_enqueue_callback(lambda: order.append(b"X"))
            loop.call_soon(order.append, b"yield")
        bound(client, limit)

    client.on_message = first
    try:
        assert (
            client._apply_message_effect_batch_inline(batch(limit + 1), client._connection_epoch)
            == limit
        )
        assert client._callback_queue.full()
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert order == [str(i).encode() for i in range(limit)] + [b"yield", b"X"]
    finally:
        await finish(client)


@pytest.mark.parametrize(
    "action", ["error", "self-error", "cancel-return", "cancel-raise", "replace"]
)
async def test_error_cancellation_and_capture_are_per_notification(scheduler, action):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=3)
    seen, errors, tasks = [], [], []
    client._delivery.report_callback_error = lambda cb, exc: errors.append(exc)

    def old(m):
        seen.append(m.payload)
        if m.payload != b"0":
            return
        tasks.append(asyncio.current_task())
        client.on_message = lambda m: seen.append(b"new:" + m.payload)
        if action == "error":
            raise ValueError("isolated")
        if action == "self-error":
            raise asyncio.CancelledError("isolated")
        if action.startswith("cancel"):
            tasks[0].cancel()
            if action == "cancel-raise":
                raise asyncio.CancelledError("interrupted")

    client.on_message = old
    try:
        client._apply_message_effect_batch_inline(batch(3), client._connection_epoch)
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen == [b"0", b"1", b"2"]
        assert len(errors) == (action in ("error", "self-error"))
        if action.startswith("cancel"):
            assert tasks[0].cancelled()
            assert client._callback_worker_task is not tasks[0]
        bound(client, 3)
    finally:
        await finish(client)


@pytest.mark.parametrize("when", ["before-entry", "active", "between-rounds"])
async def test_cancelled_worker_leaves_unstarted_jobs_with_controller(scheduler, when):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []

    async def first():
        seen.append("active")
        entered.set()
        await release.wait()

    try:
        d = client._delivery
        d.spawn_callback(first)
        old = d.callback_task
        if when != "before-entry":
            await entered.wait()
        d.spawn_callback(lambda: seen.append("pending"))
        if when == "between-rounds":
            asyncio.get_running_loop().call_soon(old.cancel)
            release.set()
        else:
            old.cancel()
        await asyncio.gather(old, return_exceptions=True)
        release.set()
        await asyncio.wait_for(d.callback_queue.join(), 1)
        assert seen == ["active", "pending"]
        assert old.cancelled()
        d._callback_worker_done(old)
        d.spawn_callback(lambda: seen.append("next"))
        await d.callback_queue.join()
        assert seen[-1] == "next"
    finally:
        release.set()
        await finish(client)


@pytest.mark.parametrize("drain", [False, True])
async def test_shutdown_refuses_waiters_and_reopen_preserves_only_new_jobs(scheduler, drain):
    client = AsyncClient(
        message_delivery="callback", max_pending_callbacks=1, callback_shutdown_timeout=0.01
    )
    d = client._delivery
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []

    async def active():
        entered.set()
        await release.wait()

    producer = None
    try:
        d.spawn_callback(active)
        await entered.wait()
        d.spawn_callback(lambda: seen.append("old"))
        producer = asyncio.create_task(d.enqueue_callback(lambda: seen.append("stale")))
        for _ in range(3):
            await asyncio.sleep(0)
        assert not producer.done()
        await d.shutdown_callbacks(drain=drain)
        with pytest.raises(MessageDeliveryError):
            await producer
        d.reopen()
        d.spawn_callback(lambda: seen.append("new"))
        await d.callback_queue.join()
        assert seen == ["new"]
    finally:
        release.set()
        if producer is not None:
            await asyncio.gather(producer, return_exceptions=True)
        await finish(client)


async def test_worker_cannot_wait_for_its_own_full_queue(scheduler):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=1)
    seen = []

    async def active():
        client._delivery.spawn_callback(lambda: seen.append("tail"))
        with pytest.raises(MessageDeliveryError, match="own full queue"):
            await client._delivery.enqueue_callback(lambda: seen.append("impossible"))
        seen.append("active")

    try:
        client._delivery.spawn_callback(active)
        await asyncio.wait_for(client._callback_queue.join(), 1)
        assert seen == ["active", "tail"]
    finally:
        await finish(client)


@pytest.mark.parametrize("mode", ["callback", "both"])
async def test_shutdown_wakes_byte_admission_without_stealing_iterator_bytes(scheduler, mode):
    client = AsyncClient(
        message_delivery=mode, max_pending_delivery_bytes=4096, max_pending_callbacks=2
    )
    d = client._delivery
    entered = asyncio.Event()

    async def callback(m):
        entered.set()
        await asyncio.Event().wait()

    producer = None
    client.on_message = callback
    try:
        first = Message(topic="x", payload=b"x" * 3000)
        await d.accept(first, callback)
        await entered.wait()
        producer = asyncio.create_task(d.accept(first, callback))
        for _ in range(3):
            await asyncio.sleep(0)
        assert d.waiters == 1
        await d.shutdown_callbacks(drain=False)
        with pytest.raises(MessageDeliveryError):
            await asyncio.wait_for(producer, 1)
        assert d.waiters == 0
        if mode == "both":
            assert d.pending_bytes == 3001
            assert await anext(client.messages()) is first
        assert d.pending_bytes == 0
    finally:
        if producer:
            await asyncio.gather(producer, return_exceptions=True)
        await finish(client)


async def test_routed_burst_can_switch_sync_async_and_remove_last_filter(scheduler):
    client = AsyncClient(message_delivery="callback")
    seen = []

    async def new(m):
        await asyncio.sleep(0)
        seen.append(("new", m.payload))
        client.message_callback_remove("uniform/#")

    def old(m):
        seen.append(("old", m.payload))
        client.message_callback_add("uniform/#", new)

    client.on_message = lambda m: seen.append(("default", m.payload))
    client.message_callback_add("uniform/#", old)
    try:
        client._apply_message_effect_batch_inline(batch(3), client._connection_epoch)
        await client._callback_queue.join()
        assert seen == [("old", b"0"), ("new", b"1"), ("default", b"2")]
    finally:
        await finish(client)


class QoS2Broker(ScriptedBrokerTransport):
    def handle_packet(self, raw):
        if raw.packet_type is PacketType.PUBREC:
            self.push_rx(
                PubRelPacket(mid=int.from_bytes(raw.remaining[:2], "big")).encode(self.protocol)
            )
        else:
            super().handle_packet(raw)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [0, 1, 2])
@pytest.mark.parametrize("mode", ["callback", "both"])
@pytest.mark.parametrize("action", ["reply", "cancel", "reconnect"])
async def test_real_reader_callback_lifecycle(scheduler, protocol, qos, mode, action):
    client = AsyncClient(
        "uniform-lifecycle",
        message_delivery=mode,
        protocol=protocol,
        keepalive=0,
        max_pending_callbacks=8,
    )
    transports = [QoS2Broker(protocol=protocol), QoS2Broker(protocol=protocol)]
    client._transport_factory = transport_factory(transports[0])
    seen, owners, receipts = [], [], []
    complete = asyncio.Event()

    async def callback(m):
        seen.append(m.payload)
        owners.append(asyncio.current_task())
        assert not client._engine_lock.locked()
        if m.payload == b"0":
            if action == "reply":
                receipt = client.publish_nowait("reply/x", b"reply", qos=1)
                receipts.append(receipt)
                await asyncio.wait_for(receipt.wait(), 1)
            elif action == "cancel":
                owners[-1].cancel()
                raise asyncio.CancelledError
            else:
                await client.disconnect()
                client._transport_factory = transport_factory(transports[1])
                await client.connect("memory", timeout=1)
                transports[1].push_rx(
                    PublishPacket(
                        topic="uniform/x",
                        payload=b"new",
                        qos=QoS(qos),
                        retain=False,
                        dup=False,
                        mid=51 if qos else None,
                    ).encode(protocol)
                )
        if m.payload in (b"2", b"new"):
            complete.set()

    client.on_message = callback
    try:
        await client.connect("memory", timeout=1)
        reader = client._reader_task
        transports[0].push_rx(
            b"".join(
                PublishPacket(
                    topic="uniform/x",
                    payload=str(i).encode(),
                    qos=QoS(qos),
                    retain=False,
                    dup=False,
                    mid=i + 1 if qos else None,
                ).encode(protocol)
                for i in range(3)
            )
        )
        await asyncio.wait_for(complete.wait(), 2)
        await client._callback_queue.join()
        assert all(owner is not reader for owner in owners)
        assert client.is_connected
        if action == "reconnect":
            assert seen == [b"0", b"new"]
            assert owners[0] is owners[-1]
        else:
            assert seen == [b"0", b"1", b"2"]
        for receipt in receipts:
            assert receipt.is_done()
    finally:
        await asyncio.wait_for(client.disconnect(), 2)
        # Iterator copies remain owned until consumed/reset, not callback cleanup.
        while not client._messages.empty():
            item = client._messages.get_nowait()
            if isinstance(item, tuple):
                client._delivery.release_nowait(item[1])
        assert client._delivery.pending_bytes == 0
        await finish(client)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [1, 2])
@pytest.mark.parametrize("manual", [False, True])
@pytest.mark.parametrize("sqlite", [False, True])
async def test_persisted_delivery_ack_and_byte_accounting(
    scheduler, tmp_path, protocol, qos, manual, sqlite
):
    from mqttium.persistence import MemoryInflightStore, SqliteInflightStore

    store = SqliteInflightStore(tmp_path / "inflight.sqlite") if sqlite else MemoryInflightStore()
    client = AsyncClient(
        "uniform-persistence",
        store=store,
        protocol=protocol,
        manual_ack=manual,
        message_delivery="both",
        keepalive=0,
        max_pending_delivery_bytes=16384,
    )
    transport = QoS2Broker(protocol=protocol)
    client._transport_factory = transport_factory(transport)
    seen = []
    done = asyncio.Event()

    async def callback(m):
        assert not client._engine_lock.locked()
        seen.append(m.mid)
        if manual:
            await client.ack(m)
        if len(seen) == 3:
            done.set()

    client.on_message = callback
    try:
        await client.connect("memory", timeout=1)
        transport.push_rx(
            b"".join(
                PublishPacket(
                    topic="uniform/x",
                    payload=b"x" * 2048,
                    qos=QoS(qos),
                    retain=False,
                    dup=False,
                    mid=i + 1,
                ).encode(protocol)
                for i in range(3)
            )
        )
        await asyncio.wait_for(done.wait(), 2)
        await client._callback_queue.join()
        assert seen == [1, 2, 3]
        assert client._delivery.pending_bytes > 0
        iterator = client.messages()
        for mid in (1, 2, 3):
            assert (await anext(iterator)).mid == mid
        await iterator.aclose()
        assert client._delivery.pending_bytes == 0
        assert client.stats().delivery.callback_queued == 0
        # Decode actual wire acknowledgements rather than inferring from callbacks.
        from mqttium.codec.buffer import IncrementalDecoder

        decoder = IncrementalDecoder()
        for _ in range(5):
            await asyncio.sleep(0)
        decoder.feed(b"".join(transport.written))
        kinds = [p.packet_type for p in decoder.drain_packets()]
        assert kinds.count(PacketType.PUBACK if qos == 1 else PacketType.PUBCOMP) == 3
    finally:
        await client.disconnect()
        await finish(client)
        if sqlite:
            store.close()
