"""Callback ownership across terminal shutdown and replacement connections."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, wait_until


@pytest.fixture(params=["normal", "eager"])
async def task_factory(request):
    if request.param == "eager" and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager task factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if request.param == "eager":
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield
    finally:
        loop.set_task_factory(previous)


class _InboundBroker(ScriptedBrokerTransport):
    def __init__(self, protocol):
        super().__init__(protocol=protocol)
        self.acks = []

    def handle_packet(self, raw):
        self.acks.append(raw.packet_type)
        if raw.packet_type is PacketType.PUBREC:
            self.push_rx(encode_frame(PacketType.PUBREL, 2, raw.remaining[:2]))
        super().handle_packet(raw)

    def publish(self, payload, qos=0):
        self.push_rx(
            PublishPacket(
                topic="t",
                payload=payload,
                qos=QoS(qos),
                retain=False,
                dup=False,
                mid=9 if qos else None,
            ).encode(self.protocol)
        )


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [0, 1, 2])
@pytest.mark.parametrize("prefix_jobs", [0, 63])
async def test_callback_reopen_retires_old_jobs_but_delivers_replacement(
    task_factory, protocol, qos, prefix_jobs
):
    client = AsyncClient("callback-reopen", message_delivery="callback", protocol=protocol)
    brokers = [_InboundBroker(protocol), _InboundBroker(protocol)]
    calls = 0
    started, proceed, reopened, release = (asyncio.Event() for _ in range(4))
    seen, accounting = [], []

    async def factory(*args, **kwargs):
        nonlocal calls
        broker = brokers[calls]
        calls += 1
        return broker

    async def callback(message):
        seen.append(message.payload)
        if message.payload == b"old":
            worker = asyncio.current_task()
            started.set()
            await proceed.wait()
            await client.disconnect()
            await client.connect("fake")
            accounting.append(client.stats().delivery.pending_bytes)
            assert client._delivery.callback_task is worker
            reopened.set()
            await release.wait()

    client._transport_factory = factory
    client.on_message = callback
    try:
        await client.connect("fake")
        for _ in range(prefix_jobs):
            await client._delivery.enqueue_callback(lambda: None)
        await client._delivery.callback_queue.join()
        brokers[0].publish(b"old")
        await asyncio.wait_for(started.wait(), 1)
        await client._delivery.accept(Message(topic="t", payload=b"stale"), callback)
        await client._delivery.enqueue_callback(lambda: seen.append(b"old-notification"))
        proceed.set()
        await asyncio.wait_for(reopened.wait(), 1)
        assert accounting == [4], "the active old job still owns its bytes"
        brokers[1].publish(b"new", qos)
        if qos:
            ack = PacketType.PUBACK if qos == 1 else PacketType.PUBCOMP
            await wait_until(lambda: ack in brokers[1].acks)
        await wait_until(lambda: client._delivery.callback_queue.qsize() == 1)
        release.set()
        await asyncio.wait_for(client._delivery.callback_queue.join(), 1)
        assert seen == [b"old", b"new"]
        assert client.is_connected
        assert client.stats().delivery.pending_bytes == 0
    finally:
        proceed.set()
        release.set()
        await client.disconnect()


@pytest.mark.parametrize("failed_replacement", [False, True])
async def test_callback_terminal_stop_releases_queued_work(task_factory, failed_replacement):
    client = AsyncClient(message_delivery="callback")
    brokers = [_InboundBroker(MQTTProtocolVersion.MQTTv311)]
    calls = 0
    started, proceed, finished = (asyncio.Event() for _ in range(3))
    seen = []

    async def factory(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("replacement failed")
        return brokers[0]

    async def callback(message):
        seen.append(message.payload)
        started.set()
        await proceed.wait()
        await client.disconnect()
        if failed_replacement:
            with pytest.raises(OSError, match="replacement failed"):
                await client.connect("fake")
        finished.set()

    client.on_message = callback
    client._transport_factory = factory
    try:
        await client.connect("fake")
        brokers[0].publish(b"old")
        await asyncio.wait_for(started.wait(), 1)
        await client._delivery.accept(Message(topic="t", payload=b"stale"), callback)
        proceed.set()
        await asyncio.wait_for(finished.wait(), 1)
        await asyncio.wait_for(client._delivery.callback_queue.join(), 1)
        assert seen == [b"old"]
        assert not client.is_connected
        assert client.stats().delivery.pending_bytes == 0
        await wait_until(lambda: not client.stats().tasks.callback_worker)
    finally:
        proceed.set()
        await client.disconnect()


@pytest.mark.parametrize(
    "kind", ["future", "raised", "runtime", "owner", "connect", "disconnect", "disabled"]
)
async def test_disconnect_notification_cancellation_keeps_lifecycle_owner(task_factory, kind):
    client = AsyncClient(
        message_delivery="callback",
        reconnect=ReconnectPolicy(
            enabled=kind != "disabled", initial_delay=0, max_delay=0, stable_after=0
        ),
    )
    brokers = [_InboundBroker(MQTTProtocolVersion.MQTTv311) for _ in range(2)]
    calls, notifications = 0, 0
    entered = asyncio.Event()
    reports = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: reports.append(context))

    async def factory(*args, **kwargs):
        nonlocal calls
        broker = brokers[calls]
        calls += 1
        return broker

    async def on_disconnect(_error):
        nonlocal notifications
        notifications += 1
        if notifications > 1:
            return
        entered.set()
        if kind == "owner":
            await asyncio.Event().wait()
        if kind == "connect":
            await client.connect("fake")
        elif kind == "disconnect":
            await client.disconnect()
        if kind in {"future", "disabled"}:
            cancelled = loop.create_future()
            cancelled.cancel()
            await cancelled
        if kind == "runtime":
            raise RuntimeError("notification failed")
        raise asyncio.CancelledError("notification cancelled")

    client._transport_factory = factory
    client.on_disconnect = on_disconnect
    try:
        await client.connect("fake")
        client._delivery.ensure_callback_worker()
        reader = client._reader_task
        assert reader is not None
        await brokers[0].close()
        await asyncio.wait_for(entered.wait(), 1)
        if kind == "owner":
            reader.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reader
            assert not reports
            assert calls == 1
        else:
            await asyncio.wait_for(reader, 1)
            assert len(reports) == 1
            expected = RuntimeError if kind == "runtime" else asyncio.CancelledError
            assert isinstance(reports[0]["exception"], expected)
            if kind in {"disconnect", "disabled"}:
                assert not client.is_connected
                await wait_until(lambda: not client.stats().tasks.callback_worker)
                assert calls == 1
            else:
                await wait_until(lambda: calls == 2 and client.is_connected)
                replacement = client._reader_task
                await asyncio.sleep(0)
                assert client._reader_task is replacement
                assert replacement is not None and not replacement.done()
        assert client._reconnect_task is None or kind not in {
            "owner",
            "connect",
            "disconnect",
            "disabled",
        }
    finally:
        await client.disconnect()
        loop.set_exception_handler(previous)


async def test_automatic_reconnect_preserves_queued_callback_delivery(task_factory):
    client = AsyncClient(
        message_delivery="callback",
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=0),
    )
    brokers = [_InboundBroker(MQTTProtocolVersion.MQTTv311) for _ in range(2)]
    calls = 0
    started, release = asyncio.Event(), asyncio.Event()
    seen = []

    async def factory(*args, **kwargs):
        nonlocal calls
        broker = brokers[calls]
        calls += 1
        return broker

    async def callback(message):
        seen.append(message.payload)
        if message.payload == b"old":
            started.set()
            await release.wait()

    client._transport_factory = factory
    client.on_message = callback
    try:
        await client.connect("fake")
        brokers[0].publish(b"old")
        await asyncio.wait_for(started.wait(), 1)
        await client._delivery.accept(Message(topic="t", payload=b"queued"), callback)
        await brokers[0].close()
        await wait_until(lambda: calls == 2 and client.is_connected)
        assert client.stats().delivery.pending_bytes == 11
        release.set()
        await asyncio.wait_for(client._delivery.callback_queue.join(), 1)
        assert seen == [b"old", b"queued"]
        assert client.stats().delivery.pending_bytes == 0
    finally:
        release.set()
        await client.disconnect()
