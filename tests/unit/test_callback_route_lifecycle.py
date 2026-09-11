"""Adverse lifecycle tests for a live callback route and its bounded worker."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport
from tests.unit.test_callback_route_reconfiguration import _callback_errors


class _AckObservedTransport(ScriptedBrokerTransport):
    """Expose actual written PUBACKs as deterministic test barriers."""

    def __init__(self, protocol: MQTTProtocolVersion) -> None:
        super().__init__(protocol=protocol)
        self.acknowledged: list[int] = []
        self.waiting: dict[int, asyncio.Event] = {}

    def handle_packet(self, raw: RawPacket) -> None:
        super().handle_packet(raw)
        if raw.packet_type is PacketType.PUBACK:
            mid = int.from_bytes(raw.remaining[:2], "big")
            self.acknowledged.append(mid)
            self.waiting.setdefault(mid, asyncio.Event()).set()

    async def wait_ack(self, mid: int) -> None:
        await asyncio.wait_for(self.waiting.setdefault(mid, asyncio.Event()).wait(), timeout=2)

    def push_messages(self, *values: int) -> None:
        self.push_rx(
            b"".join(
                PublishPacket(
                    topic="life/x",
                    payload=value.to_bytes(2, "big") + b"x" * 510,
                    qos=QoS.AT_LEAST_ONCE,
                    mid=value + 1,
                    retain=False,
                    dup=False,
                ).encode(self.protocol)
                for value in values
            )
        )


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("mode", ["callback", "both"])
@pytest.mark.parametrize("budget", [None, 4096])
@pytest.mark.parametrize("reconfigure", [False, True])
async def test_callback_reconnect_keeps_new_deliveries_and_discards_old_queue(
    protocol, mode, budget, reconfigure
):
    old = _AckObservedTransport(protocol)
    new = _AckObservedTransport(protocol)
    transports = iter((old, new))
    client = AsyncClient(
        client_id="route-lifecycle",
        protocol=protocol,
        message_delivery=mode,
        max_pending_callbacks=4,
        max_pending_messages=4,
        max_pending_delivery_bytes=budget,
    )

    async def factory(*_args, **_kwargs):
        return next(transports)

    client._transport_factory = factory
    seen: list[int] = []
    resumed = asyncio.Event()
    worker = None

    async def callback(message: Message) -> None:
        nonlocal worker
        value = int.from_bytes(message.payload[:2], "big")
        seen.append(value)
        assert not client._engine_lock.locked()
        if value != 1:
            return
        worker = asyncio.current_task()
        assert client._callback_worker_task is worker
        # Old queued delivery must be discarded, not revived by clearing stop.
        old.push_messages(3)
        await old.wait_ack(4)
        await client._flush_effects()
        await client.disconnect()
        await client.connect("test", 1883)
        new.push_messages(100, 101)
        await new.wait_ack(102)
        await client._flush_effects()
        resumed.set()

    def first(message: Message) -> None:
        seen.append(int.from_bytes(message.payload[:2], "big"))
        client.message_callback_add("life/x", callback)

    client.message_callback_add("life/x", first if reconfigure else callback)
    with _callback_errors() as errors:
        try:
            await asyncio.wait_for(client.connect("test", 1883), timeout=2)
            old.push_messages(0, 1)
            await asyncio.wait_for(resumed.wait(), timeout=3)
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert new.acknowledged == [101, 102]
            assert seen == [0, 1, 100, 101]
            assert client.state is ConnectionState.CONNECTED
            assert client._callback_worker_task is worker
            assert worker is not None and not worker.done()
            assert client.stats().delivery.callback_queued == 0
            assert client._callback_queue.maxsize == 4
            if mode == "both":
                stream = client.messages()
                delivered = [await anext(stream), await anext(stream)]
                assert [int.from_bytes(m.payload[:2], "big") for m in delivered] == [100, 101]
                await stream.aclose()
            assert client.stats().delivery.pending_bytes == 0
            assert errors == []
        finally:
            await asyncio.wait_for(client.disconnect(), timeout=2)


async def test_terminal_callback_disconnect_still_discards_queued_jobs() -> None:
    client = AsyncClient(message_delivery="callback")
    seen = []

    async def callback() -> None:
        seen.append("active")
        client._delivery.spawn_callback(lambda: seen.append("stale"))
        await client.disconnect()

    client._delivery.spawn_callback(callback)
    with _callback_errors() as errors:
        await asyncio.wait_for(client._callback_queue.join(), timeout=2)
        assert seen == ["active"]
        assert errors == []
        await client._shutdown_callback_worker(drain=False)


@pytest.mark.parametrize("eager", [False, True])
async def test_reopen_releases_old_accounting_without_replacing_active_worker(eager) -> None:
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager_task_factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    client = AsyncClient(message_delivery="callback", max_pending_delivery_bytes=4096)
    delivery = client._delivery
    seen = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def active() -> None:
        seen.append("active")
        await delivery.enqueue_callback(lambda: seen.append("old"), delivery_token=token)
        await client.disconnect()
        delivery.reopen()
        assert delivery.pending_bytes == 0
        assert delivery.callback_task is asyncio.current_task()
        assert delivery._callback_state == "open"
        await delivery.enqueue_callback(lambda: seen.append("new"))
        entered.set()
        await release.wait()

    token = delivery.try_reserve(512, 1)
    assert token == 512
    with _callback_errors() as errors:
        try:
            delivery.spawn_callback(active)
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert seen == ["active"]
            release.set()
            await asyncio.wait_for(delivery.callback_queue.join(), timeout=2)
            assert seen == ["active", "new"]
            assert errors == []
        finally:
            release.set()
            await delivery.shutdown_callbacks(drain=False)
            loop.set_task_factory(factory)


async def test_repeated_stop_reopen_keeps_one_worker_and_exact_reservations() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    delivery = client._delivery
    seen = []
    workers = set()

    async def cycle(value: int) -> None:
        workers.add(asyncio.current_task())
        seen.append(value)
        token = delivery.try_reserve(128, 1)
        assert token == 128
        await delivery.enqueue_callback(lambda: seen.append("stale"), delivery_token=token)
        await delivery.shutdown_callbacks(drain=False)
        delivery.reopen()
        assert delivery.pending_bytes == 0
        assert delivery.callback_queue.maxsize == 2
        if value < 15:
            await delivery.enqueue_callback(cycle, value + 1)

    try:
        delivery.spawn_callback(cycle, 0)
        await asyncio.wait_for(delivery.callback_queue.join(), timeout=2)
        assert seen == list(range(16))
        assert workers == {delivery.callback_task}
        assert delivery.stats().callback_queued == 0
    finally:
        await delivery.shutdown_callbacks(drain=False)
