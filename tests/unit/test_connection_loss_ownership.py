"""Connection-loss sources must hand teardown ownership to the reader."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import PacketTooLargeError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Properties


class _Broker:
    def __init__(self, *, connack: bytes | None = None) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.raise_on_close = False
        self.connack = connack or encode_frame(PacketType.CONNACK, 0, b"\x00\x00")

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(self.connack)

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            self._rx.put_nowait(b"")
        if self.raise_on_close:
            raise OSError("secondary transport close failure")

    def is_closing(self) -> bool:
        return self._closing

    def publish(self, topic: str, payload: bytes) -> None:
        self._rx.put_nowait(
            PublishPacket(
                topic=topic,
                payload=payload,
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                mid=None,
            ).encode()
        )


class _FailingConnectWriter:
    """Fail CONNECT writing and then fail the best-effort transport close."""

    def __init__(self, primary: OSError, secondary: OSError) -> None:
        self.primary = primary
        self.secondary = secondary

    async def write(self, data: bytes) -> None:
        del data
        raise self.primary

    async def read(self, n: int = 65536) -> bytes:
        del n
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        raise self.secondary

    def is_closing(self) -> bool:
        return False


def _policy() -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=0.0,
        max_delay=0.0,
        max_retries=4,
        stable_after=0.0,
        connect_timeout=0.25,
    )


async def _wait_for_connection(client: AsyncClient, brokers: list[_Broker], count: int) -> _Broker:
    for _ in range(200):
        if client.is_connected and len(brokers) >= count:
            return brokers[-1]
        await asyncio.sleep(0.01)
    raise AssertionError(f"client did not establish connection #{count}")


async def _cleanup(client: AsyncClient, *tasks: asyncio.Task[object] | None) -> None:
    for task in tasks:
        if task is None:
            continue
        if not task.done():
            task.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration, Exception):
            await task
    with suppress(Exception, asyncio.CancelledError):
        await client.disconnect()


async def test_ping_timeout_reconnect_emits_one_disconnect_and_preserves_stream() -> None:
    brokers: list[_Broker] = []
    disconnects: list[BaseException | None] = []
    client = AsyncClient(
        "ping-owner",
        keepalive=1,
        ping_timeout=0.01,
        reconnect=_policy(),
        message_delivery="iterator",
    )
    client.on_disconnect = lambda exc: disconnects.append(exc)

    async def factory(host: str, port: int, *, ssl=None):
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)

        keepalive = client._keepalive_task
        assert keepalive is not None
        keepalive.cancel()
        with suppress(asyncio.CancelledError):
            await keepalive
        client._keepalive_task = None

        client._ping_pending = True
        client._ping_deadline = time.monotonic() - 1.0
        keepalive = asyncio.create_task(client._keepalive_loop())
        client._keepalive_task = keepalive
        await asyncio.wait_for(keepalive, timeout=1.0)

        second = await _wait_for_connection(client, brokers, 2)
        assert len(disconnects) == 1
        second.publish("after-ping-timeout", b"ok")
        message = await asyncio.wait_for(pending, timeout=1.0)
        assert message.topic == "after-ping-timeout"
    finally:
        await _cleanup(client, pending)


async def test_disconnect_in_reconnect_gap_wakes_logical_publish_waiter() -> None:
    brokers: list[_Broker] = []
    attempts = 0
    client = AsyncClient(
        "gap-waiter",
        reconnect=_policy(),
        message_delivery="iterator",
    )

    async def factory(host: str, port: int, *, ssl=None):
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            await asyncio.sleep(10.0)
            raise OSError("broker remains unreachable")
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    waiter_task: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        await brokers[0].close()
        for _ in range(200):
            if client._transport is None and client._reconnect_task is not None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("client never entered reconnect gap")

        async with client._engine_lock:
            waiter = client._register_publish_waiter()
        waiter_task = asyncio.create_task(client._wait_publish_space(waiter))
        await asyncio.sleep(0)

        await client.disconnect()
        assert client._teardown_final is True
        await asyncio.wait_for(waiter_task, timeout=0.5)
    finally:
        await _cleanup(client, waiter_task)


async def test_writer_failure_keeps_primary_error_when_transport_close_raises() -> None:
    broker = _Broker()
    client = AsyncClient("writer-owner", message_delivery="iterator")

    async def factory(host: str, port: int, *, ssl=None):
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    primary = OSError("writer failed")
    try:
        await client.connect("fake", 1, timeout=1.0)
        stream = client.messages()
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        broker.raise_on_close = True

        await client._writer_failed(primary)
        assert client._disconnect_exc is primary
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(pending, timeout=1.0)
    finally:
        broker.raise_on_close = False
        await _cleanup(client, pending)


async def test_terminal_broker_eof_stops_connection_keepalive_task() -> None:
    broker = _Broker()
    client = AsyncClient(
        "eof-keepalive-owner",
        keepalive=0,
        reconnect=ReconnectPolicy(enabled=False),
    )

    async def factory(host: str, port: int, *, ssl=None):
        return broker

    client._transport_factory = factory
    try:
        await client.connect("fake", 1, timeout=1.0)
        reader = client._reader_task
        keepalive = client._keepalive_task
        assert reader is not None
        assert keepalive is not None and not keepalive.done()

        await broker.close()
        await asyncio.wait_for(reader, timeout=1.0)

        assert keepalive.done()
        assert client._keepalive_task is None
    finally:
        await _cleanup(client)


async def test_impossible_pingreq_teardown_does_not_cycle_reader_and_keepalive() -> None:
    properties = Properties()
    properties.set("maximum_packet_size", 1)
    connack = encode_frame(
        PacketType.CONNACK,
        0,
        b"\x00\x00" + encode_properties(properties, CONNACK),
    )
    broker = _Broker(connack=connack)
    client = AsyncClient(
        "tiny-ping-owner",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=1,
        reconnect=ReconnectPolicy(enabled=False),
    )

    async def factory(host: str, port: int, *, ssl=None):
        return broker

    client._transport_factory = factory
    try:
        await client.connect("fake", 1, timeout=1.0)
        reader = client._reader_task
        keepalive = client._keepalive_task
        assert reader is not None and not reader.done()
        assert keepalive is not None and not keepalive.done()

        client._write_pump.last_outbound = time.monotonic() - 2.0
        await asyncio.wait_for(asyncio.gather(keepalive, reader), timeout=1.0)

        stats = client.stats()
        assert isinstance(client._disconnect_exc, PacketTooLargeError)
        assert not any(
            (
                stats.tasks.reader,
                stats.tasks.writer,
                stats.tasks.keepalive,
                stats.tasks.reconnect,
                stats.tasks.effect_flush,
                stats.tasks.callback_worker,
            )
        )
        assert stats.writer.waiters == 0
        assert stats.effects.waiters == 0
        assert stats.delivery.waiters == 0
        assert stats.writer.queued_messages == 0
        assert stats.writer.queued_bytes == 0
        assert stats.effects.pending == 0
        assert stats.receipts.publish == 0
    finally:
        await _cleanup(client)


async def test_connect_exposes_writer_error_when_transport_close_also_raises() -> None:
    primary = OSError("primary CONNECT write failure")
    secondary = OSError("secondary transport close failure")
    transport = _FailingConnectWriter(primary, secondary)
    client = AsyncClient("connecting-writer-owner")

    async def factory(host: str, port: int, *, ssl=None):
        return transport

    client._transport_factory = factory

    with pytest.raises(OSError) as caught:
        await client.connect("fake", 1, timeout=1.0)

    assert caught.value is primary
    assert client._transport is None
