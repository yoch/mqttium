"""Message iterator lifecycle across automatic reconnects."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy


class _Broker:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._rx.put_nowait(b"")

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


async def _wait_for_connection(
    client: AsyncClient,
    brokers: list[_Broker],
    *,
    count: int,
) -> _Broker:
    for _ in range(200):
        if client.is_connected and len(brokers) >= count:
            return brokers[-1]
        await asyncio.sleep(0.01)
    raise AssertionError(f"client did not establish connection #{count}")


async def _wait_for_retry_exhaustion(
    client: AsyncClient,
    attempts: Callable[[], int],
) -> None:
    for _ in range(200):
        if attempts() >= 2 and client._reconnect_task is None:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("reconnect policy did not exhaust")


async def _cleanup(client: AsyncClient, *tasks: asyncio.Task[object] | None) -> None:
    for task in tasks:
        if task is None:
            continue
        if task.done():
            with suppress(asyncio.CancelledError):
                task.exception()
            continue
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    with suppress(Exception, asyncio.CancelledError):
        await client.disconnect()


def _policy(*, max_retries: int | None = 4) -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=0.0,
        max_delay=0.0,
        max_retries=max_retries,
        stable_after=0.0,
        connect_timeout=0.25,
    )


async def test_messages_iterator_survives_unexpected_reconnect() -> None:
    brokers: list[_Broker] = []
    client = AsyncClient(
        "stream-reconnect",
        reconnect=_policy(),
        message_delivery="iterator",
    )

    async def factory(host: str, port: int, *, ssl=None):
        broker = _Broker()
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    pending: asyncio.Task[object] | None = None
    terminal: asyncio.Task[object] | None = None
    try:
        await client.connect("fake", 1, timeout=1.0)
        first = brokers[0]
        stream = client.messages()

        first.publish("before", b"1")
        before = await asyncio.wait_for(anext(stream), timeout=1.0)
        assert before.topic == "before"

        # Keep the same iterator suspended while the transport drops. This is the
        # production `async for` shape: the next __anext__ is already waiting when
        # the reader enters reconnect teardown.
        pending = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await first.close()

        second = await _wait_for_connection(client, brokers, count=2)
        second.publish("after", b"2")
        after = await asyncio.wait_for(pending, timeout=1.0)
        assert after.topic == "after"

        terminal = asyncio.create_task(anext(stream))
        await asyncio.sleep(0)
        await client.disconnect()
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(terminal, timeout=1.0)
    finally:
        await _cleanup(client, pending, terminal)


async def test_messages_iterator_survives_failed_reconnect_attempt() -> None:
    brokers: list[_Broker] = []
    attempts = 0
    client = AsyncClient(
        "stream-retry",
        reconnect=_policy(),
        message_delivery="iterator",
    )

    async def factory(host: str, port: int, *, ssl=None):
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            raise OSError("transient reconnect failure")
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

        await brokers[0].close()
        second = await _wait_for_connection(client, brokers, count=2)
        assert attempts >= 3
        second.publish("after-retry", b"ok")

        message = await asyncio.wait_for(pending, timeout=1.0)
        assert message.topic == "after-retry"
    finally:
        await _cleanup(client, pending)


async def test_messages_iterator_closes_when_reconnect_is_exhausted() -> None:
    brokers: list[_Broker] = []
    attempts = 0
    client = AsyncClient(
        "stream-exhausted",
        reconnect=_policy(max_retries=1),
        message_delivery="iterator",
    )

    async def factory(host: str, port: int, *, ssl=None):
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            raise OSError("broker remains unavailable")
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

        await brokers[0].close()
        await _wait_for_retry_exhaustion(client, lambda: attempts)
        assert attempts == 2
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(pending, timeout=1.0)
    finally:
        await _cleanup(client, pending)
