"""AsyncClient lifecycle serialization and cancellation cleanup."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy


class _Transport:
    def __init__(self, *, connack: bool) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._connack = connack
        self._closing = False
        self.connect_written = asyncio.Event()

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.connect_written.set()
                if self._connack:
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


async def test_concurrent_connects_open_one_transport() -> None:
    client = AsyncClient(client_id="lifecycle")
    release_factory = asyncio.Event()
    factory_entered = asyncio.Event()
    calls = 0
    transport = _Transport(connack=True)

    async def factory(host: str, port: int, *, ssl: object = None) -> _Transport:
        nonlocal calls
        calls += 1
        factory_entered.set()
        await release_factory.wait()
        return transport

    client._transport_factory = factory
    first = asyncio.create_task(client.connect("fake", timeout=2.0))
    await factory_entered.wait()
    second = asyncio.create_task(client.connect("fake", timeout=2.0))
    await asyncio.sleep(0)
    assert calls == 1

    release_factory.set()
    await first
    with pytest.raises(ProtocolError, match="Already connected"):
        await second
    assert calls == 1
    assert client.is_connected
    await client.disconnect()


async def test_cancelled_connect_closes_transport_and_tasks() -> None:
    reconnect = ReconnectPolicy(enabled=True, initial_delay=0.01, max_delay=0.01)
    client = AsyncClient(client_id="cancel-connect", reconnect=reconnect)
    transport = _Transport(connack=False)
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None) -> _Transport:
        nonlocal calls
        calls += 1
        return transport

    client._transport_factory = factory
    task = asyncio.create_task(client.connect("fake", timeout=30.0))
    await asyncio.wait_for(transport.connect_written.wait(), timeout=2.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.05)

    assert calls == 1
    assert transport.is_closing()
    assert client._transport is None
    assert client._reader_task is None
    assert client._writer_task is None
    assert client._keepalive_task is None
    assert client._reconnect_task is None
    assert client.state is ConnectionState.DISCONNECTED


async def test_disconnect_waits_for_in_progress_connect_without_overlap() -> None:
    client = AsyncClient(client_id="connect-disconnect")
    release_factory = asyncio.Event()
    factory_entered = asyncio.Event()
    transport = _Transport(connack=True)

    async def factory(host: str, port: int, *, ssl: object = None) -> _Transport:
        factory_entered.set()
        await release_factory.wait()
        return transport

    client._transport_factory = factory
    connecting = asyncio.create_task(client.connect("fake", timeout=2.0))
    await factory_entered.wait()
    disconnecting = asyncio.create_task(client.disconnect())
    await asyncio.sleep(0)
    assert not disconnecting.done()

    release_factory.set()
    await connecting
    await disconnecting
    assert transport.is_closing()
    assert client._transport is None
    assert client.state is ConnectionState.DISCONNECTED
