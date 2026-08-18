"""AsyncClient lifecycle serialization and cancellation cleanup."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import NotConnectedError, ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Message


class _Transport:
    def __init__(
        self,
        *,
        connack: bool,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._connack = connack
        self._protocol = protocol
        self._closing = False
        self.connect_written = asyncio.Event()

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self.connect_written.set()
                if self._connack:
                    body = (
                        b"\x00\x00\x00"
                        if self._protocol is MQTTProtocolVersion.MQTTv5
                        else b"\x00\x00"
                    )
                    self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, body))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


class _BlockedWriterTransport(_Transport):
    """Keep one application write in flight so the bounded queue fills."""

    def __init__(self) -> None:
        super().__init__(connack=True)
        self.block_application_writes = False
        self.write_started = asyncio.Event()

    async def write(self, data: bytes) -> None:
        if self.block_application_writes and data and data[0] != PacketType.CONNECT:
            self.write_started.set()
            await asyncio.Event().wait()
        await super().write(data)


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


async def test_intentional_disconnect_callback_receives_none() -> None:
    client = AsyncClient(client_id="clean-disconnect")
    transport = _Transport(connack=True)
    errors: list[BaseException | None] = []

    async def factory(host: str, port: int, *, ssl: object = None) -> _Transport:
        return transport

    client._transport_factory = factory
    client.on_disconnect = errors.append

    await client.connect("fake", timeout=2.0)
    await client.disconnect()

    assert errors == [None]


async def test_disconnect_does_not_wait_for_space_in_saturated_writer_queue() -> None:
    client = AsyncClient(
        client_id="bounded-disconnect",
        max_outbound_messages=2,
        max_outbound_bytes=1024,
    )
    transport = _BlockedWriterTransport()

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _BlockedWriterTransport:
        return transport

    client._transport_factory = factory
    await client.connect("fake", timeout=2.0)
    transport.block_application_writes = True

    await client.publish("blocked/first", b"1")
    await asyncio.wait_for(transport.write_started.wait(), timeout=1.0)
    await client.publish("blocked/queued", b"2")
    assert client._write_pump.queued_messages == 1
    assert client._write_pump.resident_messages == 2

    await asyncio.wait_for(client.disconnect(), timeout=0.5)

    await asyncio.wait_for(client.disconnect(), timeout=0.5)

    assert transport.is_closing()
    assert client._transport is None
    assert client.state is ConnectionState.DISCONNECTED


async def test_ack_after_transport_drop_preserves_durable_inbound_record() -> None:
    store = MemoryInflightStore()
    transport = _Transport(connack=True, protocol=MQTTProtocolVersion.MQTTv5)
    client = AsyncClient(
        client_id="disconnected-ack",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=False,
        manual_ack=True,
        message_delivery="callback",
        store=store,
    )
    delivered: list[Message] = []
    message_ready = asyncio.Event()

    async def factory(host: str, port: int, *, ssl: object = None) -> _Transport:
        return transport

    def on_message(message: Message) -> None:
        delivered.append(message)
        message_ready.set()

    client._transport_factory = factory
    client.on_message = on_message
    await client.connect("fake", timeout=2.0)
    transport._rx.put_nowait(
        PublishPacket(
            topic="manual/ack",
            payload=b"held",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv5)
    )
    await asyncio.wait_for(message_ready.wait(), timeout=1.0)
    assert store.get_in(7) is not None

    await transport.close()
    reader = client._reader_task
    assert reader is not None
    await asyncio.wait_for(reader, timeout=1.0)

    with pytest.raises(NotConnectedError, match="active connection"):
        await client.ack(delivered[0])
    assert store.get_in(7) is not None
