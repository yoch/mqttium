"""Disconnect behavior while a connection attempt is in progress (#237)."""

from __future__ import annotations
import asyncio
import pytest
from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import MQTTError, ProtocolError
from mqttium.packets import encode_frame
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine


class _Transport:
    def __init__(self):
        self.decoder = IncrementalDecoder()
        self.rx = asyncio.Queue()
        self.packet_types = []
        self.connect_written = asyncio.Event()
        self.disconnect_written = asyncio.Event()
        self.closing = False

    async def write(self, data):
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            self.packet_types.append(raw.packet_type)
            if raw.packet_type is PacketType.CONNECT:
                self.connect_written.set()
            if raw.packet_type is PacketType.DISCONNECT:
                self.disconnect_written.set()

    async def read(self, n=65536):
        return await self.rx.get()

    async def close(self):
        if not self.closing:
            self.closing = True
            self.rx.put_nowait(b"")

    def is_closing(self):
        return self.closing


def _connack(protocol):
    return encode_frame(
        PacketType.CONNACK,
        0,
        b"\x00\x00" + (b"\x00" if protocol is MQTTProtocolVersion.MQTTv5 else b""),
    )


def test_engine_disconnect_connecting_ignores_late_connack():
    e = ProtocolEngine(EngineConfig(client_id="c"))
    e.begin_connect()
    assert e.begin_disconnect()
    d = IncrementalDecoder()
    d.feed(_connack(MQTTProtocolVersion.MQTTv311))
    raw = d.next_packet()
    assert raw
    e.handle_raw(raw)
    assert e.state is ConnectionState.DISCONNECTING
    assert e.take_effects() == []


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_runtime_disconnect_connecting_writes_terminal_packet(protocol):
    c = AsyncClient(client_id="c", protocol=protocol)
    t = _Transport()

    async def factory(host, port, *, ssl=None):
        return t

    c._transport_factory = factory
    connecting = asyncio.create_task(c.connect("fake", timeout=30))
    await asyncio.wait_for(t.connect_written.wait(), 1)
    await asyncio.wait_for(c.disconnect(), 1)
    with pytest.raises(MQTTError, match="cancelled by disconnect"):
        await connecting
    assert t.packet_types[:2] == [PacketType.CONNECT, PacketType.DISCONNECT]
    assert t.closing and c.state is ConnectionState.DISCONNECTED
    assert c._transport is None and c._reader_task is None and c._writer_task is None
    assert c._connect_disconnect_fut is None


async def test_invalid_v5_reason_does_not_consume_connect_attempt():
    c = AsyncClient(client_id="c", protocol=MQTTProtocolVersion.MQTTv5)
    t = _Transport()

    async def factory(host, port, *, ssl=None):
        return t

    c._transport_factory = factory
    connecting = asyncio.create_task(c.connect("fake", timeout=30))
    await asyncio.wait_for(t.connect_written.wait(), 1)
    with pytest.raises((ProtocolError, ValueError)):
        await c.disconnect(0x01)
    assert t.packet_types == [PacketType.CONNECT] and not t.closing
    t.rx.put_nowait(_connack(MQTTProtocolVersion.MQTTv5))
    await asyncio.wait_for(connecting, 1)
    assert c.state is ConnectionState.CONNECTED
    await c.disconnect()


async def test_reconnect_attempt_owner_is_not_cancelled_before_disconnect_is_drained():
    c = AsyncClient(client_id="c", protocol=MQTTProtocolVersion.MQTTv5)
    t = _Transport()

    async def factory(host, port, *, ssl=None):
        return t

    c._transport_factory = factory

    async def attempt():
        async with c._lifecycle_lock:
            await c._connect_once_locked("fake", 1883, timeout=30, reconnect_attempt=True)

    task = asyncio.create_task(attempt())
    c._reconnect_task = task
    await asyncio.wait_for(t.connect_written.wait(), 1)
    await asyncio.wait_for(c.disconnect(), 1)
    with pytest.raises(MQTTError, match="cancelled by disconnect"):
        await task
    c._reconnect_task = None
    assert t.packet_types[:2] == [PacketType.CONNECT, PacketType.DISCONNECT]
    assert c._intentional_disconnect and c.state is ConnectionState.DISCONNECTED
