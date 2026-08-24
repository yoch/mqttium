"""Deterministic AsyncClient interleavings at connection ownership boundaries."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy

from tests.support import QueueTransport, transport_factory, write_item_bytes


class _ControlledTransport(QueueTransport):
    """Packet-aware transport with an explicit application-write failure gate."""

    def __init__(self) -> None:
        super().__init__()
        self.decoder = IncrementalDecoder()
        self.publish_write_entered = asyncio.Event()
        self.release_publish_write = asyncio.Event()
        self.fail_publish_write = False
        self.close_calls = 0

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        self.decoder.feed(write_item_bytes(data))
        for raw in self.decoder.drain_packets():
            await self._handle(raw)

    async def _handle(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.CONNECT:
            self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
        elif raw.packet_type is PacketType.PUBLISH:
            self.publish_write_entered.set()
            await self.release_publish_write.wait()
            if self.fail_publish_write:
                raise OSError("controlled writer failure")

    async def close(self) -> None:
        self.close_calls += 1
        if not self.is_closing():
            await super().close()


def _reconnect_policy() -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=0,
        max_delay=0,
        max_retries=2,
        stable_after=0,
        connect_timeout=1,
    )


async def test_writer_failure_racing_broker_disconnect_has_one_logical_teardown() -> None:
    transport = _ControlledTransport()
    client = AsyncClient(
        client_id="writer-disconnect-race",
        protocol=MQTTProtocolVersion.MQTTv5,
    )
    client._transport_factory = transport_factory(transport)
    disconnects: list[BaseException | None] = []
    client.on_disconnect = disconnects.append

    await client.connect("fake", timeout=1)
    receipt = await client.publish("race/topic", b"payload", qos=1)
    await asyncio.wait_for(transport.publish_write_entered.wait(), timeout=1)
    reader = client._reader_task
    assert reader is not None

    transport.fail_publish_write = True
    transport.push_rx(encode_frame(PacketType.DISCONNECT, 0, b""))
    transport.release_publish_write.set()
    await asyncio.wait_for(reader, timeout=1)

    assert len(disconnects) == 1
    assert receipt.is_done()
    with pytest.raises((MQTTError, OSError)):
        await receipt.wait()
    assert client._reader_task is reader
    assert client._reconnect_task is None


async def test_terminal_disconnect_wakes_publish_blocked_on_logical_backpressure() -> None:
    transport = _ControlledTransport()
    client = AsyncClient(
        client_id="blocked-publisher-teardown",
        protocol=MQTTProtocolVersion.MQTTv5,
        max_pending_outbound_messages=1,
    )
    client._transport_factory = transport_factory(transport)

    await client.connect("fake", timeout=1)
    first = await client.publish("blocked/first", qos=1)
    await asyncio.wait_for(transport.publish_write_entered.wait(), timeout=1)
    blocked = asyncio.create_task(client.publish("blocked/second", qos=1))
    for _ in range(20):
        if client._publish_waiters:
            break
        await asyncio.sleep(0)
    assert client._publish_waiters == 1
    reader = client._reader_task
    assert reader is not None

    transport.push_rx(encode_frame(PacketType.DISCONNECT, 0, b""))
    transport.release_publish_write.set()
    await asyncio.wait_for(reader, timeout=1)

    with pytest.raises(MQTTError):
        await asyncio.wait_for(blocked, timeout=1)
    with pytest.raises(MQTTError):
        await first.wait()
    assert client._publish_waiters == 0


async def test_writer_failure_wakes_reader_blocked_on_puback_admission() -> None:
    transport = _ControlledTransport()
    client = AsyncClient(
        client_id="reader-puback-writer-failure",
        protocol=MQTTProtocolVersion.MQTTv5,
        max_outbound_messages=1,
        max_outbound_bytes=1024,
        reconnect=ReconnectPolicy(enabled=False),
    )
    client._transport_factory = transport_factory(transport)
    disconnected = asyncio.Event()
    client.on_disconnect = lambda _exc: disconnected.set()

    await client.connect("fake", timeout=1)
    published = asyncio.create_task(client.publish("out/q0", b"hold"))
    await asyncio.wait_for(transport.publish_write_entered.wait(), timeout=1)
    transport.push_rx(
        PublishPacket(
            topic="in/q1",
            payload=b"needs-puback",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(MQTTProtocolVersion.MQTTv5)
    )
    for _ in range(100):
        if client._write_pump.waiters:
            break
        await asyncio.sleep(0)
    assert client._write_pump.waiters == 1

    transport.fail_publish_write = True
    transport.release_publish_write.set()
    await asyncio.wait_for(disconnected.wait(), timeout=1)
    await asyncio.wait_for(published, timeout=1)

    assert client._write_pump.waiters == 0
    assert client._reader_task is not None and client._reader_task.done()
    await client.disconnect()


async def test_user_disconnect_cancels_reconnect_during_transport_setup() -> None:
    first = _ControlledTransport()
    client = AsyncClient(
        client_id="disconnect-reconnect-race",
        protocol=MQTTProtocolVersion.MQTTv5,
        reconnect=_reconnect_policy(),
    )
    reconnect_factory_entered = asyncio.Event()
    calls = 0

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        del host, port, ssl
        calls += 1
        if calls == 1:
            return first
        reconnect_factory_entered.set()
        await asyncio.Event().wait()

    client._transport_factory = factory
    await client.connect("fake", timeout=1)
    reader = client._reader_task
    assert reader is not None
    await first.close()
    await asyncio.wait_for(reconnect_factory_entered.wait(), timeout=1)

    await asyncio.wait_for(client.disconnect(), timeout=1)
    await asyncio.wait_for(reader, timeout=1)

    assert calls == 2
    assert client._reconnect_task is None
    assert client._transport is None
    assert client._intentional_disconnect is True
