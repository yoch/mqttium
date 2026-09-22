"""A fatal DISCONNECT fences public admission and already deferred sends."""

import asyncio

import pytest

from mqttium.api import AsyncClient, Properties
from mqttium.api._effects import StaleConnectionEffect
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import NotConnectedError
from tests.support import ScriptedBrokerTransport, transport_factory


class _PausedTerminalTransport(ScriptedBrokerTransport):
    def __init__(self):
        super().__init__(protocol=MQTTProtocolVersion.MQTTv5)
        self.terminal_started = asyncio.Event()
        self.release = asyncio.Event()
        self.packets = []

    async def write(self, data):
        if data[0] & 0xF0 == int(PacketType.DISCONNECT):
            self.terminal_started.set()
            await self.release.wait()
        await super().write(data)

    def handle_packet(self, raw):
        self.packets.append(raw.packet_type)
        if raw.packet_type is PacketType.CONNECT:
            # Successful CONNACK repeats the CONNECT authentication method.
            self.push_rx(b"\x20\x0b\x00\x00\x08\x15\x00\x05audit")
        else:
            super().handle_packet(raw)

    async def close(self):
        self.release.set()
        await super().close()


async def test_fatal_disconnect_refuses_public_and_deferred_sends():
    transport = _PausedTerminalTransport()
    client = AsyncClient(
        "fatal-admission",
        protocol=MQTTProtocolVersion.MQTTv5,
        keepalive=0,
        auth_handler=lambda packet: None,
        connect_properties=Properties({"authentication_method": "audit"}),
    )
    client._transport_factory = transport_factory(transport)
    await client.connect("unused")
    reader = client._reader_task
    try:
        transport.push_rx(b"\xe1\x00")
        await asyncio.wait_for(transport.terminal_started.wait(), 1)
        assert not client.is_connected
        with pytest.raises(NotConnectedError):
            await client.publish("after/fatal", b"x")
        with pytest.raises(NotConnectedError):
            client.publish_nowait("after/fatal", b"x")
        with pytest.raises(NotConnectedError):
            await client.subscribe("after/fatal")
        with pytest.raises(NotConnectedError):
            await client.unsubscribe("after/fatal")
        with pytest.raises(NotConnectedError):
            await client.auth()
        # An effect already owned by this epoch must not bypass the fence.
        with pytest.raises(StaleConnectionEffect):
            await client._write_pump.enqueue(b"\xc0\x00", epoch=client._connection_epoch)
        with pytest.raises(StaleConnectionEffect):
            await client._write_pump.enqueue_ack(b"\x40\x02\x00\x01")
        transport.release.set()
        await asyncio.wait_for(reader, 1)
        assert transport.packets[0] is PacketType.CONNECT
        assert transport.packets[-1] is PacketType.DISCONNECT
        assert transport.packets.count(PacketType.DISCONNECT) == 1
        assert set(transport.packets) <= {
            PacketType.CONNECT,
            PacketType.PUBACK,
            PacketType.DISCONNECT,
        }
        # A replacement connection gets a fresh writer admission window.
        replacement = _PausedTerminalTransport()
        replacement.release.set()
        client._transport_factory = transport_factory(replacement)
        await client.connect("unused")
        await client.publish("replacement", b"accepted")
    finally:
        transport.release.set()
        await client.disconnect()
