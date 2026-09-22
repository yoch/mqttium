"""Committed shutdown retains cleanup ownership when its caller is cancelled."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from tests.support import ScriptedBrokerTransport, transport_factory


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_cancelling_terminal_drain_closes_connection_and_tasks(protocol):
    class PausedTerminalTransport(ScriptedBrokerTransport):
        def __init__(self):
            super().__init__(protocol=protocol)
            self.started = asyncio.Event()

        async def write(self, data):
            if data[0] & 0xF0 == int(PacketType.DISCONNECT):
                self.started.set()
                await asyncio.Event().wait()
            await super().write(data)

    transport = PausedTerminalTransport()
    client = AsyncClient("cancel-disconnect", protocol=protocol, keepalive=0)
    client._transport_factory = transport_factory(transport)
    await client.connect("unused")
    shutdown = asyncio.create_task(client.disconnect())
    try:
        await asyncio.wait_for(transport.started.wait(), 1)
        shutdown.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert transport.is_closing()
        assert client.state is ConnectionState.DISCONNECTED
        assert not any(client._running_tasks().values())
        assert client._transport is None
        # Idempotence and a later explicit connection remain supported.
        await client.disconnect()
        replacement = ScriptedBrokerTransport(protocol=protocol)
        client._transport_factory = transport_factory(replacement)
        await client.connect("unused")
        await client.publish("replacement", b"ok")
    finally:
        shutdown.cancel()
        await asyncio.gather(shutdown, return_exceptions=True)
        await client.disconnect()
