"""Connection stages spend one budget, including WebSocket upgrade."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import PacketType
from mqttium.errors import MQTTTimeoutError
from mqttium.transport import websocket
from tests.support import ScriptedBrokerTransport, wait_until


@pytest.mark.parametrize("override", [None, 0.2])
async def test_setup_and_connack_share_one_deadline(override):
    class DelayedConnackTransport(ScriptedBrokerTransport):
        timer = None

        def handle_packet(self, raw):
            if raw.packet_type is PacketType.CONNECT:
                self.timer = asyncio.get_running_loop().call_later(0.12, super().handle_packet, raw)
            else:
                super().handle_packet(raw)

        async def close(self):
            if self.timer is not None:
                self.timer.cancel()
            await super().close()

    transport = DelayedConnackTransport()
    client = AsyncClient("deadline", connect_timeout=0.2, keepalive=0)

    async def factory(*args, **kwargs):
        await asyncio.sleep(0.12)
        return transport

    client._transport_factory = factory
    try:
        with pytest.raises(MQTTTimeoutError):
            await client.connect("unused", timeout=override)
        assert transport.is_closing()
        await wait_until(lambda: not any(client._running_tasks().values()))
    finally:
        await client.disconnect()


@pytest.mark.parametrize("override", [None, 60])
async def test_public_websocket_attempt_owns_upgrade_deadline(monkeypatch, override):
    class Writer:
        closed = False

        def write(self, data):
            pass

        async def drain(self):
            pass

        def close(self):
            self.closed = True

        async def wait_closed(self):
            pass

    writer = Writer()

    async def open_connection(*args, **kwargs):
        return asyncio.StreamReader(), writer

    cause = OSError("upgrade failure")

    async def read_upgrade(reader, timeout):
        # The outer attempt already covers opening and the entire upgrade.
        # An inner default here would truncate public budgets above 30 seconds.
        assert timeout is None
        raise cause

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    monkeypatch.setattr(websocket, "_read_handshake_response", read_upgrade)
    client = AsyncClient("websocket-deadline", connect_timeout=60, keepalive=0)
    with pytest.raises(OSError) as caught:
        await client.connect_ws("ws://unused/mqtt", timeout=override)
    assert caught.value is cause
    assert writer.closed
    assert not any(client._running_tasks().values())
