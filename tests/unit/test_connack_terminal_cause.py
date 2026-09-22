"""Known pre-CONNACK failures must reach the caller before its deadline."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.errors import MalformedPacketError, MQTTError
from tests.support import QueueTransport, transport_factory, wait_until


@pytest.mark.parametrize("failure", ["eof", "malformed", "io"])
async def test_reader_failure_completes_connect_with_original_cause(failure):
    cause = OSError("read failed")

    class FailingPeer(QueueTransport):
        async def write(self, data):
            if data[0] & 0xF0 == int(PacketType.CONNECT):
                self.push_rx(b"\x21\x03\x00\x00\x00" if failure == "malformed" else b"")

        async def read(self, n=65536):
            data = await super().read(n)
            if failure == "io":
                raise cause
            return data

    transport = FailingPeer()
    client = AsyncClient("connack-cause", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0)
    client._transport_factory = transport_factory(transport)
    expected = {"eof": MQTTError, "malformed": MalformedPacketError, "io": OSError}[failure]
    try:
        with pytest.raises(expected) as caught:
            # Outer watchdog distinguishes prompt failure from the 5s client deadline.
            await asyncio.wait_for(client.connect("unused", timeout=5), 1)
        assert caught.value is client._disconnect_exc
        if failure == "io":
            assert caught.value is cause
        if failure == "eof":
            assert str(caught.value) == "Connection closed"
        assert transport.is_closing()
        await wait_until(lambda: not any(client._running_tasks().values()))
    finally:
        await client.disconnect()
