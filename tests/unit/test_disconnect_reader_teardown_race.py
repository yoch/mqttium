"""disconnect() racing the reader's own teardown still settles everything.

After a connection loss the reconnect policy would retry, the reader keeps
pending receipts for replay and then releases the transport. A terminal
``disconnect()`` that cancels the reader during that release must not leave
the receipts pending for ever, nor drop ``on_disconnect``.
"""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.errors import MQTTError
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, transport_factory


class _SlowCloseSilentBroker(ScriptedBrokerTransport):
    """Never acknowledges a PUBLISH and takes a while to close."""

    def __init__(self) -> None:
        super().__init__()
        self.closing = asyncio.Event()
        self.release = asyncio.Event()

    def handle_packet(self, raw):
        if raw.packet_type is PacketType.PUBLISH:
            return
        super().handle_packet(raw)

    async def close(self):
        self.closing.set()
        await self.release.wait()
        await super().close()


@pytest.mark.parametrize("policy", [ReconnectPolicy(initial_delay=30.0, max_delay=30.0), None])
async def test_disconnect_during_reader_transport_close_settles_receipts(policy):
    transport = _SlowCloseSilentBroker()
    client = AsyncClient("teardown-race", keepalive=0, clean_start=False, reconnect=policy)
    client._transport_factory = transport_factory(transport)
    causes: list[BaseException | None] = []
    hook_ran = asyncio.Event()

    async def on_disconnect(cause):
        causes.append(cause)
        hook_ran.set()

    client.on_disconnect = on_disconnect
    await client.connect("unused")
    receipt = await client.publish("topic", b"payload", qos=1)
    reader = client._reader_task
    assert reader is not None

    transport.push_rx(b"")  # the peer closes
    await asyncio.wait_for(transport.closing.wait(), 1)
    assert not reader.done()  # parked inside its own transport release

    shutdown = asyncio.create_task(client.disconnect())
    await asyncio.sleep(0)
    transport.release.set()
    await asyncio.wait_for(shutdown, 2)

    assert client.state is ConnectionState.DISCONNECTED
    with pytest.raises(MQTTError):
        await asyncio.wait_for(receipt.wait(), 1)
    assert client.stats().receipts.publish == 0
    await asyncio.wait_for(hook_ran.wait(), 1)
    assert len(causes) == 1 and causes[0] is not None
    # The lifecycle worker that ran the hook ends on its own loop turns.
    for _ in range(100):
        if not any(client._running_tasks().values()):
            break
        await asyncio.sleep(0)
    assert not any(client._running_tasks().values())
