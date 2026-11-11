"""An unexpected keepalive failure retires the connection.

Without the keepalive loop nothing detects a dead peer, so its failure must
never leave a connection that still looks alive.
"""

import asyncio

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from tests.support import ScriptedBrokerTransport, transport_factory


async def test_keepalive_failure_ends_the_connection_with_its_cause():
    transport = ScriptedBrokerTransport()
    client = AsyncClient("keepalive-failure", keepalive=1)
    client._transport_factory = transport_factory(transport)
    causes: list[BaseException | None] = []
    hook_ran = asyncio.Event()

    async def on_disconnect(cause):
        causes.append(cause)
        hook_ran.set()

    client.on_disconnect = on_disconnect
    failure = RuntimeError("keepalive bug")

    def broken_ping():
        raise failure

    await client.connect("unused")
    client._engine.queue_ping = broken_ping
    await asyncio.wait_for(hook_ran.wait(), 5)

    assert causes == [failure]
    assert client.state is ConnectionState.DISCONNECTED
    # The lifecycle worker that ran the hook ends on its own loop turns.
    for _ in range(100):
        if not any(client._running_tasks().values()):
            break
        await asyncio.sleep(0)
    assert not any(client._running_tasks().values())


class _SlowWrite(ScriptedBrokerTransport):
    slow = False

    async def write(self, data: bytes) -> None:
        if self.slow:
            await asyncio.sleep(1.5)  # a congested socket
        await super().write(data)


async def test_a_ping_due_during_a_graceful_disconnect_does_not_abort_it() -> None:
    """While DISCONNECT drains, the engine is no longer CONNECTED and a ping
    cannot be queued. That is the shutdown in progress, not a keepalive
    failure: the DISCONNECT must still be written and the cause stay clean."""
    transport = _SlowWrite()
    client = AsyncClient("graceful", keepalive=1)
    client._transport_factory = transport_factory(transport)
    causes: list[BaseException | None] = []
    ran = asyncio.Event()

    async def on_disconnect(cause: BaseException | None) -> None:
        causes.append(cause)
        ran.set()

    client.on_disconnect = on_disconnect
    await client.connect("unused")
    transport.slow = True
    await asyncio.wait_for(client.disconnect(), 10)
    await asyncio.wait_for(ran.wait(), 2)

    assert causes == [None]
    assert b"\xe0\x00" in transport.written
