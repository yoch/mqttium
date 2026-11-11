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
