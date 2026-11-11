"""A caller cancelled inside disconnect() or connect() stays cancelled.

Both join the reconnect task they cancel. That join raises ``CancelledError``
as the joined task's outcome, and also when the caller itself is cancelled;
absorbing it unconditionally lost the caller's cancellation and broke
``asyncio.timeout()`` and ``TaskGroup`` semantics.
"""

import asyncio

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport


async def _client_in_reconnect_backoff() -> AsyncClient:
    transport = ScriptedBrokerTransport()
    client = AsyncClient(
        "join-cancel",
        keepalive=0,
        reconnect=ReconnectPolicy(initial_delay=30.0, max_delay=30.0),
    )
    attempts = 0

    async def factory(host, port, *, ssl=None):
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            raise OSError("down")
        return transport

    client._transport_factory = factory
    await client.connect("unused")
    transport.push_rx(b"")
    for _ in range(200):
        if client._running_tasks()["reconnect"]:
            break
        await asyncio.sleep(0.005)
    assert client._running_tasks()["reconnect"]
    return client


async def test_cancelling_disconnect_during_reconnect_join_propagates_and_finishes():
    client = await _client_in_reconnect_backoff()
    after_disconnect: list[str] = []

    async def application():
        await client.disconnect()
        after_disconnect.append("ran")
        await asyncio.Event().wait()

    task = asyncio.create_task(application())
    await asyncio.sleep(0)  # inside disconnect(), joining the reconnect task
    task.cancel()
    await asyncio.wait({task}, timeout=1)

    assert task.cancelled()
    assert after_disconnect == []
    # The committed shutdown still completed.
    assert client.state is ConnectionState.DISCONNECTED
    assert not any(client._running_tasks().values())
    await client.disconnect()


async def test_disconnect_join_without_caller_cancellation_returns_normally():
    client = await _client_in_reconnect_backoff()
    await asyncio.wait_for(client.disconnect(), 1)
    assert client.state is ConnectionState.DISCONNECTED
    assert not any(client._running_tasks().values())
