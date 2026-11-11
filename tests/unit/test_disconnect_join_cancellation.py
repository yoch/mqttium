"""A caller cancelled inside disconnect() or connect() stays cancelled.

Both join the reconnect task they cancel. That join raises ``CancelledError``
as the joined task's outcome, and also when the caller itself is cancelled;
absorbing it unconditionally lost the caller's cancellation and broke
``asyncio.timeout()`` and ``TaskGroup`` semantics.
"""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.errors import MQTTError
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


async def _connected_with_policy(delay: float = 30.0):
    transport = ScriptedBrokerTransport()
    client = AsyncClient(
        "backoff",
        keepalive=0,
        clean_start=False,
        reconnect=ReconnectPolicy(initial_delay=delay, max_delay=delay),
    )
    attempts: list[int] = []

    async def factory(host, port, *, ssl=None):
        attempts.append(1)
        if len(attempts) > 1:
            raise OSError("down")
        return transport

    client._transport_factory = factory
    await client.connect("unused")
    return client, transport, attempts


async def _lose_connection(client, transport) -> None:
    transport.push_rx(b"")
    for _ in range(400):
        if client._running_tasks()["reconnect"]:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("the reconnect task never started")


async def test_cleanup_of_an_already_cancelled_task_is_not_cancelled_again() -> None:
    """A task cancelled once runs ``finally: await client.disconnect()``. The
    join of the reconnect task raises CancelledError as that task's outcome;
    it is not a second cancellation of the caller, whose cleanup must go on."""
    client, transport, _attempts = await _connected_with_policy()
    await _lose_connection(client, transport)
    log: list[str] = []

    async def application() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            await client.disconnect()
            log.append("cleanup after disconnect")

    task = asyncio.create_task(application())
    await asyncio.sleep(0)
    task.cancel()  # exactly one cancellation
    await asyncio.wait({task}, timeout=2)

    assert client.state is ConnectionState.DISCONNECTED
    assert log == ["cleanup after disconnect"]


async def test_connect_cancelled_while_taking_over_does_not_strand_the_client() -> None:
    """connect() stops the automatic reconnect before it connects. Cancelled
    at that point it must not leave RECONNECTING with nothing reconnecting."""
    client, transport, attempts = await _connected_with_policy(delay=0.05)

    receipt = client.publish_nowait("topic", b"payload", qos=1)
    transport.handle_packet = lambda raw: None  # the PUBLISH is never acknowledged
    await _lose_connection(client, transport)

    task = asyncio.create_task(client.connect("unused"))
    await asyncio.sleep(0)  # inside connect(), joining the reconnect task
    task.cancel()
    await asyncio.wait({task}, timeout=1)
    assert task.cancelled()

    seen = len(attempts)
    await asyncio.sleep(0.3)  # several backoff periods
    assert not client._running_tasks()["reconnect"] and len(attempts) == seen
    # Nobody reconnects, so nothing may stay pending or claim to reconnect.
    assert client.state is ConnectionState.DISCONNECTED
    with pytest.raises(MQTTError):
        await asyncio.wait_for(receipt.wait(), 1)
