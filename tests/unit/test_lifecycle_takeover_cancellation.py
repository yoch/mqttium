"""Lifecycle calls that overlap, time out or are cancelled leave a usable client.

Regression tests from the second review of the teardown changes: each
scenario below produced a client that was connected without a keepalive or a
message stream, a lost deadline, or a state nothing would ever leave.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport


class _SilentPublish(ScriptedBrokerTransport):
    """Never acknowledges a PUBLISH; ``close()`` takes a moment, as TLS does."""

    close_delay = 0.0

    def handle_packet(self, raw) -> None:
        if raw.packet_type is PacketType.PUBLISH:
            return
        super().handle_packet(raw)

    async def close(self) -> None:
        self._closing = True
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        await super().close()


@pytest.mark.parametrize("turns", range(24))
async def test_connect_overlapping_a_disconnect_yields_a_whole_connection(turns: int) -> None:
    """``disconnect()`` must not finish its shutdown on the connection a
    concurrent ``connect()`` has just installed."""
    transports: list[ScriptedBrokerTransport] = []

    async def factory(host, port, *, ssl=None):
        transports.append(_SilentPublish())
        return transports[-1]

    client = AsyncClient("overlap", keepalive=5, clean_start=False)
    client._transport_factory = factory
    await client.connect("unused")
    shutdown = asyncio.create_task(client.disconnect())
    for _ in range(turns):
        await asyncio.sleep(0)
    connecting = asyncio.create_task(client.connect("unused"))
    await asyncio.wait_for(asyncio.gather(shutdown, connecting, return_exceptions=True), 5)
    await asyncio.sleep(0.05)
    try:
        if client.is_connected:
            assert not client._delivery.closed.is_set(), "messages() ended on a live connection"
            assert client._running_tasks()["keepalive"], "no keepalive on a live connection"
            assert not client._teardown_final
            assert client._transport is not None and not client._transport.is_closing()
    finally:
        await asyncio.wait_for(client.disconnect(), 3)


async def _automatically_reconnected(close_delay: float = 0.0):
    """A client on its second, automatically established connection, whose
    reconnect supervisor is still watching it."""
    transports: list[_SilentPublish] = []

    async def factory(host, port, *, ssl=None):
        transport = _SilentPublish()
        transport.close_delay = close_delay
        transports.append(transport)
        return transport

    client = AsyncClient(
        "takeover",
        keepalive=0,
        clean_start=False,
        connect_timeout=5.0,
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01, stable_after=30),
    )
    client._transport_factory = factory
    await client.connect("unused")
    transports[0].push_rx(b"")
    for _ in range(400):
        if len(transports) == 2 and client.is_connected:
            break
        await asyncio.sleep(0.005)
    assert client.is_connected and client._running_tasks()["reconnect"]
    return client, transports


@pytest.mark.parametrize("call", ["connect", "disconnect"])
async def test_a_deadline_expiring_during_a_takeover_is_not_lost(call: str) -> None:
    """The deadline expires while the old reader is being joined; the call
    used to absorb it and run to completion."""
    client, _transports = await _automatically_reconnected(close_delay=0.2)
    try:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.1):
                await (client.connect("unused") if call == "connect" else client.disconnect())
    finally:
        await asyncio.wait_for(client.disconnect(), 3)
    assert client.state is ConnectionState.DISCONNECTED


@pytest.mark.parametrize("turns", [1, 2])
async def test_connect_cancelled_before_replacing_a_live_connection_leaves_it_whole(
    turns: int,
) -> None:
    """Nothing was torn down yet: the live connection keeps its receipts and
    still reconnects after a later loss."""
    client, _transports = await _automatically_reconnected()
    receipt = await client.publish("topic", b"payload", qos=1)
    task = asyncio.create_task(client.connect("unused"))
    for _ in range(turns):
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.wait({task}, timeout=2)
    await asyncio.sleep(0.05)
    try:
        if client.is_connected:
            assert not receipt.is_done()
            assert not client._intentional_disconnect
    finally:
        await asyncio.wait_for(client.disconnect(), 3)


async def test_connect_cancelled_between_attempts_is_not_left_reconnecting() -> None:
    async def factory(host, port, *, ssl=None):
        raise OSError("broker down")

    client = AsyncClient(
        "backoff",
        keepalive=0,
        clean_start=False,
        reconnect=ReconnectPolicy(initial_delay=0.2, max_delay=0.2),
    )
    client._transport_factory = factory
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.3):  # expires inside the wait between attempts
            await client.connect("unused")
    await asyncio.sleep(0.5)
    assert client.state is not ConnectionState.RECONNECTING
    assert not client._reconnect_pending
    assert not any(client._running_tasks().values())
    await client.disconnect()
