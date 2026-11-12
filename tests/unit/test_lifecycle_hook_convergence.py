"""Lifecycle hooks converge on the client's final state.

``on_connect`` and ``on_disconnect`` describe the latest state, not every
transition: a pending notification may be replaced by a newer one. Two things
still hold once lifecycle calls have returned and hooks have run:

- the last hook delivered matches the state: a client that announced
  ``on_connect`` and is no longer connected has received ``on_disconnect``;
- ``on_disconnect(None)`` means an established connection was closed at the
  application's request. A connection that never got its CONNACK, or that was
  lost, is reported with an exception.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.errors import MQTTError, MQTTTimeoutError
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport


class _Broker(ScriptedBrokerTransport):
    """Acknowledges CONNECT unless told to stay silent."""

    def __init__(self, *, silent: bool = False) -> None:
        super().__init__()
        self.silent = silent

    def handle_packet(self, raw) -> None:
        if self.silent and raw.packet_type is PacketType.CONNECT:
            return
        super().handle_packet(raw)


class _World:
    def __init__(self) -> None:
        self.transports: list[_Broker] = []
        self.hang = False
        self.silent = False
        self.hooks: list[tuple[str, BaseException | None]] = []

    async def factory(self, host, port, *, ssl=None):
        if self.hang:
            await asyncio.sleep(3600)
        transport = _Broker(silent=self.silent)
        self.transports.append(transport)
        return transport

    def client(self, **kwargs) -> AsyncClient:
        client = AsyncClient("convergence", keepalive=0, clean_start=False, **kwargs)
        client._transport_factory = self.factory

        async def on_connect(packet) -> None:
            self.hooks.append(("C", None))

        async def on_disconnect(cause) -> None:
            self.hooks.append(("D", cause))

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        return client


async def _settled(client: AsyncClient) -> None:
    """Let every pending notification run."""
    for _ in range(100):
        await asyncio.sleep(0.01)
        hooks = client._lifecycle_hooks
        if hooks.pending is None and (hooks.task is None or hooks.task.done()):
            return
    raise AssertionError("lifecycle notifications never settled")


def _assert_converged(world: _World, client: AsyncClient) -> None:
    if not world.hooks:
        return
    last = world.hooks[-1][0]
    expected = "C" if client.is_connected else "D"
    assert last == expected, (world.hooks, client.state)


async def _automatically_reconnected(world: _World) -> AsyncClient:
    client = world.client(
        connect_timeout=5.0,
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01, stable_after=30),
    )
    await client.connect("unused")
    world.transports[0].push_rx(b"")
    for _ in range(400):
        if len(world.transports) == 2 and client.is_connected:
            break
        await asyncio.sleep(0.005)
    assert client.is_connected
    await _settled(client)
    assert world.hooks[-1][0] == "C"
    return client


async def test_a_takeover_that_times_out_ends_with_on_disconnect() -> None:
    world = _World()
    client = await _automatically_reconnected(world)
    world.hang = True
    try:
        with pytest.raises(MQTTTimeoutError):
            await client.connect("unused", timeout=0.05)
        await _settled(client)
        assert client.state is ConnectionState.DISCONNECTED
        _assert_converged(world, client)
    finally:
        await asyncio.wait_for(client.disconnect(), 3)


async def test_a_cancelled_takeover_ends_with_on_disconnect() -> None:
    world = _World()
    client = await _automatically_reconnected(world)
    world.hang = True
    try:
        task = asyncio.create_task(client.connect("unused"))
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.wait({task}, timeout=2)
        await _settled(client)
        assert not client.is_connected
        _assert_converged(world, client)
    finally:
        await asyncio.wait_for(client.disconnect(), 3)


@pytest.mark.parametrize("turns", range(16))
async def test_a_takeover_overtaken_by_disconnect_ends_with_on_disconnect(turns: int) -> None:
    world = _World()
    client = await _automatically_reconnected(world)
    connecting = asyncio.create_task(client.connect("unused"))
    for _ in range(turns):
        await asyncio.sleep(0)
    await asyncio.wait_for(client.disconnect(), 3)
    await asyncio.wait({connecting}, timeout=3)
    await _settled(client)
    assert client.state is ConnectionState.DISCONNECTED
    _assert_converged(world, client)


@pytest.mark.parametrize("turns", range(16))
async def test_a_replaced_connection_is_reported_as_requested(turns: int) -> None:
    """A connection that ``connect()`` replaces was closed at the
    application's request, whenever the takeover landed."""
    world = _World()
    client = await _automatically_reconnected(world)
    before = len(world.hooks)
    try:
        for _ in range(turns):
            await asyncio.sleep(0)
        await asyncio.wait_for(client.connect("unused"), 3)
        await _settled(client)
        assert client.is_connected
        _assert_converged(world, client)
        assert all(cause is None for kind, cause in world.hooks[before:] if kind == "D")
    finally:
        await asyncio.wait_for(client.disconnect(), 3)


@pytest.mark.parametrize("turns", range(0, 12, 2))
async def test_disconnect_during_the_handshake_is_not_a_clean_close(turns: int) -> None:
    """No CONNACK, no established connection: the report carries a cause."""
    world = _World()
    world.silent = True
    client = world.client(connect_timeout=5.0)
    connecting = asyncio.create_task(client.connect("unused"))
    for _ in range(4 + turns):
        await asyncio.sleep(0)
    await asyncio.wait_for(client.disconnect(), 3)
    await asyncio.wait({connecting}, timeout=3)
    await _settled(client)
    assert not any(kind == "C" for kind, _ in world.hooks)
    assert all(cause is not None for kind, cause in world.hooks if kind == "D"), world.hooks


async def test_a_connack_timeout_is_reported_as_a_timeout() -> None:
    world = _World()
    world.silent = True
    client = world.client(connect_timeout=0.05)
    with pytest.raises(MQTTError):
        await client.connect("unused")
    await _settled(client)
    causes = [cause for kind, cause in world.hooks if kind == "D"]
    assert causes and isinstance(causes[-1], MQTTTimeoutError), causes
