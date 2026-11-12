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
from tests.support import ScriptedBrokerTransport, wait_until


class _Broker(ScriptedBrokerTransport):
    """Acknowledges CONNECT unless told to stay silent; ``close()`` can take
    a moment, as TLS does."""

    close_delay = 0.0

    def __init__(self, *, silent: bool = False) -> None:
        super().__init__()
        self.silent = silent

    def handle_packet(self, raw) -> None:
        if self.silent and raw.packet_type is PacketType.CONNECT:
            return
        super().handle_packet(raw)

    async def close(self) -> None:
        if self.close_delay:
            await asyncio.sleep(self.close_delay)
        await super().close()


class _World:
    def __init__(self) -> None:
        self.transports: list[_Broker] = []
        self.hang = False
        self.refuse = False
        self.silent = False
        self.hooks: list[tuple[str, BaseException | None]] = []

    async def factory(self, host, port, *, ssl=None):
        if self.hang:
            await asyncio.sleep(3600)
        if self.refuse:
            raise OSError("connection refused")
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


@pytest.mark.parametrize("turns", range(20))
async def test_a_loss_then_a_failed_takeover_ends_with_the_loss_report(turns: int) -> None:
    """The loss's on_disconnect may have been taken by the worker, but not
    started, when the takeover supersedes it: that hook told the
    application nothing, so the failed takeover must still report the loss."""
    world = _World()
    client = await _automatically_reconnected(world)
    before = len(world.hooks)
    world.hang = True
    world.transports[-1].push_rx(b"")
    for _ in range(turns):
        await asyncio.sleep(0)
    try:
        with pytest.raises(MQTTError):
            await client.connect("unused", timeout=0.05)
        await _settled(client)
        assert client.state is ConnectionState.DISCONNECTED
        _assert_converged(world, client)
        reports = [cause for kind, cause in world.hooks[before:] if kind == "D"]
        assert reports and all(cause is not None for cause in reports), world.hooks
    finally:
        world.hang = False
        await asyncio.wait_for(client.disconnect(), 3)


async def test_a_replaced_connection_is_reported_with_a_cause() -> None:
    """A takeover that never gets a connection reports the connection it
    replaced; only disconnect() reports None."""
    world = _World()
    client = await _automatically_reconnected(world)
    before = len(world.hooks)
    world.hang = True
    try:
        with pytest.raises(MQTTError):
            await client.connect("unused", timeout=0.05)
        await _settled(client)
        reports = [cause for kind, cause in world.hooks[before:] if kind == "D"]
        assert reports and all(cause is not None for cause in reports), world.hooks
    finally:
        world.hang = False
        await asyncio.wait_for(client.disconnect(), 3)


@pytest.mark.parametrize("turns", range(12))
async def test_a_connect_cancelled_before_its_takeover_reports_nothing_more(turns: int) -> None:
    """Cancelled before it replaced anything, connect() superseded no
    notification: the loss is reported once, by its own reader."""
    world = _World()
    client = await _automatically_reconnected(world)
    before = len(world.hooks)
    for transport in world.transports:
        transport.close_delay = 0.05
    world.transports[-1].push_rx(b"")
    connecting = asyncio.create_task(client.connect("unused"))
    for _ in range(turns):
        await asyncio.sleep(0)
    connecting.cancel()
    await asyncio.wait({connecting}, timeout=3)
    try:
        await _settled(client)
        reports = [cause for kind, cause in world.hooks[before:] if kind == "D"]
        assert all(cause is not None for cause in reports), world.hooks
    finally:
        await asyncio.wait_for(client.disconnect(), 3)


@pytest.mark.parametrize("failure", ["hang", "refuse"])
async def test_a_hook_awaiting_its_own_takeover_receives_the_failure(failure: str) -> None:
    """Caller preservation: the on_connect that awaits connect() gets that
    connect's failure and is not cancelled by the report that follows."""
    world = _World()
    log: list[str] = []
    client = world.client(
        connect_timeout=5.0,
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01, stable_after=30),
    )
    connects = 0

    async def on_connect(packet) -> None:
        nonlocal connects
        connects += 1
        world.hooks.append(("C", None))
        if connects != 2:
            return
        if failure == "hang":
            world.hang = True
        else:
            world.refuse = True
        try:
            await client.connect("unused", timeout=0.05)
        except MQTTError:
            log.append("failed")
        try:
            await asyncio.sleep(0.01)
            log.append("finished")
        except asyncio.CancelledError:
            log.append("cancelled")
            raise

    client.on_connect = on_connect
    await client.connect("unused")
    await _settled(client)
    world.transports[0].push_rx(b"")
    await wait_until(lambda: "finished" in log or "cancelled" in log, timeout=3)
    world.hang = False
    world.refuse = False
    await asyncio.wait_for(client.disconnect(), 3)
    assert log == ["failed", "finished"]


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
    reports = [cause for kind, cause in world.hooks if kind == "D"]
    assert reports and all(cause is not None for cause in reports), world.hooks


async def test_a_connack_timeout_is_reported_as_a_timeout() -> None:
    world = _World()
    world.silent = True
    client = world.client(connect_timeout=0.05)
    with pytest.raises(MQTTError):
        await client.connect("unused")
    await _settled(client)
    causes = [cause for kind, cause in world.hooks if kind == "D"]
    assert causes and isinstance(causes[-1], MQTTTimeoutError), causes


@pytest.mark.parametrize("turns", range(40))
async def test_disconnect_after_a_loss_keeps_the_loss_report(turns: int) -> None:
    """``disconnect()`` supersedes a pending ``on_connect``, never the
    pending report of a loss that already happened."""
    world = _World()
    client = world.client()
    await client.connect("unused")
    await _settled(client)
    world.transports[0].push_rx(b"")
    for _ in range(turns):
        await asyncio.sleep(0)
    await asyncio.wait_for(client.disconnect(), 3)
    await _settled(client)
    assert client.state is ConnectionState.DISCONNECTED
    _assert_converged(world, client)


@pytest.mark.parametrize("turns", range(1, 40))
async def test_a_second_disconnect_keeps_the_first_report(turns: int) -> None:
    """The first ``disconnect()`` is cancelled part-way; the second one,
    made from its cleanup, must not drop the report of the closed connection."""
    world = _World()
    client = world.client()
    await client.connect("unused")
    await _settled(client)

    async def disconnect_twice() -> None:
        try:
            await client.disconnect()
            await asyncio.sleep(3600)
        finally:
            await client.disconnect()

    task = asyncio.create_task(disconnect_twice())
    for _ in range(turns):
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.wait({task}, timeout=3)
    await _settled(client)
    assert client.state is ConnectionState.DISCONNECTED
    _assert_converged(world, client)
