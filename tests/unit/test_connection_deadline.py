"""Connection stages spend one budget, including WebSocket upgrade."""

import asyncio

import pytest

from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.api import async_client
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


class _Clock:
    """Advance the loop's monotonic clock instead of sleeping through budgets."""

    def __init__(self, loop):
        self._real = loop.time
        self.offset = 0.0

    def time(self):
        return self._real() + self.offset


@pytest.fixture
async def clock(monkeypatch):
    loop = asyncio.get_running_loop()
    fake = _Clock(loop)
    monkeypatch.setattr(loop, "time", fake.time)
    return fake


class _SlowConnackBroker(ScriptedBrokerTransport):
    """Spend `connack_cost` of loop time before answering CONNECT."""

    def __init__(self, clock, connack_cost=0.0, answer=True):
        super().__init__()
        self.clock = clock
        self.connack_cost = connack_cost
        self.answer = answer
        self.connect_seen = asyncio.Event()

    def handle_packet(self, raw):
        if raw.packet_type is not PacketType.CONNECT:
            super().handle_packet(raw)
            return
        self.connect_seen.set()
        if not self.answer:
            return
        self.clock.offset += self.connack_cost
        # Deliver after every timer that the elapsed cost made due.
        asyncio.get_running_loop().call_later(0, super().handle_packet, raw)


def _factory(clock, transports, setup_cost):
    async def factory(*args, **kwargs):
        clock.offset += setup_cost
        return transports.pop(0)

    return factory


async def _assert_released(client, *transports):
    # Lifecycle hooks run after transport cleanup; they must finish, not linger.
    await wait_until(lambda: not any(client._running_tasks().values()))
    assert client._connack_fut is None or client._connack_fut.done()
    assert client._connect_disconnect_fut is None
    for transport in transports:
        assert transport.is_closing()


async def _connect(client, route, timeout):
    if route == "tcp":
        return await client.connect("unused", timeout=timeout)
    if route == "unix":
        return await client.connect_unix("/unused.sock", timeout=timeout)
    return await client.connect_ws("ws://unused/mqtt", timeout=timeout)


def _install(monkeypatch, client, route, factory):
    if route == "tcp":
        client._transport_factory = factory
    elif route == "unix":
        monkeypatch.setattr(async_client.UnixSocketTransport, "connect", factory)
    else:

        async def ws_connect(url, *, ssl=None, extra_headers=None, timeout=30.0):
            # AsyncClient owns the whole attempt; no inner WebSocket deadline.
            assert timeout is None
            return await factory(url)

        monkeypatch.setattr(async_client.WebSocketTransport, "connect", ws_connect)


@pytest.mark.parametrize("route", ["tcp", "unix", "ws"])
@pytest.mark.parametrize("override", [None, 60])
async def test_setup_and_connack_each_shorter_than_budget_but_not_together(
    clock, monkeypatch, route, override
):
    broker = _SlowConnackBroker(clock, connack_cost=40)
    client = AsyncClient("deadline-sum", connect_timeout=60, keepalive=0)
    _install(monkeypatch, client, route, _factory(clock, [broker], setup_cost=40))
    try:
        with pytest.raises(MQTTTimeoutError, match="CONNACK"):
            await _connect(client, route, override)
        assert not client.is_connected
        await _assert_released(client, broker)
    finally:
        await client.disconnect()


@pytest.mark.parametrize("route", ["tcp", "unix", "ws"])
@pytest.mark.parametrize("override", [None, 60])
async def test_setup_and_connack_within_budget_connect(clock, monkeypatch, route, override):
    broker = _SlowConnackBroker(clock, connack_cost=25)
    client = AsyncClient("deadline-ok", connect_timeout=60, keepalive=0)
    _install(monkeypatch, client, route, _factory(clock, [broker], setup_cost=25))
    try:
        await _connect(client, route, override)
        assert client.is_connected
    finally:
        await client.disconnect()


async def test_budget_already_spent_by_setup_times_out_despite_prompt_connack(clock):
    broker = _SlowConnackBroker(clock)
    client = AsyncClient("deadline-spent", connect_timeout=60, keepalive=0)
    client._transport_factory = _factory(clock, [broker], setup_cost=60)
    try:
        with pytest.raises(MQTTTimeoutError, match="CONNACK"):
            await client.connect("unused")
        await _assert_released(client, broker)
    finally:
        await client.disconnect()


async def test_automatic_reconnect_attempt_uses_one_deadline(clock):
    first = _SlowConnackBroker(clock)
    slow = _SlowConnackBroker(clock, connack_cost=40)
    last = _SlowConnackBroker(clock)
    costs = [0, 40, 0]
    transports = [first, slow, last]

    async def factory(*args, **kwargs):
        clock.offset += costs.pop(0)
        return transports.pop(0)

    client = AsyncClient(
        "deadline-reconnect",
        connect_timeout=60,
        keepalive=0,
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=3600),
    )
    client._transport_factory = factory
    await client.connect("unused")
    try:
        first.push_rx(b"")
        # asyncio.timeout() reads the advanced clock; bound by loop turns instead.
        for _ in range(10_000):
            if client._transport is last and client.is_connected:
                break
            await asyncio.sleep(0)
        assert client._transport is last and client.is_connected
        assert slow.connect_seen.is_set()
        assert slow.is_closing()
        assert not transports
    finally:
        await client.disconnect()


@pytest.mark.parametrize("phase", ["setup", "connack"])
async def test_cancellation_releases_every_attempt_resource(phase):
    gate = asyncio.Event()
    entered = asyncio.Event()
    broker = _SlowConnackBroker(None, answer=False)

    async def factory(*args, **kwargs):
        entered.set()
        if phase == "setup":
            await gate.wait()
        return broker

    client = AsyncClient("deadline-cancel", connect_timeout=60, keepalive=0)
    client._transport_factory = factory
    attempt = asyncio.create_task(client.connect("unused"))
    try:
        await (entered.wait() if phase == "setup" else broker.connect_seen.wait())
        attempt.cancel()
        with pytest.raises(asyncio.CancelledError):
            await attempt
        assert not client.is_connected
        await _assert_released(client, *([broker] if phase == "connack" else []))
        # The client remains usable for a fresh attempt.
        retry = ScriptedBrokerTransport()
        client._transport_factory = lambda *args, **kwargs: _ready(retry)
        await client.connect("unused", timeout=1)
        assert client.is_connected
    finally:
        gate.set()
        await asyncio.gather(attempt, return_exceptions=True)
        await client.disconnect()


async def _ready(transport):
    return transport


async def test_cancellation_during_public_websocket_upgrade_closes_the_socket(monkeypatch):
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
    upgrading = asyncio.Event()

    async def open_connection(*args, **kwargs):
        return asyncio.StreamReader(), writer

    async def read_upgrade(reader, timeout):
        assert timeout is None
        upgrading.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    monkeypatch.setattr(websocket, "_read_handshake_response", read_upgrade)
    client = AsyncClient("websocket-cancel", connect_timeout=60, keepalive=0)
    attempt = asyncio.create_task(client.connect_ws("ws://unused/mqtt"))
    await upgrading.wait()
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    assert writer.closed
    await _assert_released(client)
    await client.disconnect()


async def test_direct_websocket_transport_keeps_its_default_upgrade_timeout(monkeypatch):
    seen = []

    async def open_connection(*args, **kwargs):
        raise OSError("refused")

    async def fake_wait_for(awaitable, timeout):
        seen.append(timeout)
        return await awaitable

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)
    with pytest.raises(OSError, match="refused"):
        await websocket.WebSocketTransport.connect("ws://unused/mqtt")
    assert seen == [30.0]
