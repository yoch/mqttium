"""Lifecycle hook completion must not gate automatic transport recovery."""

from __future__ import annotations

import asyncio

from mqttium.api import AsyncClient
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, wait_until


def _policy(*, max_retries: int | None = None) -> ReconnectPolicy:
    return ReconnectPolicy(
        initial_delay=0.0,
        multiplier=1.0,
        max_delay=0.0,
        max_retries=max_retries,
        stable_after=0.0,
    )


async def test_disconnect_hook_can_wait_for_receipt_completed_by_reconnect() -> None:
    brokers = [ScriptedBrokerTransport(), ScriptedBrokerTransport()]
    calls = 0
    events: list[str] = []
    disconnected_done = asyncio.Event()
    reconnected_hook = asyncio.Event()
    client = AsyncClient(
        "disconnect-hook-receipt",
        keepalive=0,
        clean_start=False,
        reconnect=_policy(),
    )

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        del host, port, ssl
        broker = brokers[min(calls, 1)]
        calls += 1
        return broker

    async def on_disconnect(_error: BaseException | None) -> None:
        events.append("disconnect-enter")
        receipt = await client.publish("offline/hook", b"x", qos=1)
        events.append("receipt-obtained")
        await receipt.wait()
        events.append("disconnect-exit")
        disconnected_done.set()

    def on_connect(_packet: object) -> None:
        events.append("connect")
        reconnected_hook.set()

    client._transport_factory = factory
    await client.connect("fake")
    client.on_disconnect = on_disconnect
    client.on_connect = on_connect
    try:
        await brokers[0].close()

        await asyncio.wait_for(disconnected_done.wait(), timeout=1)
        await asyncio.wait_for(reconnected_hook.wait(), timeout=1)

        assert calls == 2
        assert len(brokers[1].publishes) == 1
        assert events == [
            "disconnect-enter",
            "receipt-obtained",
            "disconnect-exit",
            "connect",
        ]
        assert client.is_connected
    finally:
        await client.disconnect()


async def test_reconnect_exhaustion_settles_receipt_and_unblocks_disconnect_hook() -> None:
    first = ScriptedBrokerTransport()
    failure = ConnectionRefusedError("replacement broker unavailable")
    calls = 0
    hook_done = asyncio.Event()
    observed: list[BaseException] = []
    client = AsyncClient(
        "disconnect-hook-exhaustion",
        keepalive=0,
        clean_start=False,
        reconnect=_policy(max_retries=2),
        connect_timeout=0.1,
    )

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        del host, port, ssl
        calls += 1
        if calls == 1:
            return first
        raise failure

    async def on_disconnect(_error: BaseException | None) -> None:
        receipt = await client.publish("offline/exhaustion", b"x", qos=1)
        try:
            await receipt.wait()
        except BaseException as exc:
            observed.append(exc)
        hook_done.set()

    client._transport_factory = factory
    await client.connect("fake")
    client.on_disconnect = on_disconnect
    try:
        await first.close()

        await asyncio.wait_for(hook_done.wait(), timeout=1)
        await wait_until(lambda: client._reconnect_task is None)
        await wait_until(lambda: client._lifecycle_hooks.hook_task is None)

        assert calls == 3  # initial connection + two allowed retry attempts
        assert observed == [failure]
        assert not client.is_connected
        assert client._delivery.closed.is_set()
        assert client._disconnect_exc is failure
    finally:
        await client.disconnect()
