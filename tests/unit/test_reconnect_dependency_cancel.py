"""Reconnect dependencies may raise CancelledError without cancelling the owner task."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, wait_until


def _policy(*, max_retries: int) -> ReconnectPolicy:
    return ReconnectPolicy(
        initial_delay=0.0,
        multiplier=1.0,
        max_delay=0.0,
        max_retries=max_retries,
        stable_after=0.0,
    )


async def test_dependency_cancelled_error_uses_reconnect_retry_and_terminal_policy() -> None:
    first = ScriptedBrokerTransport()
    failure = asyncio.CancelledError("factory self-cancel")
    calls = 0
    client = AsyncClient(
        "reconnect-dependency-cancel",
        keepalive=0,
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

    client._transport_factory = factory
    await client.connect("fake")
    stream = client.messages()
    try:
        first.push_rx(b"")
        await wait_until(lambda: client._delivery.closed.is_set())
        await wait_until(lambda: client._reconnect_task is None)

        assert calls == 3  # initial connection + two retry attempts
        assert not client.is_connected
        assert client._disconnect_exc is failure
        assert client.stats().reconnect_attempt == 2
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(stream), timeout=1)
    finally:
        await stream.aclose()
        await client.disconnect()


async def test_disconnect_cancels_in_progress_reconnect_without_extra_retry() -> None:
    first = ScriptedBrokerTransport()
    reconnect_entered = asyncio.Event()
    calls = 0
    client = AsyncClient(
        "reconnect-owner-cancel",
        keepalive=0,
        reconnect=_policy(max_retries=5),
        connect_timeout=30.0,
    )

    async def factory(host: str, port: int, *, ssl: object = None):
        nonlocal calls
        del host, port, ssl
        calls += 1
        if calls == 1:
            return first
        reconnect_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("blocked reconnect factory resumed without cancellation")

    client._transport_factory = factory
    await client.connect("fake")
    try:
        first.push_rx(b"")
        await asyncio.wait_for(reconnect_entered.wait(), timeout=1)
        assert calls == 2

        await asyncio.wait_for(client.disconnect(), timeout=1)
        for _ in range(4):
            await asyncio.sleep(0)

        assert calls == 2
        assert client._reconnect_task is None
        assert not client.is_connected
        assert client._delivery.closed.is_set()
    finally:
        await client.disconnect()
