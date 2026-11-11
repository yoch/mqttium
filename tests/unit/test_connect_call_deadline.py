"""An explicit connect timeout bounds the whole call, retries included."""

import asyncio
import time

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.errors import MQTTTimeoutError
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport


def _refusing_factory(attempts: list[float]):
    async def factory(host, port, *, ssl=None):
        attempts.append(time.monotonic())
        raise OSError("refused")

    return factory


async def test_explicit_timeout_ends_a_retrying_connect():
    attempts: list[float] = []
    client = AsyncClient(
        "deadline", keepalive=0, reconnect=ReconnectPolicy(initial_delay=0.02, max_delay=0.02)
    )
    client._transport_factory = _refusing_factory(attempts)
    started = time.monotonic()
    with pytest.raises(MQTTTimeoutError) as raised:
        await asyncio.wait_for(client.connect("unused", timeout=0.2), 5)
    elapsed = time.monotonic() - started
    assert 0.15 <= elapsed < 2.0
    assert len(attempts) > 1
    assert isinstance(raised.value.__cause__, OSError)
    assert client.state is not ConnectionState.CONNECTED
    assert not any(client._running_tasks().values())
    # Nothing keeps retrying after the call gave up.
    seen = len(attempts)
    await asyncio.sleep(0.1)
    assert len(attempts) == seen


async def test_default_timeout_still_lets_the_policy_wait_for_a_late_broker():
    attempts: list[float] = []
    transport = ScriptedBrokerTransport()

    async def factory(host, port, *, ssl=None):
        attempts.append(time.monotonic())
        if len(attempts) < 4:
            raise OSError("not yet")
        return transport

    client = AsyncClient(
        "late-broker",
        keepalive=0,
        connect_timeout=0.05,
        reconnect=ReconnectPolicy(initial_delay=0.05, max_delay=0.05),
    )
    client._transport_factory = factory
    # Three failures and their backoff exceed connect_timeout: it is a
    # per-attempt bound when the call names no timeout of its own.
    await asyncio.wait_for(client.connect("unused"), 5)
    assert client.is_connected
    assert len(attempts) == 4
    await client.disconnect()


async def test_explicit_timeout_without_policy_is_one_attempt():
    attempts: list[float] = []
    client = AsyncClient("single", keepalive=0)
    client._transport_factory = _refusing_factory(attempts)
    with pytest.raises(OSError):
        await client.connect("unused", timeout=1.0)
    assert len(attempts) == 1
