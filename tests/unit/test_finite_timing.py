"""Non-finite timing must be refused before it can disable liveness."""

import asyncio

import pytest

from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.enums import ConnectionState
from tests.support import ScriptedBrokerTransport


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize(
    "name",
    [
        "connect_timeout",
        "ping_timeout",
        "subscribe_timeout",
        "auth_timeout",
        "iterator_admission_timeout",
    ],
)
def test_client_rejects_nonfinite_timing(name, value):
    with pytest.raises(ValueError, match=name):
        AsyncClient(**{name: value})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize("name", ["initial_delay", "multiplier", "max_delay", "stable_after"])
def test_policy_rejects_nonfinite_timing(name, value):
    with pytest.raises(ValueError, match=name):
        ReconnectPolicy(**{name: value})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), 0, -1])
@pytest.mark.parametrize(
    "operation", ["connect", "connect_unix", "connect_ws", "subscribe", "unsubscribe"]
)
async def test_invalid_override_does_not_mutate_client(operation, value):
    client = AsyncClient("finite-timing")
    with pytest.raises(ValueError, match="timeout"):
        await getattr(client, operation)("unused", timeout=value)
    assert client.state is ConnectionState.NEW
    assert client._transport is None
    assert not client._sub_futs and not client._unsub_futs
    # Refused connection attempts must not freeze callback configuration.
    client.message_callback_add("still/configurable", lambda message: None)


def test_documented_zero_and_none_sentinels_are_preserved():
    AsyncClient(keepalive=0, ping_timeout=None, iterator_admission_timeout=None)
    ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=0, multiplier=1, max_retries=None)
    # Integer resource bounds are not converted to floats by timeout validation.
    AsyncClient(max_write_queue_bytes=10**400)


_BAD_OVERRIDES = [float("nan"), float("inf"), -float("inf"), 0, -1]
_OPERATIONS = ["connect", "connect_unix", "connect_ws", "subscribe", "unsubscribe"]


def _endpoint(client):
    return (
        client._host,
        client._port,
        client._ssl,
        client._unix_path,
        client._ws_url,
        client._ws_headers,
        client._transport_factory,
    )


class _CountingFactory:
    def __init__(self, transport, gate=None):
        self.transport = transport
        self.gate = gate
        self.entered = asyncio.Event()
        self.calls = 0

    async def __call__(self, *args, **kwargs):
        self.calls += 1
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        return self.transport


@pytest.mark.parametrize("value", _BAD_OVERRIDES)
@pytest.mark.parametrize("operation", _OPERATIONS)
async def test_invalid_override_leaves_a_connected_client_intact(operation, value):
    transport = ScriptedBrokerTransport()
    factory = _CountingFactory(transport)
    client = AsyncClient("finite-connected", keepalive=0)
    client._transport_factory = factory
    await client.connect("broker", 1884)
    try:
        endpoint = _endpoint(client)
        identifiers = len(client._engine.outbound.packet_ids)
        written = len(transport.written)
        with pytest.raises(ValueError, match="timeout"):
            await getattr(client, operation)("unused", timeout=value)
        assert client.is_connected
        assert client._transport is transport
        assert client._explicit_connect_task is None
        assert _endpoint(client) == endpoint
        assert factory.calls == 1
        assert len(client._engine.outbound.packet_ids) == identifiers
        assert len(transport.written) == written
        assert not client._sub_futs and not client._unsub_futs
        # The legitimate connection still carries requests.
        assert (await client.subscribe("still/works", timeout=1)).mid > 0
    finally:
        await client.disconnect()


@pytest.mark.parametrize("value", _BAD_OVERRIDES)
@pytest.mark.parametrize("operation", _OPERATIONS)
async def test_invalid_override_does_not_disturb_an_active_attempt(operation, value):
    transport = ScriptedBrokerTransport()
    factory = _CountingFactory(transport, gate=asyncio.Event())
    client = AsyncClient("finite-attempt", keepalive=0)
    client._transport_factory = factory
    attempt = asyncio.create_task(client.connect("broker", 1884))
    try:
        await factory.entered.wait()
        endpoint = _endpoint(client)
        with pytest.raises(ValueError, match="timeout"):
            await getattr(client, operation)("unused", timeout=value)
        assert client._explicit_connect_task is attempt
        assert _endpoint(client) == endpoint
        assert factory.calls == 1
        assert not client._sub_futs and not client._unsub_futs
        factory.gate.set()
        await asyncio.wait_for(attempt, 1)
        assert client.is_connected
        assert client._transport is transport
    finally:
        factory.gate.set()
        await asyncio.gather(attempt, return_exceptions=True)
        await client.disconnect()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), 65536, -1])
def test_keepalive_must_stay_within_its_integer_range(value):
    with pytest.raises(ValueError, match="keepalive"):
        AsyncClient(keepalive=value)


@pytest.mark.parametrize("value", [0, -1, -0.5])
@pytest.mark.parametrize(
    "name", ["connect_timeout", "ping_timeout", "subscribe_timeout", "auth_timeout"]
)
def test_timeouts_must_be_positive(name, value):
    with pytest.raises(ValueError, match=name):
        AsyncClient(**{name: value})


@pytest.mark.parametrize(
    ("kwargs", "name"),
    [
        ({"initial_delay": -0.1}, "initial_delay"),
        ({"max_delay": -1}, "max_delay"),
        ({"stable_after": -1}, "stable_after"),
        ({"multiplier": 0.99}, "multiplier"),
    ],
)
def test_policy_keeps_its_specific_bounds(kwargs, name):
    with pytest.raises(ValueError, match=name):
        ReconnectPolicy(**kwargs)


def test_large_integer_durations_and_bounds_are_not_converted_to_float():
    ReconnectPolicy(initial_delay=0, max_delay=10**400, stable_after=10**400)
    AsyncClient(max_write_queue_messages=10**400, max_unacknowledged_bytes=10**400)
