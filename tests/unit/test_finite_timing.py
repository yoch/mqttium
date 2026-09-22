"""Non-finite timing must be refused before it can disable liveness."""

import pytest

from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.enums import ConnectionState


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
