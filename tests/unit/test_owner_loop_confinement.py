"""Awaitable entry points refuse a call from another event loop.

The client is confined to the loop that connected it. Only ``connect`` and
``publish_nowait`` used to check; the others ran on the foreign loop and
raced the owner's.
"""

import asyncio
import threading

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory

_CALLS = {
    "publish": lambda c: c.publish("t", b"x"),
    "publish_many": lambda c: c.publish_many([PublishMessage("t", b"x")]),
    "subscribe": lambda c: c.subscribe("t"),
    "unsubscribe": lambda c: c.unsubscribe("t"),
    "ack": lambda c: c.ack(Message(topic="t", payload=b"")),
    "disconnect": lambda c: c.disconnect(),
}


def _run_on_another_loop(make_call) -> BaseException | None:
    outcome: list[BaseException | None] = []

    def target():
        async def call():
            await make_call()

        try:
            asyncio.run(call())
        except BaseException as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    thread = threading.Thread(target=target)
    thread.start()
    thread.join(5)
    assert not thread.is_alive()
    return outcome[0]


@pytest.mark.parametrize("name", sorted(_CALLS))
async def test_call_from_another_loop_is_refused(name):
    client = AsyncClient("confined", keepalive=0)
    client._transport_factory = transport_factory(ScriptedBrokerTransport())
    await client.connect("unused")
    try:
        outcome = await asyncio.to_thread(_run_on_another_loop, lambda: _CALLS[name](client))
        assert isinstance(outcome, RuntimeError)
        assert "different event loop" in str(outcome)
        assert client.is_connected
    finally:
        await client.disconnect()


async def test_an_unconnected_client_is_not_bound_yet():
    client = AsyncClient("unbound", keepalive=0)
    await client.disconnect()  # no owner loop yet: nothing to refuse


def test_a_late_disconnect_after_the_owner_loop_closed_is_harmless() -> None:
    """Idempotent cleanup in a second ``asyncio.run``: the loop that owned the
    client is closed and runs nothing, so there is no one to race."""
    client = AsyncClient("late")
    client._transport_factory = transport_factory(ScriptedBrokerTransport())

    async def session() -> None:
        await client.connect("unused")
        await client.disconnect()

    asyncio.run(session())
    asyncio.run(client.disconnect())
