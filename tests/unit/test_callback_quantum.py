"""Synchronous callback invocations, including route fan-out, share a bounded quantum.

The delivering reader runs callbacks directly. ``accept()`` hands back a
cooperative yield once the private quantum of invocations is reached, and the
reader-owned lane awaits it between messages, so a long incoming lot cannot
monopolise the loop without a scheduling point.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from mqttium.api import AsyncClient
from mqttium.api._delivery import ApplicationDelivery, _CALLBACK_QUANTUM
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory


def _delivery():
    return ApplicationDelivery(
        mode="callback",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_pending_messages=2048,
        max_pending_delivery_bytes=65536,
        delivery_timeout=1,
    )


async def _settle(pending) -> None:
    if pending is not None:
        assert inspect.isawaitable(pending)
        await pending


@pytest.mark.parametrize("kind", ["sync", "error", "cancelled"])
async def test_accept_runs_inline_and_yields_exactly_at_the_quantum(kind):
    delivery = _delivery()
    seen, errors = [], []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context["exception"]))

    def callback(message):
        seen.append(int(message.payload))
        if len(seen) == 63 and kind == "error":
            raise ValueError("isolated callback fault")
        if len(seen) == 63 and kind == "cancelled":
            raise asyncio.CancelledError("user callback cancellation")

    try:
        yields = []
        for index in range(_CALLBACK_QUANTUM * 2 + 1):
            pending = delivery.accept(Message("t", str(index).encode()), callback)
            # The callback already ran before accept() returned.
            assert seen[-1] == index
            if pending is not None:
                yields.append(index)
                await pending
        assert yields == [_CALLBACK_QUANTUM - 1, 2 * _CALLBACK_QUANTUM - 1]
        assert seen == list(range(_CALLBACK_QUANTUM * 2 + 1))
        assert delivery.pending_bytes == 0
        assert delivery.callback_invocations == _CALLBACK_QUANTUM * 2 + 1
        assert len(errors) == int(kind in ("error", "cancelled"))
    finally:
        loop.set_exception_handler(previous)


@pytest.mark.parametrize("fanout", [_CALLBACK_QUANTUM + 1, 500])
async def test_route_fanout_counts_every_invocation_toward_the_quantum(fanout):
    client = AsyncClient(message_delivery="callback")
    seen = []
    message = Message("/".join(["t"] * 9), b"owned")

    def callback(_message):
        seen.append(len(seen))

    for index in range(fanout):
        topic_filter = "/".join("+" if index & (1 << bit) else "t" for bit in range(9))
        client.message_callback_add(topic_filter, callback)
    client._freeze_message_routes()
    pending = client._delivery.accept(message, client._message_callback)
    # Fan-out is never preempted inside one message, but its invocations are
    # charged, so the reader yields at the next message boundary.
    assert seen == list(range(fanout))
    assert pending is not None
    await pending
    assert client._delivery.callback_invocations == fanout
    assert client._delivery.accept(Message("t", b"next"), lambda _m: seen.append(-1)) is None
    assert seen[-1] == -1
    assert client._delivery.pending_bytes == 0


async def test_reader_yields_between_quantum_groups_of_one_lot():
    client = AsyncClient(message_delivery="callback", keepalive=0)
    broker = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(broker)
    loop = asyncio.get_running_loop()
    heartbeat = loop.create_future()
    seen = []

    def callback(_message):
        seen.append(len(seen))
        if len(seen) == 1:
            loop.call_soon(lambda: heartbeat.set_result(len(seen)))

    client.on_message = callback
    await client.connect("fake")
    try:
        wire = PublishPacket(
            topic="t", payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        ).encode()
        total = _CALLBACK_QUANTUM * 2 - 5
        broker.push_rx(wire * total)
        # One bounded ingress lot carries every message; the loop still ran the
        # heartbeat after exactly one quantum of synchronous invocations.
        assert await asyncio.wait_for(heartbeat, 1) == _CALLBACK_QUANTUM
        await asyncio.wait_for(_wait_seen(lambda: len(seen) == total), 1)
        assert client.stats().delivery.callback_invocations == total
    finally:
        await client.disconnect()


async def _wait_seen(predicate) -> None:
    while not predicate():
        await asyncio.sleep(0)
