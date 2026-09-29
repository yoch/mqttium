"""stats() answers the questions a service asks after an incident."""

from __future__ import annotations

import asyncio
import time

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


def _qos0(topic: str) -> bytes:
    return PublishPacket(
        topic=topic, payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    ).encode(MQTTProtocolVersion.MQTTv311)


async def test_connected_since_and_last_disconnect_error() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("since")
    client._transport_factory = transport_factory(broker)
    assert client.stats().connected_since is None
    before = time.monotonic()
    try:
        await client.connect("broker")
        since = client.stats().connected_since
        assert since is not None and before <= since <= time.monotonic()
        broker.push_rx(b"")  # the broker closes the connection
        await wait_until(lambda: client.stats().last_disconnect_error is not None)
        stats = client.stats()
        assert stats.connected_since is None
        assert isinstance(stats.last_disconnect_error, BaseException)
    finally:
        await client.disconnect()


async def test_callback_failures_and_unrouted_messages_are_counted() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("counted", message_delivery="callback")
    client._transport_factory = transport_factory(broker)
    reported: list[object] = []

    def failing(message) -> None:  # noqa: ANN001
        raise RuntimeError("application bug")

    client.message_callback_add("plant/fails", failing)
    client.message_callback_add("plant/ok", lambda message: None)
    try:
        await client.connect("broker")
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: reported.append(context)
        )
        for topic in ("plant/fails", "plant/ok", "plant/nobody"):
            broker.push_rx(_qos0(topic))
        await wait_until(lambda: client.stats().delivery.unrouted_messages == 1)
        delivery = client.stats().delivery
        assert delivery.callback_invocations == 2
        assert delivery.callback_failures == 1
        assert len(reported) == 1
    finally:
        asyncio.get_running_loop().set_exception_handler(None)
        await client.disconnect()
