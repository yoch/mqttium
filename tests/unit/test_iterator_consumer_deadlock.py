"""A consumer that waits on the reader from inside messages() is bounded.

While the iterator queue is full the reader waits for space and reads
nothing, so the PUBACK such a consumer awaits can never arrive. The admission
deadline turns that wait cycle into a named connection failure.
"""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, QoS
from mqttium.errors import MessageDeliveryError, MQTTError
from mqttium.packets import PublishPacket
from tests.support import ScriptedBrokerTransport, transport_factory


def _backlog(count: int) -> bytes:
    frame = PublishPacket(
        topic="in", payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False, mid=None
    ).encode(ScriptedBrokerTransport().protocol)
    return frame * count


async def test_consumer_awaiting_a_receipt_with_a_full_queue_fails_in_bounded_time():
    transport = ScriptedBrokerTransport(publish_after_subscribe=_backlog(40))
    client = AsyncClient(
        "consumer-deadlock",
        keepalive=0,
        max_iterator_messages=2,
        iterator_admission_timeout=0.2,
    )
    client._transport_factory = transport_factory(transport)
    causes: list[BaseException | None] = []
    disconnected = asyncio.Event()

    async def on_disconnect(cause):
        causes.append(cause)
        disconnected.set()

    client.on_disconnect = on_disconnect
    await client.connect("unused")
    await client.subscribe("in")
    outcomes: list[BaseException] = []

    async def consumer():
        async for _message in client.messages():
            receipt = await client.publish("out", b"reply", qos=1)
            try:
                await receipt.wait()  # its PUBACK is behind the parked reader
            except MQTTError as exc:
                outcomes.append(exc)
                return

    task = asyncio.create_task(consumer())
    try:
        await asyncio.wait_for(disconnected.wait(), 5)
        await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.disconnect()
    assert len(causes) == 1 and isinstance(causes[0], MessageDeliveryError)
    assert "messages() consumer" in str(causes[0])
    assert len(outcomes) == 1
    assert client.state is ConnectionState.DISCONNECTED


def test_admission_deadline_is_finite_by_default_and_can_be_disabled():
    assert AsyncClient("default")._delivery.iterator_admission_timeout == 60.0
    unbounded = AsyncClient("unbounded", iterator_admission_timeout=None)
    assert unbounded._delivery.iterator_admission_timeout is None
    # Callback delivery has no queue; either spelling of "unset" is accepted.
    AsyncClient("callback", message_delivery="callback")
    AsyncClient(
        "callback-none",
        message_delivery="callback",
        iterator_admission_timeout=None,
    )
    with pytest.raises(ValueError):
        AsyncClient("callback-set", message_delivery="callback", iterator_admission_timeout=1.0)
