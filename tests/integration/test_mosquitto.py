"""Live Mosquitto integration (requires broker on 127.0.0.1:11883)."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.types import Properties


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [0, 1, 2])
async def test_pubsub_roundtrip(protocol: MQTTProtocolVersion, qos: int) -> None:
    topic = f"mqttium/it/{int(protocol)}/{qos}"
    got: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    sub = AsyncClient(f"sub-{protocol}-{qos}", protocol=protocol, message_delivery="callback")
    pub = AsyncClient(f"pub-{protocol}-{qos}", protocol=protocol)

    def on_message(msg) -> None:  # noqa: ANN001
        if not got.done():
            got.set_result(msg.payload)

    sub.on_message = on_message
    try:
        await sub.connect("127.0.0.1", 11883, timeout=5)
        result = await sub.subscribe(topic, qos=qos if qos else 0)
        assert result.reason_codes == (qos,)
        await pub.connect("127.0.0.1", 11883, timeout=5)
        receipt = await pub.publish(topic, b"hello-it", qos=qos)
        if qos:
            await receipt.wait()
        payload = await asyncio.wait_for(got, timeout=5)
        assert payload == b"hello-it"
    finally:
        await pub.disconnect()
        await sub.disconnect()


async def test_subscribe_only_durable_session_resumes() -> None:
    client = AsyncClient(
        "mqttium-it-subscribe-only-resume",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=True,
        connect_properties=Properties({"session_expiry_interval": 3600}),
    )
    try:
        first = await client.connect("127.0.0.1", 11883, timeout=5)
        assert first.session_present is False
        await client.subscribe("mqttium/it/subscribe-only-resume", qos=1)
        await client.disconnect()

        resumed = await client.connect("127.0.0.1", 11883, timeout=5)
        assert resumed.session_present is True
    finally:
        await client.disconnect()


async def test_durable_session_resumes_after_a_process_restart() -> None:
    """A new client instance (a restarted process) resumes its broker session."""
    client_id = "mqttium-it-restart-resume"
    topic = "mqttium/it/restart-resume"
    options = {
        "protocol": MQTTProtocolVersion.MQTTv5,
        "clean_start": False,
        "connect_properties": Properties({"session_expiry_interval": 60}),
    }
    first = AsyncClient(client_id, **options)
    try:
        await first.connect("127.0.0.1", 11883, timeout=5)
        await first.subscribe(topic, qos=1)
    finally:
        await first.disconnect()

    publisher = AsyncClient("mqttium-it-restart-publisher")
    try:
        await publisher.connect("127.0.0.1", 11883, timeout=5)
        receipt = await publisher.publish(topic, b"while-offline", qos=1)
        await receipt.wait()
    finally:
        await publisher.disconnect()

    restarted = AsyncClient(client_id, **options)
    try:
        connack = await restarted.connect("127.0.0.1", 11883, timeout=5)
        assert connack.session_present is True
        message = await asyncio.wait_for(anext(restarted.messages()), timeout=5)
        assert message.payload == b"while-offline"
    finally:
        await restarted.disconnect()


@pytest.mark.parametrize("qos", [1, 2])
async def test_mqtt311_publish_burst_reaches_the_subscriber(qos: int) -> None:
    """Mosquitto acknowledges and drops QoS 1/2 beyond 20 in flight on 3.1.1."""
    count = 500
    topic = f"mqttium/it/burst/{qos}"
    received = 0
    done = asyncio.Event()
    sub = AsyncClient(f"burst-sub-{qos}", message_delivery="callback")
    pub = AsyncClient(f"burst-pub-{qos}")

    def on_message(msg) -> None:  # noqa: ANN001
        nonlocal received
        received += 1
        if received == count:
            done.set()

    sub.on_message = on_message
    try:
        await sub.connect("127.0.0.1", 11883, timeout=5)
        await sub.subscribe(topic, qos=qos)
        await pub.connect("127.0.0.1", 11883, timeout=5)
        receipts = [await pub.publish(topic, b"x" * 64, qos=qos) for _ in range(count)]
        async with asyncio.timeout(10):
            for receipt in receipts:
                await receipt.wait()
            await done.wait()
    finally:
        await pub.disconnect()
        await sub.disconnect()
    assert received == count


@pytest.mark.parametrize("qos", [1, 2])
async def test_reader_owns_the_ack_permit_in_callback_delivery(qos: int) -> None:
    """With auto-ack callback delivery the reader restores the ACK permit itself.

    Its eager ACKs schedule no next-turn re-arm callback, every exchange still
    completes, and the permit is back once the reader waits for more bytes.
    """
    count = 200
    topic = f"mqttium/it/ack-owner/{qos}"
    received = 0
    done = asyncio.Event()
    sub = AsyncClient(f"ack-owner-sub-{qos}", message_delivery="callback")
    pub = AsyncClient(f"ack-owner-pub-{qos}")
    rearm_callbacks = 0
    pump = sub._write_pump
    schedule = pump._schedule_eager_rearm

    def counted_schedule() -> None:
        nonlocal rearm_callbacks
        rearm_callbacks += 1
        schedule()

    def on_message(msg) -> None:  # noqa: ANN001
        nonlocal received
        received += 1
        if received == count:
            done.set()

    sub.on_message = on_message
    try:
        await sub.connect("127.0.0.1", 11883, timeout=5)
        assert pump._ack_rearm_owner is sub._reader_task
        await sub.subscribe(topic, qos=qos)
        pump._schedule_eager_rearm = counted_schedule  # type: ignore[method-assign]
        await pub.connect("127.0.0.1", 11883, timeout=5)
        for _ in range(count):
            await (await pub.publish(topic, b"x" * 64, qos=qos)).wait()
        async with asyncio.timeout(10):
            await done.wait()
        await asyncio.sleep(0.05)
        assert sub.stats().inbound.inflight == 0
        assert pump.eager_writes > 0
        assert rearm_callbacks == 0
        assert pump._ack_eager_armed is True
    finally:
        await pub.disconnect()
        await sub.disconnect()
    assert received == count


@pytest.mark.parametrize("manual_ack", [False, True])
async def test_iterator_delivery_keeps_the_next_turn_ack_rearm(manual_ack: bool) -> None:
    client = AsyncClient(f"ack-owner-iterator-{manual_ack}", manual_ack=manual_ack)
    try:
        await client.connect("127.0.0.1", 11883, timeout=5)
        assert client._write_pump._ack_rearm_owner is None
    finally:
        await client.disconnect()
