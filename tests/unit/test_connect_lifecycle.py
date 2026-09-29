"""Connection lifecycle as a service sees it: start, retry, stop, shut down."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ConnectError, ConnectRefusedError, MQTTError, NotConnectedError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport, wait_until


def _fast_policy(**overrides) -> ReconnectPolicy:  # noqa: ANN003
    options = {"initial_delay": 0.002, "max_delay": 0.008, "stable_after": 0}
    options.update(overrides)
    return ReconnectPolicy(**options)


class _RefusingBroker(ScriptedBrokerTransport):
    def __init__(self, reason: int) -> None:
        super().__init__()
        self.reason = reason

    def handle_packet(self, raw) -> None:  # noqa: ANN001
        if raw.packet_type is PacketType.CONNECT:
            self.push_rx(encode_frame(PacketType.CONNACK, 0, bytes((0, self.reason))))
        else:
            super().handle_packet(raw)


def _qos0(topic: str, payload: bytes) -> bytes:
    packet = PublishPacket(
        topic=topic, payload=payload, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    )
    return packet.encode(MQTTProtocolVersion.MQTTv311)


async def test_first_connect_retries_until_the_broker_is_up() -> None:
    broker = ScriptedBrokerTransport()
    attempts: list[ConnectionState] = []
    client = AsyncClient("starts-early", reconnect=_fast_policy())

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        attempts.append(client.state)
        if len(attempts) < 3:
            raise ConnectionRefusedError("broker not up yet")
        return broker

    client._transport_factory = factory
    try:
        await client.connect("broker")
        assert client.is_connected
        # Between attempts the client reports that it is still trying.
        assert attempts[1:] == [ConnectionState.RECONNECTING] * 2
    finally:
        await client.disconnect()


async def test_first_connect_stops_on_a_terminal_refusal() -> None:
    attempts = []
    client = AsyncClient("refused-early", reconnect=_fast_policy())

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        attempts.append(True)
        return _RefusingBroker(reason=5)

    client._transport_factory = factory
    with pytest.raises(ConnectRefusedError) as caught:
        await client.connect("broker")
    assert caught.value.reason_code == 5
    assert len(attempts) == 1
    assert client.state is ConnectionState.DISCONNECTED
    await client.disconnect()


async def test_disconnect_abandons_a_first_connect_waiting_to_retry() -> None:
    client = AsyncClient("abandoned", reconnect=ReconnectPolicy(initial_delay=30.0, max_delay=30.0))

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        raise ConnectionRefusedError("down")

    client._transport_factory = factory
    connecting = asyncio.create_task(client.connect("broker"))
    await wait_until(lambda: client.state is ConnectionState.RECONNECTING)
    async with asyncio.timeout(2):
        await client.disconnect()
        with pytest.raises(MQTTError, match="cancelled by disconnect"):
            await connecting
    # Never connected, and nothing is trying any more.
    assert client.state is ConnectionState.NEW


async def test_failed_connect_keeps_the_message_stream_for_a_later_connection() -> None:
    client = AsyncClient("stream-survives")
    stream = client.messages()
    consumer = asyncio.create_task(anext(stream))

    async def refused(*args, **kwargs):  # noqa: ANN002, ANN003
        raise ConnectionRefusedError("down")

    client._transport_factory = refused
    with pytest.raises(ConnectError):
        await client.connect("broker")
    await asyncio.sleep(0)
    assert not consumer.done()

    broker = ScriptedBrokerTransport()

    async def up(*args, **kwargs):  # noqa: ANN002, ANN003
        return broker

    client._transport_factory = up
    try:
        await client.connect("broker")
        broker.push_rx(_qos0("after/retry", b"ok"))
        message = await asyncio.wait_for(consumer, 2)
        assert message.payload == b"ok"
    finally:
        await client.disconnect()


async def test_publish_refuses_when_no_connection_is_pending() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("stopped-publisher")

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        return broker

    client._transport_factory = factory
    # Before the first connect() the offline queue stays available.
    queued = await client.publish("t/early", b"x", qos=1)
    await client.connect("broker")
    await asyncio.wait_for(queued.wait(), 2)
    await client.disconnect()
    with pytest.raises(NotConnectedError):
        await client.publish("t/late", b"x", qos=1)
    with pytest.raises(NotConnectedError):
        client.publish_nowait("t/late", b"x", qos=2)


async def test_lost_connection_without_policy_refuses_new_publications() -> None:
    broker = ScriptedBrokerTransport()
    client = AsyncClient("lost-publisher")

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        return broker

    client._transport_factory = factory
    try:
        await client.connect("broker")
        broker.push_rx(b"")
        await wait_until(lambda: client.state is ConnectionState.DISCONNECTED)
        with pytest.raises(NotConnectedError):
            await client.publish("t/x", b"x", qos=1)
    finally:
        await client.disconnect()


async def test_externally_cancelled_reader_does_not_start_a_reconnect() -> None:
    """Event-loop shutdown cancels every task, including the client's reader."""
    broker = ScriptedBrokerTransport()
    client = AsyncClient("shutdown", reconnect=_fast_policy())

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        return broker

    client._transport_factory = factory
    try:
        await client.connect("broker")
        reader = client._reader_task
        assert reader is not None
        reader.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reader
        assert client._reconnect_task is None
        assert client.state is ConnectionState.DISCONNECTED
    finally:
        await client.disconnect()


async def test_async_with_disconnects_on_exit() -> None:
    broker = ScriptedBrokerTransport()

    async def factory(*args, **kwargs):  # noqa: ANN002, ANN003
        return broker

    async with AsyncClient("scoped") as client:
        client._transport_factory = factory
        await client.connect("broker")
        assert client.is_connected
    assert client.state is ConnectionState.DISCONNECTED
    await wait_until(lambda: not any(client._running_tasks().values()))
