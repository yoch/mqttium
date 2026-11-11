"""A timeout argument bounds the whole call it is passed to.

``subscribe``/``unsubscribe`` used to bound only the acknowledgement wait, so
a request stuck behind a blocked writer outlived its timeout; ``publish`` and
receipt waits had no bound at all.
"""

import asyncio
import time

import pytest

from mqttium.api import AsyncClient, PublishMessage
from mqttium.enums import PacketType
from mqttium.errors import FlowControlError, MQTTError, MQTTTimeoutError
from mqttium.packets import PubAckPacket
from tests.support import ScriptedBrokerTransport, transport_factory


class _SilentBroker(ScriptedBrokerTransport):
    """Completes CONNECT, then never acknowledges a PUBLISH."""

    def handle_packet(self, raw):
        if raw.packet_type is PacketType.PUBLISH:
            self.decoded = getattr(self, "decoded", 0) + 1
            return
        super().handle_packet(raw)


class _BlockedWriter(ScriptedBrokerTransport):
    """Completes CONNECT, then never finishes another write."""

    def __init__(self):
        super().__init__()
        self.connected = False

    async def write(self, data):
        if self.connected:
            await asyncio.Event().wait()
        await super().write(data)
        self.connected = True


async def _connected(transport, **options) -> AsyncClient:
    client = AsyncClient("deadlines", keepalive=0, **options)
    client._transport_factory = transport_factory(transport)
    await client.connect("unused")
    return client


async def _fill_writer(client: AsyncClient) -> asyncio.Task[None]:
    async def flood():
        while True:
            await client.publish("flood", b"x" * 65536)

    task = asyncio.create_task(flood())
    for _ in range(400):
        await asyncio.sleep(0.005)
        if client.stats().writer.waiters:
            return task
    task.cancel()
    raise AssertionError("the writer never filled")


@pytest.mark.parametrize("operation", ["subscribe", "unsubscribe"])
async def test_request_timeout_covers_the_handoff_to_a_blocked_writer(operation):
    client = await _connected(_BlockedWriter())
    flood = await _fill_writer(client)
    try:
        started = time.monotonic()
        with pytest.raises(MQTTTimeoutError):
            await asyncio.wait_for(getattr(client, operation)("a/b", timeout=0.2), 5)
        assert time.monotonic() - started < 2.0
    finally:
        flood.cancel()
        await asyncio.gather(flood, return_exceptions=True)
        await client._force_close()


async def test_publish_timeout_before_admission_admits_nothing():
    client = await _connected(_SilentBroker(), max_unacknowledged_messages=1)
    try:
        first = await client.publish("t", b"1", qos=1)
        started = time.monotonic()
        with pytest.raises(MQTTTimeoutError):
            await asyncio.wait_for(client.publish("t", b"2", qos=1, timeout=0.1), 5)
        assert time.monotonic() - started < 2.0
        assert client.stats().receipts.publish == 1
        assert not first.is_done()
        # The abandoned waiter does not consume a later wakeup.
        assert client.stats().receipts.publish_waiters == 0
    finally:
        await client.disconnect()


async def test_publish_timeout_behind_a_blocked_writer_admits_nothing():
    client = await _connected(_BlockedWriter())
    flood = await _fill_writer(client)
    try:
        with pytest.raises(MQTTTimeoutError):
            await asyncio.wait_for(client.publish("t", b"late", qos=1, timeout=0.1), 5)
        assert client.stats().receipts.publish == 0
    finally:
        flood.cancel()
        await asyncio.gather(flood, return_exceptions=True)
        await client._force_close()


async def test_publish_timeout_after_admission_returns_the_receipt():
    client = await _connected(_BlockedWriter())
    try:
        # Fill the writer itself, leaving no effect waiting behind it.
        with pytest.raises(FlowControlError):
            while True:
                client.publish_nowait("flood", b"x" * 65536)
        receipt = await asyncio.wait_for(client.publish("t", b"kept", qos=1, timeout=0.1), 5)
        # Admitted before the deadline: owned by the client, never undone.
        assert not receipt.is_done()
        assert client.stats().receipts.publish == 1
    finally:
        await client._force_close()
    with pytest.raises(MQTTError):
        await asyncio.wait_for(receipt.wait(), 1)


async def test_receipt_wait_timeout_leaves_the_publication_active():
    transport = _SilentBroker()
    client = await _connected(transport)
    try:
        receipt = await client.publish("t", b"x", qos=1)
        with pytest.raises(MQTTTimeoutError):
            await receipt.wait(timeout=0.05)
        assert not receipt.is_done()
        assert receipt._waiters is None  # the timed-out waiter was retired
        transport.push_rx(PubAckPacket(mid=receipt.mid).encode(transport.protocol))
        await receipt.wait(timeout=1)
        assert receipt.is_done()
    finally:
        await client.disconnect()


async def test_receipt_wait_reraises_a_failure_that_is_itself_a_timeout():
    client = await _connected(_SilentBroker())
    receipt = await client.publish("t", b"x", qos=1)
    failure = MQTTTimeoutError("PINGRESP timed out")
    waiting = asyncio.create_task(receipt.wait(timeout=5))
    await asyncio.sleep(0)
    client._fail_pending(failure)
    with pytest.raises(MQTTTimeoutError) as raised:
        await waiting
    assert raised.value is failure
    await client.disconnect()


async def test_batch_wait_timeout_leaves_the_batch_active():
    client = await _connected(_SilentBroker())
    try:
        batch = await client.publish_many([PublishMessage("t", b"x", qos=1)])
        with pytest.raises(MQTTTimeoutError):
            await batch.wait(timeout=0.05)
        assert not batch.is_done()
    finally:
        await client.disconnect()


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
async def test_invalid_timeouts_are_rejected(timeout):
    client = await _connected(_SilentBroker())
    try:
        with pytest.raises(ValueError):
            await client.publish("t", b"x", timeout=timeout)
        receipt = await client.publish("t", b"x", qos=1)
        with pytest.raises(ValueError):
            await receipt.wait(timeout=timeout)
    finally:
        await client.disconnect()
