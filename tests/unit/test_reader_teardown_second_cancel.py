"""A second cancellation during reader teardown never strands the client.

After a reconnectable loss the reader decides to reconnect, then releases its
resources. A cancellation arriving during that release used to end the reader
before it started the reconnect task: the client stayed ``RECONNECTING`` for
ever with nothing to reconnect it.
"""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.protocol.reconnect import ReconnectPolicy
from tests.support import ScriptedBrokerTransport


class _MultiTurnClose(ScriptedBrokerTransport):
    async def close(self):
        self._closing = True
        for _ in range(3):
            await asyncio.sleep(0)
        await super().close()


async def _lose_connection_then_cancel(turns: int, internal: bool):
    transport = _MultiTurnClose()
    attempts = 0

    async def factory(host, port, *, ssl=None):
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            raise OSError("down")
        return transport

    client = AsyncClient(
        "second-cancel",
        keepalive=0,
        clean_start=False,
        reconnect=ReconnectPolicy(initial_delay=0.01, max_delay=0.01),
    )
    client._transport_factory = factory
    await client.connect("unused")
    reader = client._reader_task
    assert reader is not None
    transport.push_rx(b"")
    for _ in range(turns):
        await asyncio.sleep(0)
    if not reader.done():
        if internal:
            # What a failing writer, keepalive, effect or auth task does.
            await client._close_transport_after_connection_failure()
        else:
            reader.cancel()
    await asyncio.sleep(0.1)
    return client


@pytest.mark.parametrize("turns", range(14))
async def test_internal_cancellation_during_teardown_still_reconnects(turns):
    client = await _lose_connection_then_cancel(turns, internal=True)
    try:
        assert client.state is ConnectionState.RECONNECTING
        assert client._running_tasks()["reconnect"] or client.stats().reconnect_attempt > 0
    finally:
        await client.disconnect()


@pytest.mark.parametrize("turns", range(14))
async def test_external_cancellation_during_teardown_is_never_stranded(turns):
    client = await _lose_connection_then_cancel(turns, internal=False)
    try:
        stranded = (
            client.state is ConnectionState.RECONNECTING
            and not client._running_tasks()["reconnect"]
            and client.stats().reconnect_attempt == 0
        )
        assert not stranded
    finally:
        await client.disconnect()


class _SilentMultiTurnClose(_MultiTurnClose):
    def handle_packet(self, raw):
        from mqttium.enums import PacketType

        if raw.packet_type is PacketType.PUBLISH:
            return  # never acknowledged
        super().handle_packet(raw)


@pytest.mark.parametrize("turns", range(14))
async def test_disconnect_after_an_interrupted_release_settles_receipts(turns: int) -> None:
    """The interrupted reader keeps the reconnect decision and is gone before
    the backoff ends. A ``disconnect()`` in that gap finds no reader to settle
    pending work: it must do so itself, and no stale transport may remain."""
    from mqttium.errors import MQTTError

    transport = _SilentMultiTurnClose()
    attempts = 0

    async def factory(host, port, *, ssl=None):
        nonlocal attempts
        attempts += 1
        if attempts > 1:
            raise OSError("down")
        return transport

    client = AsyncClient(
        "gap",
        keepalive=0,
        clean_start=False,
        reconnect=ReconnectPolicy(initial_delay=30, max_delay=30),
    )
    client._transport_factory = factory
    await client.connect("unused")
    receipt = await client.publish("topic", b"payload", qos=1)
    reader = client._reader_task
    assert reader is not None
    transport.push_rx(b"")
    for _ in range(turns):
        await asyncio.sleep(0)
    if not reader.done():
        await client._close_transport_after_connection_failure()
    await asyncio.sleep(0.05)
    assert reader.done()
    assert client._transport is None

    await asyncio.wait_for(client.disconnect(), 2)
    assert client.state is ConnectionState.DISCONNECTED
    with pytest.raises(MQTTError):
        await asyncio.wait_for(receipt.wait(), 1)
