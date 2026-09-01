"""Async publication behavior at the logical outbound admission boundary."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import PublishMessage
from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.errors import FlowControlError, PublishBatchError
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy


async def test_nowait_rejection_is_atomic() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    first = await client.publish("admission/first", b"one", qos=1)
    assert first.mid is not None
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    with pytest.raises(FlowControlError):
        await client.publish("admission/rejected", b"two", qos=1, nowait=True)

    assert client._engine.pending_outbound_messages == 1
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert len(client._receipts) == 1
    assert not client._pending_effects


async def test_error_mode_refuses_without_waiting() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
        publish_backpressure="error",
    )
    await client.publish("admission/first", b"one", qos=1)

    with pytest.raises(FlowControlError):
        await client.publish("admission/rejected", b"two", qos=1)

    assert client._engine.pending_outbound_messages == 1


async def test_cancellation_while_waiting_leaves_no_publication_state() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    await client.publish("admission/first", b"one", qos=1)
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    waiting = asyncio.create_task(client.publish("admission/cancelled", b"two", qos=1))
    await asyncio.sleep(0)
    assert not waiting.done()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert client._engine.pending_outbound_messages == 1
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert len(client._receipts) == 1


async def test_nowait_writer_rejection_is_atomic_for_qos0() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True

    with pytest.raises(FlowControlError):
        await client.publish("admission/writer", b"payload", qos=0, nowait=True)

    assert client._outbound.qsize() == 1
    assert client._outbound_bytes == len(b"occupied")
    assert not client._pending_effects
    assert not client._engine.take_effects()


async def test_nowait_writer_rejection_is_atomic_for_qos1() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    with pytest.raises(FlowControlError):
        await client.publish("admission/writer", b"payload", qos=1, nowait=True)

    assert client._engine.pending_outbound_messages == 0
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert not client._receipts
    assert not client._pending_effects


async def test_publish_nowait_writer_rejection_is_atomic_for_qos1() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    with pytest.raises(FlowControlError):
        client.publish_nowait("admission/writer", b"payload", qos=1)

    assert client._engine.pending_outbound_messages == 0
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert not client._receipts
    assert not client._pending_effects


async def test_nowait_batch_writer_rejection_is_atomic() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True

    with pytest.raises(PublishBatchError) as exc_info:
        await client.publish_many(
            [PublishMessage("admission/batch", b"payload", qos=1)],
            nowait=True,
        )
    assert isinstance(exc_info.value.cause, FlowControlError)

    assert client._engine.pending_outbound_messages == 0
    assert not client._engine.packet_ids
    assert not list(client._engine.store.out_items())
    assert not client._batch_receipts
    assert not client._pending_effects


class _ClosingTransport:
    """Answers CONNECT with a fresh-session CONNACK, then closes on demand."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False

    async def write(self, data: bytes) -> None:
        if data and data[0] == PacketType.CONNECT:
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self.drop()

    def is_closing(self) -> bool:
        return self._closing

    def drop(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")


async def _connect(client: AsyncClient) -> _ClosingTransport:
    transport = _ClosingTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> _ClosingTransport:
        return transport

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)
    return transport


async def test_parked_publish_keeps_waiting_while_reconnect_is_pending() -> None:
    client = AsyncClient(
        client_id="c",
        clean_start=False,
        max_pending_outbound_messages=1,
        reconnect=ReconnectPolicy(enabled=True, initial_delay=30.0),
    )
    transport = await _connect(client)
    await client.publish("admission/first", b"one", qos=1)

    parked = asyncio.create_task(client.publish("admission/second", b"two", qos=1))
    await asyncio.sleep(0)
    assert not parked.done()

    transport.drop()
    await asyncio.sleep(0.1)

    assert not parked.done(), "a reconnecting client must keep the producer parked"

    parked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await parked
    assert client._publish_waiters == 0
    await client.disconnect()


async def test_flow_control_error_names_the_message_bound() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024 * 1024)
    client._engine.state = ConnectionState.CONNECTED

    client.publish_nowait("bound/messages", b"x", qos=0)
    with pytest.raises(FlowControlError) as excinfo:
        client.publish_nowait("bound/messages", b"x", qos=0)

    message = str(excinfo.value)
    assert "max_outbound_messages=1" in message
    assert "max_outbound_bytes" not in message


async def test_flow_control_error_names_the_byte_bound() -> None:
    """The default pairing that makes large payloads surprising."""
    client = AsyncClient(max_outbound_messages=10_000, max_outbound_bytes=4096)
    client._engine.state = ConnectionState.CONNECTED

    client.publish_nowait("bound/bytes", b"x" * 3000, qos=0)
    with pytest.raises(FlowControlError) as excinfo:
        client.publish_nowait("bound/bytes", b"x" * 3000, qos=0)

    message = str(excinfo.value)
    assert "max_outbound_bytes=4096" in message
    assert "already queued" in message


async def test_batch_refusal_names_the_bound_the_batch_actually_hit() -> None:
    """publish_many admits against running totals, not the live queue.

    With an empty writer, the message bound is only reached part-way through
    the batch, so the error must not describe the byte bound it never hit.
    """
    client = AsyncClient(max_outbound_messages=3, max_outbound_bytes=64 * 1024 * 1024)
    client._engine.state = ConnectionState.CONNECTED

    requests = [PublishMessage("bound/batch", b"x", 0) for _ in range(8)]
    with pytest.raises(PublishBatchError) as excinfo:
        await client.publish_many(requests, nowait=True)

    message = str(excinfo.value.cause)
    assert "max_outbound_messages=3" in message
    assert "max_outbound_bytes" not in message
