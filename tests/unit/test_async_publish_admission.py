"""Async publication behavior at the logical outbound admission boundary."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import PublishMessage
from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.errors import FlowControlError
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
    before_records = list(
        client._engine.store.get_out(summary.mid)
        for page in client._engine.store.out_summary_pages()
        for summary in page
    )

    with pytest.raises(FlowControlError):
        client.publish_nowait("admission/rejected", b"two", qos=1)

    assert client._engine.pending_outbound_messages == 1
    assert len(client._engine.packet_ids) == before_ids
    assert (
        list(
            client._engine.store.get_out(summary.mid)
            for page in client._engine.store.out_summary_pages()
            for summary in page
        )
        == before_records
    )
    assert len(client._receipts) == 1
    assert not client._effect_pump.pending


def test_global_publish_backpressure_option_is_removed() -> None:
    with pytest.raises(TypeError):
        AsyncClient(publish_backpressure="error")


async def test_cancellation_while_waiting_leaves_no_publication_state() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    await client.publish("admission/first", b"one", qos=1)
    before_ids = len(client._engine.packet_ids)
    before_records = list(
        client._engine.store.get_out(summary.mid)
        for page in client._engine.store.out_summary_pages()
        for summary in page
    )

    waiting = asyncio.create_task(client.publish("admission/cancelled", b"two", qos=1))
    await asyncio.sleep(0)
    assert not waiting.done()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert client._engine.pending_outbound_messages == 1
    assert len(client._engine.packet_ids) == before_ids
    assert (
        list(
            client._engine.store.get_out(summary.mid)
            for page in client._engine.store.out_summary_pages()
            for summary in page
        )
        == before_records
    )
    assert len(client._receipts) == 1


async def test_nowait_writer_rejection_is_atomic_for_qos0() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True

    with pytest.raises(FlowControlError):
        client.publish_nowait("admission/writer", b"payload", qos=0)

    assert client._write_pump.queue.qsize() == 1
    assert client._write_pump.queued_bytes == len(b"occupied")
    assert not client._effect_pump.pending
    assert not client._engine.take_effects()


async def test_nowait_writer_rejection_is_atomic_for_qos1() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True
    before_ids = len(client._engine.packet_ids)
    before_records = list(
        client._engine.store.get_out(summary.mid)
        for page in client._engine.store.out_summary_pages()
        for summary in page
    )

    with pytest.raises(FlowControlError):
        client.publish_nowait("admission/writer", b"payload", qos=1)

    assert client._engine.pending_outbound_messages == 0
    assert len(client._engine.packet_ids) == before_ids
    assert (
        list(
            client._engine.store.get_out(summary.mid)
            for page in client._engine.store.out_summary_pages()
            for summary in page
        )
        == before_records
    )
    assert not client._receipts
    assert not client._effect_pump.pending


async def test_publish_nowait_writer_rejection_is_atomic_for_qos1() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied") is True
    before_ids = len(client._engine.packet_ids)
    before_records = list(
        client._engine.store.get_out(summary.mid)
        for page in client._engine.store.out_summary_pages()
        for summary in page
    )

    with pytest.raises(FlowControlError):
        client.publish_nowait("admission/writer", b"payload", qos=1)

    assert client._engine.pending_outbound_messages == 0
    assert len(client._engine.packet_ids) == before_ids
    assert (
        list(
            client._engine.store.get_out(summary.mid)
            for page in client._engine.store.out_summary_pages()
            for summary in page
        )
        == before_records
    )
    assert not client._receipts
    assert not client._effect_pump.pending


async def test_cancelled_batch_waiting_for_writer_keeps_committed_prefix() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    assert client._write_pump.try_enqueue(b"occupied")
    pending = asyncio.create_task(client.publish_many([PublishMessage("batch", b"x", qos=1)]))
    await asyncio.sleep(0)
    assert client._engine.pending_outbound_messages == 1
    receipt = next(iter(client._batch_receipts.values()))
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert receipt.submitted == 1
    assert receipt._sealed
    assert client._engine.pending_outbound_messages == 1
    await client._force_close()


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
