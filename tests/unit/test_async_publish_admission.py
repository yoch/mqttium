"""Async publication behavior at the logical outbound admission boundary."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import PublishMessage
from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState, PacketType
from mqttium.errors import FlowControlError, MQTTError, PublishBatchError
from mqttium.packets import encode_frame
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.protocol.reconnect import ReconnectPolicy


async def _complete(client: AsyncClient, mid: int) -> None:
    client._engine._complete_outbound_record(mid)
    client._engine.packet_ids.release(mid)
    await client._apply_effect(
        EngineEffect(kind=EffectKind.PUBLISH_COMPLETE, data=mid),
        nowait=False,
    )


async def test_wait_mode_blocks_until_logical_capacity_is_released() -> None:
    client = AsyncClient(
        max_pending_outbound_messages=1,
        max_pending_outbound_bytes=None,
    )
    first = await client.publish("admission/first", b"one", qos=1)
    assert first.mid is not None

    second_task = asyncio.create_task(client.publish("admission/second", b"two", qos=1))
    await asyncio.sleep(0)

    assert not second_task.done()
    assert client._engine.pending_outbound_messages == 1
    assert len(client._receipts) == 1

    await _complete(client, first.mid)
    second = await asyncio.wait_for(second_task, timeout=1.0)

    assert second.mid is not None
    assert client._engine.pending_outbound_messages == 1
    assert len(client._receipts) == 1


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
    client._outbound.put_nowait(b"occupied")
    client._outbound_bytes = len(b"occupied")

    with pytest.raises(FlowControlError):
        await client.publish("admission/writer", b"payload", qos=0, nowait=True)

    assert client._outbound.qsize() == 1
    assert client._outbound_bytes == len(b"occupied")
    assert not client._pending_effects
    assert not client._engine.take_effects()


async def test_nowait_writer_rejection_is_atomic_for_qos1() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    client._outbound.put_nowait(b"occupied")
    client._outbound_bytes = len(b"occupied")
    before_ids = len(client._engine.packet_ids)
    before_records = list(client._engine.store.out_items())

    with pytest.raises(FlowControlError):
        await client.publish("admission/writer", b"payload", qos=1, nowait=True)

    assert client._engine.pending_outbound_messages == 0
    assert len(client._engine.packet_ids) == before_ids
    assert list(client._engine.store.out_items()) == before_records
    assert not client._receipts
    assert not client._pending_effects


async def test_nowait_batch_writer_rejection_is_atomic() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    client._outbound.put_nowait(b"occupied")
    client._outbound_bytes = len(b"occupied")

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
    """Answers CONNECT with a session-present CONNACK, then closes on demand."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False

    async def write(self, data: bytes) -> None:
        if data and data[0] == PacketType.CONNECT:
            # session_present=1 keeps queued QoS 1 messages replayable, so the
            # admission budget stays occupied across the disconnect.
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x01\x00"))

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


async def test_parked_publish_fails_when_the_connection_is_gone_for_good() -> None:
    """Admission capacity is only released by an ACK: without a connection to
    deliver one, a parked publish() would wait forever."""
    client = AsyncClient(
        client_id="c",
        clean_start=False,
        max_pending_outbound_messages=1,
        reconnect=ReconnectPolicy(enabled=False),
    )
    transport = await _connect(client)
    await client.publish("admission/first", b"one", qos=1)

    parked = asyncio.create_task(client.publish("admission/second", b"two", qos=1))
    await asyncio.sleep(0)
    assert not parked.done()
    assert client._publish_waiters == 1

    transport.drop()

    with pytest.raises(MQTTError):
        await asyncio.wait_for(parked, timeout=1.0)
    assert client._publish_waiters == 0


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
