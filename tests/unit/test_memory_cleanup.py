"""Regression tests for low-risk memory cleanup paths."""

from __future__ import annotations

from sys import getsizeof

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.transport.websocket import WebSocketTransport
from mqttium.types import InboundMessage, OutboundMessage


def test_packet_id_pool_releases_peak_containers_when_idle() -> None:
    pool = PacketIdPool()
    original_used = pool._used
    original_free = pool._free
    mids = [pool.allocate() for _ in range(1_000)]

    for mid in mids:
        pool.release(mid)

    assert len(pool) == 0
    assert pool._used == set()
    assert pool._free == []
    assert pool._used is not original_used
    assert pool._free is not original_free
    assert pool.allocate() == 1


def test_packet_id_pool_clear_releases_peak_containers() -> None:
    pool = PacketIdPool()
    for _ in range(1_000):
        pool.allocate()
    original_used = pool._used
    original_free = pool._free

    pool.clear()

    assert pool._used == set()
    assert pool._free == []
    assert pool._used is not original_used
    assert pool._free is not original_free
    assert pool.allocate() == 1


def test_memory_store_releases_outbound_hash_capacity_when_empty() -> None:
    """A table that grew to a deep window must not retain that capacity.

    Asserted on the released footprint rather than on object identity: a
    shallow window drains to empty on every acknowledgement and has no
    peak-sized table to drop.
    """
    store = MemoryInflightStore()
    for mid in range(1, 513):
        store.put_out(
            OutboundMessage(
                mid=mid,
                topic="memory/out",
                payload=b"payload",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBACK,
            )
        )
    grown = getsizeof(store._out)

    for mid in range(1, 513):
        assert store.delete_out(mid) is True

    assert store._out == {}
    assert getsizeof(store._out) < grown / 8


def test_memory_store_releases_inbound_hash_capacity_when_empty() -> None:
    store = MemoryInflightStore()
    for mid in range(1, 513):
        store.put_in(
            InboundMessage(
                mid=mid,
                topic="memory/in",
                payload=b"payload",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                state=InboundQoSState.WAIT_PUBREL,
            )
        )
    grown = getsizeof(store._in)

    for mid in range(1, 513):
        assert store.pop_in(mid) is not None

    assert store._in == {}
    assert getsizeof(store._in) < grown / 8


class _FakeWriter:
    def __init__(self) -> None:
        self.written: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None

    def is_closing(self) -> bool:
        return self.closed


async def test_websocket_close_releases_connection_buffers() -> None:
    writer = _FakeWriter()
    transport = WebSocketTransport(object(), writer)  # type: ignore[arg-type]
    transport._recv_buf.extend(b"buffered-frame")
    transport._pending_control.append(b"control-frame")
    transport._fragment = bytearray(b"fragmented-message")

    await transport.close()

    assert writer.closed is True
    assert transport._recv_buf == bytearray()
    assert transport._pending_control == []
    assert transport._fragment is None


async def test_force_close_discards_writer_queue_and_decoder_buffer() -> None:
    from mqttium.api import AsyncClient

    client = AsyncClient()
    client._outbound.put_nowait(b"one")
    client._outbound.put_nowait(b"two")
    client._outbound_bytes = 6
    client._decoder.feed(b"partial")

    await client._force_close()

    assert client._outbound.empty()
    assert client._outbound_bytes == 0
    assert client._decoder.buffered == 0


async def test_reset_message_stream_releases_abandoned_iterator_delivery() -> None:
    from mqttium.api import AsyncClient
    from mqttium.protocol.engine import EffectKind, EngineEffect
    from mqttium.types import Message

    client = AsyncClient(
        message_delivery="iterator",
        max_pending_delivery_bytes=1024,
    )
    message = Message(topic="reset", payload=b"payload")
    await client._apply_effect(EngineEffect(EffectKind.MESSAGE, message), nowait=False)
    assert client.pending_delivery_bytes > 0
    client._closed.set()

    await client._reset_message_stream()

    assert client._messages.empty()
    assert client.pending_delivery_bytes == 0
    assert not hasattr(message, "_delivery_references")
    assert not hasattr(message, "_delivery_logical_bytes")
