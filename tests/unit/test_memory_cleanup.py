"""Regression tests for low-risk memory cleanup paths."""

from __future__ import annotations

import pytest

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.transport.websocket import WebSocketTransport, _mask_client_frame, _parse_frame
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
    store = MemoryInflightStore()
    original = store._out
    message = OutboundMessage(
        mid=1,
        topic="memory/out",
        payload=b"payload",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBACK,
    )
    store.put_out(message)

    assert store.delete_out(message.mid) is True
    assert store._out == {}
    assert store._out is not original


def test_memory_store_releases_inbound_hash_capacity_when_empty() -> None:
    store = MemoryInflightStore()
    original = store._in
    message = InboundMessage(
        mid=1,
        topic="memory/in",
        payload=b"payload",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_PUBREL,
    )
    store.put_in(message)

    assert store.pop_in(message.mid) is message
    assert store._in == {}
    assert store._in is not original


@pytest.mark.parametrize("size", [0, 1, 125, 126, 65_535, 65_536])
def test_websocket_masking_round_trips_payload(size: int) -> None:
    payload = bytes(index & 0xFF for index in range(size))
    frame = _mask_client_frame(0x2, payload)

    assert isinstance(frame, bytearray)
    parsed = _parse_frame(frame, max(size, 1), expect_masked=True)

    assert parsed == (True, 0x2, payload)
    assert frame == bytearray()


class _FakeWriter:
    def __init__(self) -> None:
        self.written: list[bytes | bytearray] = []
        self.closed = False

    def write(self, data: bytes | bytearray) -> None:
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
    transport._pending_control.append(bytearray(b"control-frame"))
    transport._fragment = bytearray(b"fragmented-message")

    await transport.close()

    assert writer.closed is True
    assert transport._recv_buf == bytearray()
    assert transport._pending_control == []
    assert transport._fragment is None
