"""Use constant-time PacketIdPool.clear only when no MID can survive."""

from __future__ import annotations


from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, OutboundQoSState, PacketType, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.types import OutboundMessage


class TrackingPacketIdPool(PacketIdPool):
    def __init__(self) -> None:
        super().__init__()
        self.clear_calls = 0
        self.release_calls = 0

    def clear(self) -> None:
        self.clear_calls += 1
        super().clear()

    def release(self, mid: int) -> None:
        self.release_calls += 1
        super().release(mid)


def _tracking_engine() -> tuple[ProtocolEngine, TrackingPacketIdPool]:
    engine = ProtocolEngine()
    pool = TrackingPacketIdPool()
    engine.packet_ids = pool
    return engine, pool


def _outbound(mid: int, state: OutboundQoSState) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic="t",
        payload=b"x",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=state,
        logical_size=2,
    )


def test_transport_close_clears_a_sub_only_pool() -> None:
    engine, pool = _tracking_engine()
    engine.state = ConnectionState.CONNECTED
    for mid in (pool.allocate(), pool.allocate()):
        engine._pending_sub_requests[mid] = (PacketType.SUBACK, 1)

    engine.notify_transport_closed()

    assert pool.release_calls == 0
    assert pool.clear_calls == 1
    assert len(pool) == 0
    assert not engine._pending_sub_mids


def test_transport_close_keeps_publish_ids_when_releasing_subscriptions() -> None:
    engine, pool = _tracking_engine()
    engine.state = ConnectionState.CONNECTED
    publish_mid = pool.allocate()
    sub_mid = pool.allocate()
    engine.outbound._pending_messages = 1
    engine._pending_sub_requests[sub_mid] = (PacketType.SUBACK, 1)

    engine.notify_transport_closed()

    assert pool.clear_calls == 0
    assert pool.release_calls == 1
    assert pool.in_use(publish_mid)
    assert not pool.in_use(sub_mid)


def test_missing_session_clears_all_abandoned_inflight_packet_ids_once() -> None:
    store = MemoryInflightStore()
    for mid in (7, 19):
        store.put_out(_outbound(mid, OutboundQoSState.WAIT_PUBACK))
    engine = ProtocolEngine(store=store)
    pool = TrackingPacketIdPool()
    for mid in (7, 19):
        pool.reserve(mid)
    engine.packet_ids = pool
    engine.state = ConnectionState.CONNECTING

    engine._on_connack(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))

    assert pool.release_calls == 0
    assert pool.clear_calls == 1
    assert len(pool) == 0
    assert (
        tuple(store.get_out(summary.mid) for page in store.out_summary_pages() for summary in page)
        == ()
    )
    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0


def test_missing_session_preserves_queued_packet_ids() -> None:
    store = MemoryInflightStore()
    store.put_out(_outbound(23, OutboundQoSState.QUEUED))
    engine = ProtocolEngine(store=store)
    pool = TrackingPacketIdPool()
    pool.reserve(23)
    engine.packet_ids = pool
    engine.state = ConnectionState.CONNECTING

    engine._on_connack(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))

    assert pool.clear_calls == 0
    assert pool.release_calls == 0
    assert pool.in_use(23)
    assert tuple(
        store.get_out(summary.mid) for page in store.out_summary_pages() for summary in page
    )
