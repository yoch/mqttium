from pathlib import Path

engine_path = Path("src/mqttium/protocol/engine.py")
text = engine_path.read_text()

replacements = [
    (
        """        handles: list[PublishHandle] = []
        try:
""",
        """        packet_ids_empty_start = len(self.packet_ids) == 0
        handles: list[PublishHandle] = []
        try:
""",
    ),
    (
        """            for handle in handles:
                if handle.mid is None:
                    continue
                self._delete_outbound_store_record(handle.mid)
                self.packet_ids.release(handle.mid)
            self._pending_outbound_messages = pending_messages_start
""",
        """            for handle in handles:
                if handle.mid is None:
                    continue
                self._delete_outbound_store_record(handle.mid)
                if not packet_ids_empty_start:
                    self.packet_ids.release(handle.mid)
            if packet_ids_empty_start:
                # Every live MID was allocated by this failed atomic batch.
                self.packet_ids.clear()
            self._pending_outbound_messages = pending_messages_start
""",
    ),
    (
        """        # Release sub/unsub MIDs still in flight — no ACK will arrive now.
        for mid in self._pending_sub_mids:
            self.packet_ids.release(mid)
        self._pending_sub_mids.clear()
""",
        """        # Release sub/unsub MIDs still in flight — no ACK will arrive now.
        if self._pending_sub_mids:
            if self._pending_outbound_messages == 0:
                # No publish MID survives this transport: reset in constant time.
                self.packet_ids.clear()
            else:
                for mid in self._pending_sub_mids:
                    self.packet_ids.release(mid)
            self._pending_sub_mids.clear()
""",
    ),
    (
        """        if not connack.session_present:
            for page in self._outbound_store_summary_pages():
""",
        """        if not connack.session_present:
            # _queued mirrors every outbound record that must survive a missing
            # broker session. With no queued or SUB/UNSUB work, all packet ids
            # belong to the inflight records discarded below.
            clear_abandoned_packet_ids = not self._queued and not self._pending_sub_mids
            for page in self._outbound_store_summary_pages():
""",
    ),
    (
        """                    self._complete_outbound_record(msg.mid, msg)
                    self.packet_ids.release(msg.mid)
                    self._emit(
""",
        """                    self._complete_outbound_record(msg.mid, msg)
                    if not clear_abandoned_packet_ids:
                        self.packet_ids.release(msg.mid)
                    self._emit(
""",
    ),
    (
        """            self.store.clear_in()
            self._recovered_inbound_mids.clear()
""",
        """            if clear_abandoned_packet_ids:
                self.packet_ids.clear()
            self.store.clear_in()
            self._recovered_inbound_mids.clear()
""",
    ),
]

for old, new in replacements:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one engine replacement, found {count}: {old[:80]!r}")
    text = text.replace(old, new)

engine_path.write_text(text)

Path("tests/unit/test_packet_id_clear_paths.py").write_text('''"""Use constant-time PacketIdPool.clear only when no MID can survive."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, OutboundQoSState, PacketType, QoS
from mqttium.errors import ProtocolError
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


def test_failed_batch_clears_packet_ids_when_pool_started_empty() -> None:
    engine, pool = _tracking_engine()

    with pytest.raises(ProtocolError):
        engine.queue_publish_many(
            [
                ("ok", b"x", QoS.AT_LEAST_ONCE, False, None),
                ("bad/#", b"x", QoS.AT_LEAST_ONCE, False, None),
            ]
        )

    assert pool.release_calls == 0
    assert pool.clear_calls == 1
    assert len(pool) == 0
    assert pool.allocate() == 1
    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0
    assert tuple(engine.store.out_items()) == ()


def test_failed_batch_preserves_packet_ids_that_predate_it() -> None:
    engine, pool = _tracking_engine()
    existing_mid = pool.allocate()

    with pytest.raises(ProtocolError):
        engine.queue_publish_many(
            [
                ("ok", b"x", QoS.AT_LEAST_ONCE, False, None),
                ("bad/#", b"x", QoS.AT_LEAST_ONCE, False, None),
            ]
        )

    assert pool.clear_calls == 0
    assert pool.release_calls == 1
    assert pool.in_use(existing_mid)
    assert len(pool) == 1


def test_transport_close_clears_a_sub_only_pool() -> None:
    engine, pool = _tracking_engine()
    engine.state = ConnectionState.CONNECTED
    engine._pending_sub_mids.update((pool.allocate(), pool.allocate()))

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
    engine._pending_outbound_messages = 1
    engine._pending_sub_mids.add(sub_mid)

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
    engine._pending_connect = True

    engine._on_connack(RawPacket(PacketType.CONNACK, 0, b"\\x00\\x00"))

    assert pool.release_calls == 0
    assert pool.clear_calls == 1
    assert len(pool) == 0
    assert tuple(store.out_items()) == ()
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
    engine._pending_connect = True

    engine._on_connack(RawPacket(PacketType.CONNACK, 0, b"\\x00\\x00"))

    assert pool.clear_calls == 0
    assert pool.release_calls == 0
    assert pool.in_use(23)
    assert tuple(store.out_items())
''')
