"""Regression for BUG-1 drain phantom: transition+delete failure must not create phantom."""
from pathlib import Path
import tempfile
import pytest
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.enums import ConnectionState, OutboundQoSState, QoS
from mqttium.protocol.effects import EffectKind
from mqttium.packets import PubAckPacket
from tests.support import feed_engine

class Boom(Exception):
    pass

class FaultMemoryStore(MemoryInflightStore):
    def __init__(self):
        super().__init__()
        self.fail_mid = None
    def transition_out(self, mid, expected, new_state, *, compact=False):
        if self.fail_mid == mid:
            self.fail_mid = None
            raise Boom("transition fail")
        return super().transition_out(mid, expected, new_state, compact=compact)
    def delete_out(self, mid):
        if self.fail_mid == mid:
            raise Boom("delete fail")
        return super().delete_out(mid)

class FaultSqliteStore(SqliteInflightStore):
    def __init__(self, path):
        super().__init__(path)
        self.fail_mid = None
    def transition_out(self, mid, expected, new_state, *, compact=False):
        if self.fail_mid == mid:
            self.fail_mid = None
            raise Boom("transition fail")
        return super().transition_out(mid, expected, new_state, compact=compact)
    def delete_out(self, mid):
        if self.fail_mid == mid:
            raise Boom("delete fail")
        return super().delete_out(mid)

def make_queued_qos1(store):
    cfg = EngineConfig(local_receive_maximum=1, max_outbound_inflight=1)
    engine = ProtocolEngine(cfg, store=store)
    engine.state = ConnectionState.CONNECTED
    engine.flow._limit = 1
    h1 = engine.queue_publish("a/b", b"first", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    h2 = engine.queue_publish("a/b", b"second", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    assert store.get_out(h2.mid).state == OutboundQoSState.QUEUED
    return engine, h1, h2

def test_drain_transition_and_delete_failure_does_not_create_phantom_memory():
    store = FaultMemoryStore()
    engine, h1, h2 = make_queued_qos1(store)
    # Inject fault for h2: transition fails, then delete also fails (same mid)
    # We need delete to fail on same mid as transition's discard
    # Our store fails only once per fail_mid flag, so we need to make both fail.
    # We will make transition fail first, then set fail again for delete.
    store.fail_mid = h2.mid
    # Monkey patch delete to fail as well on same mid after transition
    orig_delete = store.delete_out
    def failing_delete(mid):
        if mid == h2.mid:
            # first call after transition is delete in drain's discard
            raise Boom("delete fail")
        return orig_delete(mid)
    # Instead we can set fail_mid again inside transition? Simpler: make transition set fail_mid for delete
    # We'll use a wrapper that fails transition then leaves fail_mid set for delete
    original_transition = store.transition_out
    def trans(mid, exp, new, *, compact=False):
        if mid == h2.mid:
            # fail transition, but keep fail for delete
            store.fail_mid = h2.mid  # will cause next delete to fail
            raise Boom("transition fail")
        return original_transition(mid, exp, new, compact=compact)
    store.transition_out = trans
    store.delete_out = failing_delete

    # Now PUBACK h1 to free flow and trigger drain of h2
    feed_engine(engine, PubAckPacket(mid=h1.mid).encode())
    effects = engine.take_effects()
    # Should have PUBLISH_COMPLETE for h1, no PUBLISH_FAILED for h2 (since durable may remain)
    assert any(e.kind == EffectKind.PUBLISH_COMPLETE and e.data == h1.mid for e in effects)
    assert not any(e.kind == EffectKind.PUBLISH_FAILED and e.data.mid == h2.mid for e in effects)
    # Invariant: must not have phantom with released reservation/id
    # If durable remains, pending and packet id must still be held
    assert store.get_out(h2.mid) is not None, "durable row must remain (transient failure) or be gone (success) but not phantom with released accounting"
    # After fix, pending should still count h2, and packet id still reserved, and queued should still contain h2 for retry
    # Check that we did NOT release reservation/id while durable exists
    if store.get_out(h2.mid) is not None:
        assert engine.pending_outbound_messages == 1, "reservation must still be held if durable remains"
        assert h2.mid in engine.packet_ids._used, "packet id must still be held"
        assert any(m.mid == h2.mid for m in engine._queued) or engine._queued, "queued should still contain failed message for retry"

def test_drain_transition_and_delete_failure_does_not_create_phantom_sqlite(tmp_path: Path):
    path = tmp_path / "phantom.db"
    store = FaultSqliteStore(path)
    engine, h1, h2 = make_queued_qos1(store)
    original_transition = store.transition_out
    orig_delete = store.delete_out
    def trans(mid, exp, new, *, compact=False):
        if mid == h2.mid:
            store.fail_mid = h2.mid
            raise Boom("transition fail")
        return original_transition(mid, exp, new, compact=compact)
    def failing_delete(mid):
        if mid == h2.mid:
            raise Boom("delete fail")
        return orig_delete(mid)
    store.transition_out = trans
    store.delete_out = failing_delete
    feed_engine(engine, PubAckPacket(mid=h1.mid).encode())
    effects = engine.take_effects()
    assert any(e.kind == EffectKind.PUBLISH_COMPLETE and e.data == h1.mid for e in effects)
    assert not any(e.kind == EffectKind.PUBLISH_FAILED and getattr(e.data, 'mid', None) == h2.mid for e in effects)
    assert store.get_out(h2.mid) is not None
    if store.get_out(h2.mid) is not None:
        assert engine.pending_outbound_messages == 1
        assert h2.mid in engine.packet_ids._used
    store.close()

def test_sqlite_realistic_operational_error(tmp_path: Path):
    """Characterization with realistic sqlite3.OperationalError."""
    import sqlite3
    path = tmp_path / "realistic.db"
    base = SqliteInflightStore(path)
    # Create queued
    cfg = EngineConfig(local_receive_maximum=1, max_outbound_inflight=1)
    engine = ProtocolEngine(cfg, store=base)
    engine.state = ConnectionState.CONNECTED
    engine.flow._limit = 1
    h1 = engine.queue_publish("a/b", b"first", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    h2 = engine.queue_publish("a/b", b"second", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    # Wrap to raise realistic OperationalError on transition and delete for h2
    orig_trans = base.transition_out
    orig_del = base.delete_out
    def trans(mid, exp, new, *, compact=False):
        if mid == h2.mid:
            raise sqlite3.OperationalError("disk I/O error")
        return orig_trans(mid, exp, new, compact=compact)
    def dele(mid):
        if mid == h2.mid:
            raise sqlite3.OperationalError("disk I/O error")
        return orig_del(mid)
    base.transition_out = trans
    base.delete_out = dele
    feed_engine(engine, PubAckPacket(mid=h1.mid).encode())
    effects = engine.take_effects()
    # Should not create phantom
    assert base.get_out(h2.mid) is not None
    # Pending and packet id still held
    assert engine.pending_outbound_messages == 1
    assert h2.mid in engine.packet_ids._used
    base.close()

def test_qos2_drain_phantom(tmp_path: Path):
    store = FaultMemoryStore()
    cfg = EngineConfig(local_receive_maximum=1, max_outbound_inflight=1)
    engine = ProtocolEngine(cfg, store=store)
    engine.state = ConnectionState.CONNECTED
    engine.flow._limit = 1
    h1 = engine.queue_publish("a/b", b"first", qos=QoS.EXACTLY_ONCE)
    engine.take_effects()
    h2 = engine.queue_publish("a/b", b"second", qos=QoS.EXACTLY_ONCE)
    engine.take_effects()
    assert store.get_out(h2.mid).state == OutboundQoSState.QUEUED
    # Drive h1 to completion to free flow: need PUBREC and PUBCOMP
    from mqttium.packets import PubRecPacket, PubCompPacket
    # First make h1 succeed to free flow, but inject fault for h2 drain
    orig_trans = store.transition_out
    orig_del = store.delete_out
    def trans(mid, exp, new, *, compact=False):
        if mid == h2.mid:
            raise Boom("transition")
        return orig_trans(mid, exp, new, compact=compact)
    def dele(mid):
        if mid == h2.mid:
            raise Boom("delete")
        return orig_del(mid)
    store.transition_out = trans
    store.delete_out = dele
    feed_engine(engine, PubRecPacket(mid=h1.mid).encode())
    engine.take_effects()
    feed_engine(engine, PubCompPacket(mid=h1.mid).encode())
    effects = engine.take_effects()
    # h2 should not be failed
    assert not any(e.kind == EffectKind.PUBLISH_FAILED and getattr(e.data, 'mid', None) == h2.mid for e in effects)
    assert store.get_out(h2.mid) is not None
    assert engine.pending_outbound_messages == 1
