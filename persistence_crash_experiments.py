"""Persistence crash-consistency experiments for MQTTium research agent.

Run: PYTHONPATH=src python persistence_crash_experiments.py
"""
from __future__ import annotations
import sqlite3
import tempfile
from pathlib import Path

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS, ConnectionState
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.types import OutboundMessage, InboundMessage
from mqttium.protocol.effects import EffectKind
from mqttium.packets import PublishPacket, PubAckPacket, PubRecPacket, PubRelPacket, encode_frame
from mqttium.enums import PacketType
from tests.support import feed_engine

def connack(session_present=False):
    flags = 0x01 if session_present else 0x00
    return encode_frame(PacketType.CONNACK, 0, bytes([flags, 0]))

def print_header(title):
    print("\n" + "="*80)
    print(title)
    print("="*80)

# ---- Helpers for fault injection ----

class Boom(Exception):
    pass

class FailingPutStore:
    """Wrap store, fail nth put_out."""
    def __init__(self, store, fail_at=1):
        self._store = store
        self.fail_at = fail_at
        self.calls = 0
    def __getattr__(self, name):
        return getattr(self._store, name)
    def put_out(self, msg):
        self.calls += 1
        if self.calls == self.fail_at:
            raise Boom(f"injected put_out failure at call {self.calls}")
        return self._store.put_out(msg)
    def batch(self):
        return self._store.batch()

class FailingDeleteStore:
    def __init__(self, store, fail_at=1):
        self._store = store
        self.fail_at = fail_at
        self.calls = 0
    def __getattr__(self, name):
        return getattr(self._store, name)
    def delete_out(self, mid):
        self.calls += 1
        if self.calls == self.fail_at:
            raise Boom(f"injected delete_out failure {self.calls}")
        return self._store.delete_out(mid)

class FailingCompleteStore:
    def __init__(self, store):
        self._store = store
        self.fail_next = False
    def __getattr__(self, name):
        return getattr(self._store, name)
    def complete_out(self, mid, expected):
        if self.fail_next:
            self.fail_next = False
            raise Boom("injected complete_out raise")
        return self._store.complete_out(mid, expected)

class FailingPutInStore:
    def __init__(self, store, fail_at=1):
        self._store = store
        self.fail_at = fail_at
        self.calls = 0
    def __getattr__(self, name):
        return getattr(self._store, name)
    def put_in(self, msg):
        self.calls += 1
        if self.calls == self.fail_at:
            raise Boom("injected put_in")
        return self._store.put_in(msg)

# ---- Experiment 1: outbound admission rollback must not leave phantom ----
def experiment_outbound_admission_rollback():
    print_header("EXPERIMENT 1: outbound admission rollback phantom")
    for label, store in [
        ("Memory", MemoryInflightStore()),
        ("SQLite", SqliteInflightStore(Path(tempfile.mktemp(suffix=".db")))),
    ]:
        print(f"\n-- {label} --")
        engine = ProtocolEngine(EngineConfig(), store)
        engine.state = ConnectionState.CONNECTED
        # First successful publish to have baseline
        h1 = engine.queue_publish("a/b", b"one", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        before_mids = sorted(m.mid for m in store.out_items())
        before_pending = engine.pending_outbound_bytes
        before_used = sorted(engine.packet_ids._used)
        before_effects = len(engine._effects)
        before_queued = list(engine._queued)
        print(f" before: mids={before_mids} pending_bytes={before_pending} used={before_used} queued={len(before_queued)}")

        # Now inject failing put on next admission
        # Wrap outbound.store to failing? Need to inject at session level.
        # We use FailingPutStore wrapping underlying store but engine.outbound.store points there
        failing = FailingPutStore(store, fail_at=1)
        # Need to keep original store for verification after, but ensure delete sees same underlying?
        # The rollback's delete_record will call failing.delete_out which delegates to real store.
        # That should remove any partially inserted row if it had been inserted - but we fail BEFORE insert,
        # so no row should exist. This is benign case. Let's test failure AFTER put but before flow?
        # For that we need to fail in _launch after put.
        engine.outbound.store = failing
        # Also need to ensure _paged_store and _transitions point to failing wrapper? They were set at init to original store.
        # We should also patch those to failing so failure path uses same store.
        engine.outbound._paged_store = failing if isinstance(failing, type(store)) else None
        # Actually isinstance check fails for wrapper. So we keep original _paged_store? But delete will still work via outbound.store
        try:
            engine.queue_publish("a/b", b"two", qos=QoS.AT_LEAST_ONCE)
            print("  UNEXPECTED success")
        except Boom as e:
            print(f"  caught Boom: {e}")
        except Exception as e:
            print(f"  caught other: {e}")
        after_mids = sorted(m.mid for m in store.out_items())
        after_pending = engine.pending_outbound_bytes
        after_used = sorted(engine.packet_ids._used)
        print(f"  after: mids={after_mids} pending_bytes={after_pending} used={after_used} queued={len(list(engine._queued))} effects={len(engine._effects)}")
        if after_mids != before_mids or after_pending != before_pending or after_used != before_used:
            print("  *** BUG: leaked state")
        else:
            print("  OK: rollback restored")
        if label=="SQLite":
            store.close()
            try:
                Path(store._path).unlink(missing_ok=True)
            except: pass

def experiment_outbound_admission_after_put():
    print_header("EXPERIMENT 1b: outbound admission failure AFTER durable put")
    # This tests rollback when put succeeded but subsequent launch encode fails
    for label, store in [
        ("Memory", MemoryInflightStore()),
        ("SQLite", SqliteInflightStore(Path(tempfile.mktemp(suffix=".db")))),
    ]:
        print(f"\n-- {label} --")
        engine = ProtocolEngine(EngineConfig(), store)
        engine.state = ConnectionState.CONNECTED
        h1 = engine.queue_publish("a/b", b"one", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        before = {
            "mids": sorted(m.mid for m in store.out_items()),
            "pending": engine.pending_outbound_bytes,
            "used": sorted(engine.packet_ids._used),
            "queued": [m.mid for m in engine._queued],
        }
        print(f" before {before}")
        # Inject failure in _launch after put_out: monkeypatch _launch to raise after put
        orig_launch = engine.outbound._launch
        def failing_launch(self, msg, **kwargs):
            # Simulate put_out succeeded, then encode fails before SEND
            self.store.put_out(msg)  # ensure row exists
            raise Boom("injected launch failure after put")
        import types
        # Patch at class level temporarily via monkey on instance's type
        # Use object.__setattr__ bypass slots? Just patch class attribute for this engine's class
        orig_class_launch = engine.outbound.__class__._launch
        engine.outbound.__class__._launch = failing_launch
        # Need to ensure flow has slot so _try_launch is attempted; it is.
        try:
            engine.queue_publish("a/b", b"two", qos=QoS.AT_LEAST_ONCE)
            print(" unexpected success")
        except Boom as e:
            print(f" caught Boom {e}")
        except Exception as e:
            print(f" caught {type(e).__name__}: {e}")
        after = {
            "mids": sorted(m.mid for m in store.out_items()),
            "pending": engine.pending_outbound_bytes,
            "used": sorted(engine.packet_ids._used),
            "queued": [m.mid for m in engine._queued],
        }
        print(f" after {after}")
        if after != before:
            print(" *** BUG diff")
            print(f"   diff mids before {before['mids']} after {after['mids']}")
            print(f"   pending before {before['pending']} after {after['pending']}")
            print(f"   used before {before['used']} after {after['used']}")
        else:
            print(" OK rollback")
        engine.outbound.__class__._launch = orig_class_launch
        if label=="SQLite":
            store.close()
            Path(store._path).unlink(missing_ok=True)

def experiment_batch_atomicity():
    print_header("EXPERIMENT 2: batch atomicity with put failure mid-chunk")
    for label, store in [
        ("Memory", MemoryInflightStore()),
        ("SQLite", SqliteInflightStore(Path(tempfile.mktemp(suffix=".db")))),
    ]:
        print(f"\n-- {label} --")
        engine = ProtocolEngine(EngineConfig(max_pending_outbound_messages=10), store)
        engine.state = ConnectionState.CONNECTED
        # Use queue_publish_many with failing store on 3rd call
        failing = FailingPutStore(store, fail_at=3)
        engine.outbound.store = failing
        # need to ensure outbound batch uses failing's batch (nullcontext vs real)
        # For memory, failing batch is nullcontext, for sqlite it's real batch. But queue_publish_many uses self.store.batch() which is failing's batch returning sqlite's batch.
        before = sorted(m.mid for m in store.out_items())
        print(f" before mids {before} pending {engine.pending_outbound_bytes} used {sorted(engine.packet_ids._used)}")
        batch = [("t/1", b"a", QoS.AT_LEAST_ONCE, False, None),
                 ("t/2", b"b", QoS.AT_LEAST_ONCE, False, None),
                 ("t/3", b"c", QoS.AT_LEAST_ONCE, False, None),
                 ("t/4", b"d", QoS.AT_LEAST_ONCE, False, None)]
        try:
            engine.queue_publish_many(batch)
            print(" unexpected success")
        except Boom as e:
            print(f" caught Boom {e}")
        except Exception as e:
            print(f" caught {type(e).__name__}: {e} ")
        after = sorted(m.mid for m in store.out_items())
        print(f" after mids {after} pending {engine.pending_outbound_bytes} used {sorted(engine.packet_ids._used)} queued {len(engine._queued)}")
        if after != before:
            print(f" *** LEAK: before {before} after {after}")
            # Check if SQLite rolled back transaction vs Memory not?
            # For SQLite, after should be before because batch rollback + explicit rollback deletes.
            # Let's also check underlying sqlite directly if batch left rows?
            if label=="SQLite":
                # reopen to see durability
                store.close()
                reopened = SqliteInflightStore(store._path)
                reopen_mids = sorted(m.mid for m in reopened.out_items())
                print(f"  reopen mids {reopen_mids}")
                reopened.close()
                Path(store._path).unlink(missing_ok=True)
        else:
            print(" OK no leak")
            if label=="SQLite":
                store.close()
                Path(store._path).unlink(missing_ok=True)

def experiment_delete_failure_phantom():
    print_header("EXPERIMENT 3: delete failure phantom via rollback's delete_record swallow")
    # OutboundSession.delete_record swallows exceptions (pass). So if rollback tries to delete but store fails, it swallows => phantom remains.
    # Test by making delete_out raise during rollback after put failure
    # First, make put succeed then launch fail, rollback will try delete. If delete raises, phantom remains but pending budget already reset wholesale? Let's see outbound._rollback comments: "store rows go through delete_record rather than discard_record for same reason: a transactional store has already rolled its batch back by the time this runs, so per-record sizes are unrecoverable and second per-record release would double-count."
    # So after batch rollback, store already empty, delete_record is no-op. But for non-transactional store (Memory), rows remain and must be deleted.
    # The swallow behavior could hide bug but also leak?
    # Simulate Memory store where delete fails
    class DeleteFailMemory(MemoryInflightStore):
        def delete_out(self, mid):
            raise Boom("delete fail")
    store = DeleteFailMemory()
    engine = ProtocolEngine(EngineConfig(), store)
    engine.state = ConnectionState.CONNECTED
    # Force put success then launch fail, rollback tries delete which fails but is swallowed, pending restored wholesale, packet id released
    # Check store still has phantom?
    before = sorted(m.mid for m in store.out_items())
    print(f" before {before}")
    orig_class_launch = engine.outbound.__class__._launch
    def failing_launch(self, msg, **kw):
        # do actual put
        store.put_out(msg)
        raise Boom("launch fail after put")
    engine.outbound.__class__._launch = failing_launch
    engine.outbound.store = store
    try:
        engine.queue_publish("a/b", b"x", qos=QoS.AT_LEAST_ONCE)
    except Boom:
        print(" caught Boom")
    after = sorted(m.mid for m in store.out_items())
    print(f" after mids {after} pending {engine.pending_outbound_bytes} used {sorted(engine.packet_ids._used)}")
    if after != before:
        print(" *** PHANTOM due to delete swallow")
    else:
        print(" OK no phantom despite delete failure (maybe pending wholesale hides it but store still has row?)")
    engine.outbound.__class__._launch = orig_class_launch

def experiment_settlement_budget_leak():
    print_header("EXPERIMENT 4: settlement budget leak with unknown logical_size")
    # Create store with legacy row logical_size 0
    # Populate via direct put with logical_size 0
    for label in ["Memory", "SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
            msg = OutboundMessage(mid=1, topic="a/b", payload=b"hello", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=0)
            store.put_out(msg)
            engine = ProtocolEngine(EngineConfig(), store)
            # hydration should have pending_bytes computed from recomputed size, but transitions set_out not called? In memory, set_out just updates in-memory logical_size
            # Let's check pending after hydrate
            print(f" after hydrate pending_bytes={engine.pending_outbound_bytes} pending_messages={engine.pending_outbound_messages}")
            print(f" store logical_size after hydrate={store.get_out(1).logical_size}")
            # Now simulate PUBACK settlement
            engine.state = ConnectionState.CONNECTED
            feed_engine(engine, PubAckPacket(mid=1).encode())
            engine.take_effects()
            print(f" after PUBACK pending_bytes={engine.pending_outbound_bytes} store empty? {list(store.out_items())}")
            if engine.pending_outbound_bytes != 0:
                print(" *** BUG leak: pending_bytes not zero despite settlement")
            else:
                print(" OK")
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
            # Insert legacy row with logical_size 0 directly via sql? Or put_out with 0 then close and reopen with logic that should backfill during hydrate
            msg = OutboundMessage(mid=1, topic="a/b", payload=b"hello", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=0)
            store.put_out(msg)
            # Also create inbound legacy?
            store.close()
            # Reopen as new engine to trigger hydration backfill via set_out_logical_size inside batch
            store2 = SqliteInflightStore(path)
            engine = ProtocolEngine(EngineConfig(), store2)
            print(f" after hydrate pending_bytes={engine.pending_outbound_bytes}")
            stored = store2.get_out(1)
            print(f" stored logical_size after hydrate={stored.logical_size if stored else None}")
            # Before fix, pending_bytes includes recomputed size, stored maybe updated? Let's check sqlite behavior: hydrate does with batch: for each page, _hydrate_message calls set_out_logical_size if unknown_size and transitions else
            # So it should have persisted.
            engine.state = ConnectionState.CONNECTED
            feed_engine(engine, PubAckPacket(mid=1).encode())
            engine.take_effects()
            print(f" after PUBACK pending_bytes={engine.pending_outbound_bytes} store items {list(store2.out_items())}")
            if engine.pending_outbound_bytes != 0:
                print(" *** BUG leak")
            else:
                print(" OK")
            store2.close()
            Path(path).unlink(missing_ok=True)

def experiment_inbound_manual_ack_drain():
    print_header("EXPERIMENT 5: inbound manual ack drain interruption")
    for label in ["Memory", "SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(manual_ack=True), store)
        engine.state = ConnectionState.CONNECTED
        # Simulate inbound QoS1 messages 1,2,3
        for mid in [1,2,3]:
            feed_engine(engine, PublishPacket(topic="t", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, mid=mid).encode())
        engine.take_effects()
        print(f" after ingress stored={sorted(m.mid for m in store.in_items())} order={list(engine.inbound._manual_qos1_order)} inflight={engine.inbound._inflight} pending_bytes={engine.inbound._pending_bytes}")
        # Now ack out of order: ack 2 then 1 should drain 1 then 2
        # Simulate crash mid-drain: patch complete_in to fail on second delete
        call_count = 0
        saved_memory = MemoryInflightStore.complete_in
        saved_sqlite = SqliteInflightStore.complete_in
        def failing_memory(self, mid, expected):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Boom("crash after first")
            return saved_memory(self, mid, expected)
        def failing_sqlite(self, mid, expected):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise Boom("crash after first")
            return saved_sqlite(self, mid, expected)
        # Patch at class level for this iteration only
        if label=="Memory":
            MemoryInflightStore.complete_in = failing_memory
            patched_class = MemoryInflightStore
            patched_orig = saved_memory
        else:
            SqliteInflightStore.complete_in = failing_sqlite
            patched_class = SqliteInflightStore
            patched_orig = saved_sqlite
        try:
            engine.ack(2)  # puts 2 in pending, drain not yet because order[0]==1 not ready
            print(f" after ack 2 pending_set={engine.inbound._pending_manual_qos1_acks} order={list(engine.inbound._manual_qos1_order)} stored={sorted(m.mid for m in store.in_items())}")
            engine.ack(1)  # should drain 1 and 2, but second will Boom
            print(" unexpected success")
        except Boom as e:
            print(f" caught Boom during drain: {e}")
        except Exception as e:
            print(f" caught {type(e).__name__}: {e}")
        stored_after = sorted(m.mid for m in store.in_items())
        print(f" after partial drain stored={stored_after} order={list(engine.inbound._manual_qos1_order)} pending_set={engine.inbound._pending_manual_qos1_acks} inflight={engine.inbound._inflight} pending_bytes={engine.inbound._pending_bytes}")
        # Restore patch before crash-reopen so new engine sees clean store logic
        patched_class.complete_in = patched_orig
        # Now simulate crash/reopen: discard engine, reopen store
        if label=="Memory":
            # Memory is lost on crash, so simulate by creating new engine with same store object? But memory store object still has partial state
            # Actually crash would lose in-memory order and pending_set, but store retains 2 and 3 (1 deleted)
            # Rehydrate new engine from same store to see recovered order
            new_engine = ProtocolEngine(EngineConfig(manual_ack=True), store)
            print(f"  REOPEN recovered order={list(new_engine.inbound._manual_qos1_order)} recovered_mids={new_engine.inbound._recovered_mids} stored={sorted(m.mid for m in store.in_items())} inflight={new_engine.inbound._inflight}")
            # The pending ack for 2 was lost (volatile). User must ack again? If they ack 2 again, will it work?
            new_engine.state = ConnectionState.CONNECTED
            try:
                new_engine.ack(2)
                new_engine.take_effects()
                print(f"   after re-ack 2 stored={sorted(m.mid for m in store.in_items())} send={ [e.kind for e in new_engine.take_effects()]}")
            except Exception as e:
                print(f"   re-ack failed {e}")
            # Check if duplicate delivery via replay happens?
            # Simulate reconnect replay
            new_engine.inbound.replay_session()
            effects = new_engine.take_effects()
            replay_mids = [e.data.mid for e in effects if e.kind==EffectKind.MESSAGE]
            print(f"   replay after reopen mids={replay_mids}")
        else:
            # SQLite: close and reopen
            store.close()
            store2 = SqliteInflightStore(path)
            engine2 = ProtocolEngine(EngineConfig(manual_ack=True), store2)
            print(f"  REOPEN file store mids={sorted(m.mid for m in store2.in_items())} recovered_order={list(engine2.inbound._manual_qos1_order)} recovered_mids={engine2.inbound._recovered_mids}")
            engine2.state = ConnectionState.CONNECTED
            try:
                engine2.ack(2)
                engine2.take_effects()
                print(f"   after re-ack2 stored={sorted(m.mid for m in store2.in_items())}")
            except Exception as e:
                print(f"   re-ack failed {e}")
            engine2.inbound.replay_session()
            effects = engine2.take_effects()
            print(f"   replay mids {[e.data.mid if hasattr(e.data,'mid') else e.data for e in effects if e.kind==EffectKind.MESSAGE]}")
            store2.close()
            Path(path).unlink(missing_ok=True)

def experiment_replay_page_deletion():
    print_header("EXPERIMENT 6: replay page boundary deletion")
    # Test out_pages snapshot behavior and inbound replay bounded pages skip
    for label in ["Memory", "SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        with store.batch():
            for mid in range(1, 7):
                store.put_out(OutboundMessage(mid=mid, topic="t", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.QUEUED, logical_size=2))
        # Now iterate pages size 2, delete mid 3 after first page
        pages = store.out_pages(page_size=2)
        first = next(pages)
        print(f" first page {[m.mid for m in first]}")
        store.delete_out(3)
        print(f" deleted 3")
        rest = [m.mid for page in pages for m in page]
        print(f" rest {rest}")
        if 3 in rest:
            print(" *** BUG resurrected deleted")
        else:
            print(" OK deleted skipped")
        # Inbound replay bounded test similar
        if label=="Memory":
            store2 = MemoryInflightStore()
        else:
            store2 = SqliteInflightStore(Path(tempfile.mktemp(suffix=".db")) if label=="Memory" else Path(tempfile.mktemp(suffix=".db")))
            # Actually for sqlite inbound test use separate store
            if label=="SQLite":
                store2.close()
                path2 = Path(tempfile.mktemp(suffix=".db"))
                store2 = SqliteInflightStore(path2)
            else:
                pass
        # Fill inbound
        with store2.batch():
            for mid in range(1, 7):
                store2.put_in(InboundMessage(mid=mid, topic="t", payload=b"x"*100, qos=QoS.EXACTLY_ONCE, retain=False, state=InboundQoSState.WAIT_PUBREL, delivered=False, logical_size=102))
        # Use paged replay with manual engine
        engine = ProtocolEngine(EngineConfig(manual_ack=True), store2)
        # engine inbound already loaded, but we test direct store paging
        # Simulate bounded replay pages iteration with deletion mid-way
        pages = store2.in_replay_pages(max_messages=2, max_bytes=1000) if hasattr(store2, 'in_replay_pages') else store2.in_pages(page_size=2)
        first = next(pages)
        print(f" inbound first page {[m.mid for m in first]}")
        # delete a future mid
        store2.pop_in(5)
        print(" deleted inbound 5")
        rest = [m.mid for page in pages for m in page]
        print(f" inbound rest {rest}")
        if 5 in rest:
            print(" *** BUG inbound resurrected")
        else:
            print(" OK inbound skipped")
        if label=="SQLite":
            store.close()
            Path(store._path).unlink(missing_ok=True)
            store2.close()
            if 'path2' in locals():
                Path(path2).unlink(missing_ok=True)

def experiment_session_present_purge():
    print_header("EXPERIMENT 7: Session Present purge vs resume")
    # Test outbound purge_after_clean_session must delete WAIT_* but keep QUEUED
    for label in ["Memory", "SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(clean_start=False), store)
        engine.state = ConnectionState.CONNECTED
        # Publish 2 messages: one will be launched (WAIT_PUBACK), one will be queued (QUEUED) by filling flow
        # Set small flow limit to force queue
        engine.flow._limit = 1
        h1 = engine.queue_publish("a/b", b"x", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        print(f" after first publish state {store.get_out(h1.mid).state} flow {engine.flow.inflight} queued {len(engine._queued)}")
        h2 = engine.queue_publish("a/b", b"y", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        print(f" after second publish queued? state {store.get_out(h2.mid).state} queued {[m.mid for m in engine._queued]}")
        # Also inbound QoS2
        engine2 = ProtocolEngine(EngineConfig(manual_ack=True, clean_start=False), store)
        # Need separate engine for inbound? Use same engine but feed publish inbound
        feed_engine(engine, PublishPacket(topic="in/qos2", payload=b"p", qos=QoS.EXACTLY_ONCE, retain=False, dup=False, mid=10).encode())
        engine.take_effects()
        print(f" inbound stored {sorted(m.mid for m in store.in_items())} inflight {engine._inbound_inflight} session_state_qos2 {engine.inbound._session_state_qos2}")
        # Now simulate CONNACK session_present=False => purge
        # Engine's outbound has pending messages 2, inbound 1
        print(f" before purge store out {sorted(m.mid for m in store.out_items())} in {sorted(m.mid for m in store.in_items())}")
        # Directly call purge + discard as engine would
        engine.outbound.purge_after_clean_session(sub_mids_pending=False)
        engine.inbound.discard_session()
        print(f" after purge out {sorted(m.mid for m in store.out_items())} in {sorted(m.mid for m in store.in_items())} pending_messages {engine.pending_outbound_messages} inflight {engine.flow.inflight}")
        # Check that WAIT_* removed but QUEUED kept? In our scenario h1 is WAIT_PUBACK, h2 is QUEUED -> after purge, h1 should be gone, h2 remains
        # Let's see: _queued before purge contained h2? Actually after second publish, queued contains h2 (QUEUED). purge should keep QUEUED entries but delete WAIT.
        # Verify
        if h1.mid in [m.mid for m in store.out_items()]:
            print(" *** BUG phantom WAIT_PUBACK not purged")
        else:
            print(" OK WAIT purged")
        if h2.mid in [m.mid for m in store.out_items()]:
            print(" OK QUEUED kept")
        else:
            print(" *** BUG QUEUED lost")
        if engine.inbound._stored_inbound != 0:
            print(" *** BUG inbound not cleared")
        else:
            print(" OK inbound cleared")
        # Now test reopen after purge: new engine should hydrate with only QUEUED
        if label=="Memory":
            new_engine = ProtocolEngine(EngineConfig(clean_start=False), store)
            print(f" reopen queued {[m.mid for m in new_engine._queued]} pending {new_engine.pending_outbound_messages}")
        else:
            store.close()
            store2 = SqliteInflightStore(path)
            new_engine = ProtocolEngine(EngineConfig(clean_start=False), store2)
            print(f" reopen queued {[m.mid for m in new_engine._queued]} pending {new_engine.pending_outbound_messages} store {sorted(m.mid for m in store2.out_items())}")
            store2.close()
            Path(path).unlink(missing_ok=True)

def experiment_close_inside_batch():
    print_header("EXPERIMENT 8: close inside batch and nested batch failure")
    path = Path(tempfile.mktemp(suffix=".db"))
    store = SqliteInflightStore(path)
    # test nested batch failure -> outer rollback
    print(" nested batch rollback-only test")
    try:
        with store.batch():
            try:
                with store.batch():
                    store.put_out(OutboundMessage(mid=1, topic="a", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=2))
                    raise ValueError("inner")
            except ValueError:
                pass
    except RuntimeError as e:
        print(f" outer raised RuntimeError as expected: {e}")
    print(f" after nested failure store contains {list(store.out_items())} (should be empty)")
    if len(list(store.out_items())) != 0:
        print(" *** BUG inner row persisted despite rollback-only")
    else:
        print(" OK rolled back")
    # test close inside batch should raise
    try:
        with store.batch():
            store.put_out(OutboundMessage(mid=2, topic="b", payload=b"y", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=2))
            store.close()
            print(" *** BUG close inside batch didn't raise")
    except RuntimeError as e:
        print(f" close inside batch raised as expected: {e}")
    except Exception as e:
        print(f" close raised other {e}")
    # cleanup if still open
    try:
        store.close()
    except: pass
    Path(path).unlink(missing_ok=True)
    # Test Memory batch no transaction: nested failure should NOT rollback outer? Actually Memory batch is nullcontext, so no rollback.
    print("\n Memory nested batch (no txn) should NOT rollback")
    mem = MemoryInflightStore()
    try:
        with mem.batch():
            try:
                with mem.batch():
                    mem.put_out(OutboundMessage(mid=10, topic="a", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=2))
                    raise ValueError("inner")
            except ValueError:
                pass
    except RuntimeError:
        print(" unexpected rollback RuntimeError for memory")
    print(f" after memory nested failure contains {[m.mid for m in mem.out_items()]} (should contain 10 because memory has no txn)")
    if 10 in [m.mid for m in mem.out_items()]:
        print(" Memory correctly retains (no rollback) - divergence from SQLite expected")
    else:
        print(" Memory also rolled back?")

def experiment_reconnect_replay_drain_interleaved():
    print_header("EXPERIMENT 9: reconnect replay drain with flow limit")
    # Simulate outbound replay with limited flow: some messages queued not retransmitted, must not be lost on next reconnect
    for label in ["Memory", "SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(max_outbound_inflight=1, local_receive_maximum=10), store)
        # Create 3 persisted outbound messages in WAIT_PUBACK state manually
        with store.batch():
            for mid in [1,2,3]:
                store.put_out(OutboundMessage(mid=mid, topic="t", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=2))
        # Now new engine hydrates (should reserve pending and flow slots)
        engine2 = ProtocolEngine(EngineConfig(max_outbound_inflight=1, local_receive_maximum=10), store)
        print(f" after hydrate pending={engine2.pending_outbound_messages} inflight={engine2.flow.inflight} queued pre-replay {len(engine2._queued)}")
        engine2.state = ConnectionState.CONNECTED
        # Simulate CONNACK with session present true -> replay_session
        from mqttium.protocol.negotiated import NegotiatedSettings
        engine2.negotiated = NegotiatedSettings(receive_maximum=1)  # broker allows 1 inflight
        engine2.outbound.replay_session()
        print(f" after replay_session queued {[m.mid if hasattr(m,'mid') else m for m in engine2._queued]} inflight {engine2.flow.inflight} effects SEND count {len([e for e in engine2.take_effects() if e.kind==EffectKind.SEND])}")
        # With limit 1, only one retransmission should happen, others queued
        # Drain should have left queued with remaining
        # Now simulate transport closed before queued drained, then reconnect again
        # The queued messages should survive store (they are WAIT_PUBACK but were queued due to flow)
        # But replay_session next time should again attempt.
        # Check store still has all 3
        print(f" store after first replay {sorted(m.mid for m in store.out_items())} states {[ (m.mid, m.state) for m in store.out_items()]}")
        # Now new engine again (crash)
        engine3 = ProtocolEngine(EngineConfig(max_outbound_inflight=1, local_receive_maximum=10), store)
        engine3.state = ConnectionState.CONNECTED
        engine3.negotiated = NegotiatedSettings(receive_maximum=10)  # now larger window
        engine3.outbound.replay_session()
        sends = [e for e in engine3.take_effects() if e.kind==EffectKind.SEND]
        print(f" second reconnect with larger window SENDs {len(sends)} queued {[m.mid for m in engine3._queued]}")
        if len(sends) == 3:
            print(" OK all 3 retransmitted after window increase")
        else:
            print(f" *** maybe bug: expected 3 but got {len(sends)}")
        if label=="SQLite":
            store.close()
            Path(path).unlink(missing_ok=True)

def experiment_inbound_byte_leak_after_delivered_crash():
    print_header("EXPERIMENT 10: inbound byte accounting leak on interrupted delivered mark")
    for label in ["Memory", "SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(manual_ack=True, max_pending_inbound_bytes=1000), store)
        engine.state = ConnectionState.CONNECTED
        # Put inbound QoS1 message
        feed_engine(engine, PublishPacket(topic="t", payload=b"x"*100, qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, mid=5).encode())
        effects = engine.take_effects()
        msg_effect = [e for e in effects if e.kind==EffectKind.MESSAGE][0]
        mid = msg_effect.data.mid
        print(f" after ingress pending_bytes={engine.inbound._pending_bytes} stored={sorted(m.mid for m in store.in_items())} delivered flag before mark {store.get_in(mid).delivered if hasattr(store,'get_in') else 'n/a'}")
        # Simulate delivery without marking? The normal path would call mark_delivered after delivery.
        # We will mark delivered then check pending_bytes: manual ack doesn't release bytes until ack completes. But marking doesn't release bytes? Actually _release does only on ack.
        # So pending should remain. Let's just call mark_delivered and check deliv flag
        engine.inbound.mark_delivered(mid)
        print(f" after mark delivered flag {store.get_in(mid).delivered if store.get_in(mid) else 'none'} pending_bytes still {engine.inbound._pending_bytes}")
        # Now ack to release
        engine.ack(mid)
        engine.take_effects()
        print(f" after ack pending_bytes {engine.inbound._pending_bytes} inflight {engine.inbound._inflight} stored {sorted(m.mid for m in store.in_items())}")
        if engine.inbound._pending_bytes != 0:
            print(" *** BUG leak")
        else:
            print(" OK")
        if label=="SQLite":
            store.close()
            Path(path).unlink(missing_ok=True)

if __name__ == "__main__":
    experiment_outbound_admission_rollback()
    experiment_outbound_admission_after_put()
    experiment_batch_atomicity()
    experiment_delete_failure_phantom()
    experiment_settlement_budget_leak()
    experiment_inbound_manual_ack_drain()
    experiment_replay_page_deletion()
    experiment_session_present_purge()
    experiment_close_inside_batch()
    experiment_reconnect_replay_drain_interleaved()
    experiment_inbound_byte_leak_after_delivered_crash()
    print("\nDone")
