"""Additional crash-reopen experiments."""
from pathlib import Path
import tempfile
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.enums import InboundQoSState, OutboundQoSState, QoS, ConnectionState
from mqttium.types import OutboundMessage, InboundMessage
from mqttium.protocol.effects import EffectKind
from mqttium.packets import PublishPacket, PubRecPacket, PubRelPacket, PubAckPacket, ConnAckPacket, encode_frame
from mqttium.enums import PacketType
from tests.support import feed_engine
from mqttium.protocol.negotiated import NegotiatedSettings

class Boom(Exception): pass

def print_header(t): print("\n"+"="*80+"\n"+t+"\n"+"="*80)

def experiment_inbound_replay_interrupted():
    print_header("ADDITIONAL 1: inbound bounded replay interrupted mid-stream")
    for label in ["Memory","SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        # create 130 inbound QoS2 messages (exceeds REPLAY_BATCH_MESSAGES=64 and REPLAY_BATCH_BYTES=1M)
        # Use small payload so message count bound triggers
        with store.batch():
            for mid in range(1, 131):
                store.put_in(InboundMessage(mid=mid, topic="t", payload=b"x"*10, qos=QoS.EXACTLY_ONCE, retain=False, state=InboundQoSState.WAIT_PUBREL, delivered=False, logical_size=12))
        # Now create engine, which will load recovered_mids etc but not yet replay
        engine = ProtocolEngine(EngineConfig(manual_ack=False), store)
        print(f" after hydrate recovered_mids={len(engine.inbound._recovered_mids)} pending_bytes={engine.inbound._pending_bytes} stored={len(list(store.in_items()))}")
        engine.state = ConnectionState.CONNECTED
        # Simulate CONNACK session_present true -> replay_session starts first batch
        engine.inbound.replay_session()
        effects1 = engine.take_effects()
        batch1 = [e for e in effects1 if e.kind==EffectKind.MESSAGE]
        has_continue = any(e.kind==EffectKind.CONTINUE_INBOUND_REPLAY for e in effects1)
        print(f" first batch size {len(batch1)} has_continue={has_continue} replay_pending={engine.inbound.replay_pending}")
        # Check that batch is bounded (should be 64)
        if len(batch1) != 64:
            print(f" *** unexpected batch size, expected 64 got {len(batch1)}")
        # Now simulate crash before continuing: discard engine without draining continue
        # Reopen fresh engine from same store (crash)
        if label=="Memory":
            engine2 = ProtocolEngine(EngineConfig(manual_ack=False), store)
            print(f" reopen after crash pending_bytes={engine2.inbound._pending_bytes} recovered={len(engine2.inbound._recovered_mids)}")
            engine2.state = ConnectionState.CONNECTED
            engine2.inbound.replay_session()
            effects2 = engine2.take_effects()
            batch2 = [e for e in effects2 if e.kind==EffectKind.MESSAGE]
            print(f"  reopen first batch size {len(batch2)} has_continue={any(e.kind==EffectKind.CONTINUE_INBOUND_REPLAY for e in effects2)}")
            # Continue until done
            total = len(batch2)
            while engine2.inbound.replay_pending:
                engine2.inbound.drain_replay()
                eff = engine2.take_effects()
                b = [e for e in eff if e.kind==EffectKind.MESSAGE]
                total += len(b)
                has_c = any(e.kind==EffectKind.CONTINUE_INBOUND_REPLAY for e in eff)
                # print intermediate
            print(f"  total after full replay {total} (expected 130 or less if delivered filtering?)")
            if total != 130:
                print(f" *** maybe duplicates/loss: total {total}")
            else:
                print("  OK no loss after interrupted replay")
        else:
            store.close()
            store2 = SqliteInflightStore(path)
            engine2 = ProtocolEngine(EngineConfig(manual_ack=False), store2)
            print(f" reopen stored {len(list(store2.in_items()))} pending_bytes {engine2.inbound._pending_bytes}")
            engine2.state = ConnectionState.CONNECTED
            engine2.inbound.replay_session()
            effects2 = engine2.take_effects()
            batch2 = [e for e in effects2 if e.kind==EffectKind.MESSAGE]
            print(f"  reopen first batch {len(batch2)}")
            total = len(batch2)
            while engine2.inbound.replay_pending:
                engine2.inbound.drain_replay()
                eff = engine2.take_effects()
                b=[e for e in eff if e.kind==EffectKind.MESSAGE]
                total+=len(b)
            print(f"  total {total}")
            if total != 130:
                print(" *** bug")
            else:
                print("  OK")
            store2.close()
            Path(path).unlink(missing_ok=True)

def experiment_compact_transition_durability():
    print_header("ADDITIONAL 2: PUBREC compaction durability crash")
    for label in ["Memory","SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(), store)
        engine.state = ConnectionState.CONNECTED
        # Publish QoS2
        h = engine.queue_publish("a/b", b"payload", qos=QoS.EXACTLY_ONCE)
        mid = h.mid
        engine.take_effects()
        print(f" after publish state {store.get_out(mid).state} topic '{store.get_out(mid).topic}' payload len {len(store.get_out(mid).payload)}")
        # Simulate PUBREC success
        feed_engine(engine, PubRecPacket(mid=mid).encode())
        effects = engine.take_effects()
        sends = [e for e in effects if e.kind==EffectKind.SEND]
        print(f" after PUBREC state {store.get_out(mid).state if store.get_out(mid) else None} topic now '{store.get_out(mid).topic if store.get_out(mid) else None}' payload len {len(store.get_out(mid).payload) if store.get_out(mid) and store.get_out(mid).payload else 0} sends {len(sends)}")
        # Check compaction: topic should be "" and payload b"" for WAIT_PUBCOMP
        rec = store.get_out(mid)
        if rec and rec.topic == "" and rec.payload == b"":
            print("  OK compacted")
        else:
            print(f"  *** not compacted: topic={rec.topic if rec else None}")
        # Now crash after PUBREC but before PUBCOMP
        if label=="Memory":
            engine2 = ProtocolEngine(EngineConfig(), store)
            # hydration should see WAIT_PUBCOMP
            print(f"  reopen state {store.get_out(mid).state if store.get_out(mid) else None} pending {engine2.pending_outbound_messages} inflight {engine2.flow.inflight}")
            # Before reconnect, flow inflight after hydrate? Actually hydrate sets flow for WAIT_PUBCOMP
            # Now replay_session should retransmit PUBREL
            engine2.state = ConnectionState.CONNECTED
            engine2.negotiated = NegotiatedSettings(receive_maximum=65535)
            engine2.outbound.replay_session()
            eff = engine2.take_effects()
            sends2 = [e for e in eff if e.kind==EffectKind.SEND]
            print(f"   after replay sends {len(sends2)} (should be 1 PUBREL)")
            # Check that replay payload not needed (still compacted)
            rec2 = store.get_out(mid)
            print(f"   still compacted topic '{rec2.topic}'")
            # Now simulate PUBCOMP
            feed_engine(engine2, PubRecPacket(mid=mid).encode()) # duplicate PUBREC should still send PUBREL?
            # Actually after compact, duplicate PUBREC should still send PUBREL (idempotent)
            # Test duplicate PUBREC
            feed_engine(engine2, PubRecPacket(mid=mid).encode())
            engine2.take_effects()
            # Now PUBCOMP
            from mqttium.packets import PubCompPacket
            feed_engine(engine2, PubCompPacket(mid=mid).encode())
            engine2.take_effects()
            print(f"   after PUBCOMP store empty? {list(store.out_items())} pending {engine2.pending_outbound_bytes}")
            if list(store.out_items()):
                print("   *** leak")
            else:
                print("   OK completed")
        else:
            store.close()
            store2 = SqliteInflightStore(path)
            engine2 = ProtocolEngine(EngineConfig(), store2)
            print(f"  reopen state {store2.get_out(mid).state if store2.get_out(mid) else None} pending {engine2.pending_outbound_messages}")
            engine2.state = ConnectionState.CONNECTED
            engine2.negotiated = NegotiatedSettings(receive_maximum=65535)
            engine2.outbound.replay_session()
            eff = engine2.take_effects()
            sends2 = [e for e in eff if e.kind==EffectKind.SEND]
            print(f"   after replay sends {len(sends2)}")
            from mqttium.packets import PubCompPacket
            feed_engine(engine2, PubCompPacket(mid=mid).encode())
            engine2.take_effects()
            print(f"   after PUBCOMP empty? {list(store2.out_items())} pending {engine2.pending_outbound_bytes}")
            store2.close()
            Path(path).unlink(missing_ok=True)

def experiment_tiny_packet_limit_manual_drain():
    print_header("ADDITIONAL 3: manual ack drain with tiny peer packet limit")
    # Broker advertises maximum_packet_size 3 (<4) so PUBACK/PUBREL/PUBCOMP impossible
    # Ack should fail before deletion for first, but what about prefix order?
    for label in ["Memory","SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(manual_ack=True), store)
        engine.state = ConnectionState.CONNECTED
        # Make negotiated tiny
        engine.negotiated = NegotiatedSettings(maximum_packet_size=3)
        engine.inbound.configure_peer_packet_limit(3)
        # Create two inbound QoS1
        for mid in [1,2]:
            feed_engine(engine, PublishPacket(topic="t", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, mid=mid).encode())
        engine.take_effects()
        print(f" after ingress stored {sorted(m.mid for m in store.in_items())} order {list(engine.inbound._manual_qos1_order)}")
        # Now ack both in order, but second drain's PUBACK will be impossible due to limit
        # First ack 2 (out of order) -> pending set {2}
        # Then ack 1 -> should drain 1 then 2; 1's ack will try to send PUBACK size 4 > limit 3 => should raise MandatoryResponseTooLargeError before deletion? Let's see
        # For manual drain, ack() checks size for the triggering mid only, not for prefix earlier mids?
        # We earlier noted drain doesn't re-check size for subsequent mids after first.
        try:
            engine.ack(2)
            print(f" after ack2 pending_set {engine.inbound._pending_manual_qos1_acks}")
            engine.ack(1)
            print(f" after ack1 effects? {engine.take_effects()}")
        except Exception as e:
            print(f" ack raised {type(e).__name__}: {e}")
            # Check store after failure
            print(f" stored after failure {sorted(m.mid for m in store.in_items())} pending_bytes {engine.inbound._pending_bytes} inflight {engine.inbound._inflight}")
            print(f" take_effects after failure {engine.take_effects()}")
        # After failure, what is state? If drain partially completed 1 before failing on 2's size check missing, then 1 would be deleted but 2 not, causing partial drain.
        # Let's see actual behavior for tiny limit: ack() does size check at top for the acked mid (1) but not for 2's later drain iteration inside _drain.
        # So 1's check passes? No, for tiny limit 3, size 4 >3 so first ack's size check should fail before any deletion.
        # Actually ack(2) check would fail? For ack(2), it checks size for mid 2, which fails, so ack(2) should raise before adding to pending set? Let's see code: ack for WAIT_PUBACK does _engine._check_outbound_size before adding to pending.
        # So ack(2) itself should raise MandatoryResponseTooLargeError immediately, not be added.
        # Then ack(1) similarly would fail.
        # No drain happens.
        print(f" final stored {sorted(m.mid for m in store.in_items())} inflight {engine.inbound._inflight}")
        if label=="SQLite":
            store.close()
            Path(path).unlink(missing_ok=True)

def experiment_outbound_compaction_vs_replay_order():
    print_header("ADDITIONAL 4: outbound compact replay vs queued order")
    for label in ["Memory","SQLite"]:
        print(f"\n-- {label} --")
        if label=="Memory":
            store = MemoryInflightStore()
        else:
            path = Path(tempfile.mktemp(suffix=".db"))
            store = SqliteInflightStore(path)
        engine = ProtocolEngine(EngineConfig(), store)
        engine.state = ConnectionState.CONNECTED
        # Create QUEUED and WAIT_PUBCOMP mixed
        # Use flow limit to force QUEUED
        engine.flow._limit = 1
        h1 = engine.queue_publish("a/b", b"one", qos=QoS.AT_LEAST_ONCE) # WAIT_PUBACK inflight 1
        engine.take_effects()
        h2 = engine.queue_publish("a/b", b"two", qos=QoS.AT_LEAST_ONCE) # QUEUED
        engine.take_effects()
        print(f" before PUBREC states {[(m.mid, m.state) for m in store.out_items()]} queued {[m.mid for m in engine._queued]}")
        # Now get h1 to WAIT_PUBCOMP via PUBREC
        feed_engine(engine, PubRecPacket(mid=h1.mid).encode())
        engine.take_effects()
        print(f" after PUBREC states {[(m.mid, m.state) for m in store.out_items()]} queued {[m.mid for m in engine._queued]}")
        # Now we have one WAIT_PUBCOMP (compacted) and one QUEUED
        # Simulate reconnect with Session Present true
        # Outbound replay should handle both: WAIT_PUBCOMP -> retransmit PUBREL, QUEUED -> stay queued until flow available?
        # But replay logic: for each msg in order, if state QUEUED -> append to queued, if WAIT and flow available -> retransmit else queue
        # With limit 1, after reset flow 0, first message WAIT_PUBCOMP will try_acquire succeed -> retransmit PUBREL, flow inflight 1
        # second message QUEUED will be appended to queued (not checking flow? Actually _replay_message for QUEUED appends regardless)
        # So queued should contain second
        # Let's do replay via new engine
        if label=="Memory":
            engine2 = ProtocolEngine(EngineConfig(), store)
            engine2.state = ConnectionState.CONNECTED
            engine2.negotiated = NegotiatedSettings(receive_maximum=1)
            engine2.flow._limit = 1 # ensure limit
            engine2.outbound.replay_session()
            eff = engine2.take_effects()
            sends = [e for e in eff if e.kind==EffectKind.SEND]
            print(f" reopen replay sends {len(sends)} queued {[m.mid for m in engine2._queued]} flow {engine2.flow.inflight}")
            # Check compacted record still compacted
            rec = store.get_out(h1.mid)
            print(f"  compacted still? topic='{rec.topic}' payload len {len(rec.payload) if rec else 0}")
        else:
            store.close()
            store2 = SqliteInflightStore(path)
            engine2 = ProtocolEngine(EngineConfig(), store2)
            engine2.state = ConnectionState.CONNECTED
            engine2.negotiated = NegotiatedSettings(receive_maximum=1)
            engine2.flow._limit = 1
            engine2.outbound.replay_session()
            eff = engine2.take_effects()
            sends = [e for e in eff if e.kind==EffectKind.SEND]
            print(f" reopen replay sends {len(sends)} queued {[m.mid for m in engine2._queued]}")
            store2.close()
            Path(path).unlink(missing_ok=True)

def experiment_store_unexpected_close():
    print_header("ADDITIONAL 5: store unexpected close handling")
    path = Path(tempfile.mktemp(suffix=".db"))
    store = SqliteInflightStore(path)
    store.put_out(OutboundMessage(mid=1, topic="a", payload=b"x", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=2))
    print(f" before close items {list(store.out_items())}")
    store.close()
    print(f" after close closed={store._closed}")
    try:
        store.put_out(OutboundMessage(mid=2, topic="b", payload=b"y", qos=QoS.AT_LEAST_ONCE, retain=False, state=OutboundQoSState.WAIT_PUBACK, logical_size=2))
        print(" put after close unexpectedly succeeded")
    except Exception as e:
        print(f" put after close raised {type(e).__name__}: {e} (expected)")
    try:
        store.get_out(1)
        print(" get after close succeeded (unexpected)")
    except Exception as e:
        print(f" get after close raised {type(e).__name__}: {e}")
    # Reopen should see previous row
    store2 = SqliteInflightStore(path)
    print(f" reopen items {sorted(m.mid for m in store2.out_items())}")
    store2.close()
    Path(path).unlink(missing_ok=True)

def experiment_transport_loss_around_durable():
    print_header("ADDITIONAL 6: transport loss around durable transition (PUBREC)")
    path = Path(tempfile.mktemp(suffix=".db"))
    store = SqliteInflightStore(path)
    engine = ProtocolEngine(EngineConfig(), store)
    engine.state = ConnectionState.CONNECTED
    h = engine.queue_publish("a/b", b"payload", qos=QoS.EXACTLY_ONCE)
    mid = h.mid
    engine.take_effects()
    print(f" after publish state {store.get_out(mid).state}")
    # Simulate PUBREC arrival and transport loss immediately after (before taking effects)
    feed_engine(engine, PubRecPacket(mid=mid).encode())
    # Don't take effects yet, simulate transport closure before draining effects (like crash after durable compaction but before wire)
    # Check store state after PUBREC but before effect taken
    print(f" after PUBREC (before take) state {store.get_out(mid).state}")
    # Now simulate transport closed (engine notified)
    engine.notify_transport_closed()
    engine.take_effects()  # this will discard SEND that was not yet taken? Actually SEND was enqueued but transport_closed may discard?
    print(f" after transport closed state {store.get_out(mid).state} effects pending? {engine._effects}")
    # Now reconnect with session present
    engine2 = ProtocolEngine(EngineConfig(), SqliteInflightStore(path)) # reopen store
    # Actually store already closed? No we used same store object but we reopened second time - need to close first store?
    # For simplicity close and reopen
    store.close()
    store2 = SqliteInflightStore(path)
    engine3 = ProtocolEngine(EngineConfig(), store2)
    engine3.state = ConnectionState.CONNECTED
    engine3.negotiated = NegotiatedSettings(receive_maximum=65535)
    engine3.outbound.replay_session()
    eff = engine3.take_effects()
    sends = [e for e in eff if e.kind==EffectKind.SEND]
    print(f" after reconnect replay sends {len(sends)} (should be PUBREL)")
    if sends:
        from mqttium.codec.buffer import RawPacket
        print(f"  send kind {[e.kind for e in eff if e.kind==EffectKind.SEND]}")
    store2.close()
    Path(path).unlink(missing_ok=True)

if __name__ == "__main__":
    experiment_inbound_replay_interrupted()
    experiment_compact_transition_durability()
    experiment_tiny_packet_limit_manual_drain()
    experiment_outbound_compaction_vs_replay_order()
    experiment_store_unexpected_close()
    experiment_transport_loss_around_durable()
