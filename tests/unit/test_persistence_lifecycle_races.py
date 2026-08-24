"""Deterministic persistence/lifecycle races the sequential stateful fuzzer cannot see.

The existing stateful fuzzer compares MemoryInflightStore and SqliteInflightStore
under sequential randomized operations and checks engine invariants after every
step. This module attacks the *boundaries* between those operations:

* inbound replay yields to the runtime between batches (CONTINUE_INBOUND_REPLAY);
* store paging snapshots identifiers then looks them up;
* CONNACK session_present true/false against already-hydrated ownership;
* injected store failures on complete/transition;
* transport close while a replay cursor is live.

It does not spawn threads. mqttium's engine is loop-confined; the only
legitimate interleaving is an effect-pump yield plus a later engine re-entry.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import MQTTError, SessionDiscardedError
from mqttium.packets import PubAckPacket, PubRelPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import REPLAY_BATCH_MESSAGES
from mqttium.types import InboundMessage, OutboundMessage
from tests.support import feed_engine

StoreFactory = Callable[[Path], MemoryInflightStore | SqliteInflightStore]


def _memory(_path: Path) -> MemoryInflightStore:
    return MemoryInflightStore()


def _sqlite(path: Path) -> SqliteInflightStore:
    return SqliteInflightStore(path / "lifecycle.db")


STORES = pytest.mark.parametrize("store_factory", [_memory, _sqlite], ids=["memory", "sqlite"])


def inbound(
    mid: int,
    *,
    payload: bytes = b"body",
    state: InboundQoSState = InboundQoSState.WAIT_PUBREL,
    delivered: bool = False,
) -> InboundMessage:
    topic = f"in/{mid}"
    return InboundMessage(
        mid=mid,
        topic=topic,
        payload=payload,
        qos=QoS.EXACTLY_ONCE if state is not InboundQoSState.WAIT_PUBACK else QoS.AT_LEAST_ONCE,
        retain=False,
        state=state,
        delivered=delivered,
        logical_size=len(topic) + len(payload),
    )


def outbound(
    mid: int,
    *,
    qos: QoS = QoS.AT_LEAST_ONCE,
    state: OutboundQoSState = OutboundQoSState.WAIT_PUBACK,
    payload: bytes = b"payload",
) -> OutboundMessage:
    topic = f"out/{mid}"
    return OutboundMessage(
        mid=mid,
        topic=topic,
        payload=payload,
        qos=qos,
        retain=False,
        state=state,
        logical_size=len(topic) + len(payload),
    )


def fill_in(store: Any, count: int, **kwargs: Any) -> None:
    with store.batch():
        for mid in range(1, count + 1):
            store.put_in(inbound(mid, **kwargs))


def connack(
    *, session_present: bool, protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311
) -> bytes:
    body = bytes((0x01 if session_present else 0x00, 0x00))
    if protocol is MQTTProtocolVersion.MQTTv5:
        body += b"\x00"
    return encode_frame(PacketType.CONNACK, 0, body)


def engine_for(store: Any, **config: Any) -> ProtocolEngine:
    config.setdefault("client_id", "lifecycle")
    config.setdefault("clean_start", False)
    return ProtocolEngine(EngineConfig(**config), store=store)


def resume(engine: ProtocolEngine, *, session_present: bool) -> list[Any]:
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, bytes((1 if session_present else 0, 0))))
    return engine.take_effects()


def message_mids(effects: list[Any]) -> list[int]:
    return [effect.data.mid for effect in effects if effect.kind is EffectKind.MESSAGE]


def failed_mids(effects: list[Any]) -> dict[int, BaseException]:
    failed: dict[int, BaseException] = {}
    for effect in effects:
        if effect.kind is EffectKind.PUBLISH_FAILED:
            failed[effect.data.mid] = effect.data.reason
    return failed


def ownership(engine: ProtocolEngine) -> dict[str, Any]:
    """Snapshot the four ownership ledgers that must stay in lockstep."""
    out_records = list(engine.store.out_items())
    in_records = list(engine.store.in_items())
    return {
        "out_mids": sorted(record.mid for record in out_records),
        "out_states": {record.mid: record.state for record in out_records},
        "in_mids": sorted(record.mid for record in in_records),
        "in_states": {record.mid: record.state for record in in_records},
        "packet_ids": sorted(engine.packet_ids._used),
        "pending_messages": engine.pending_outbound_messages,
        "pending_bytes": engine.pending_outbound_bytes,
        "flow_inflight": engine.flow.inflight,
        "inbound_inflight": engine._inbound_inflight,
        "inbound_bytes": engine.inbound._pending_bytes,
        "replay_pending": engine.inbound.replay_pending,
    }


def assert_outbound_ownership(engine: ProtocolEngine) -> None:
    records = list(engine.store.out_items())
    mids = sorted(record.mid for record in records)
    assert sorted(engine.packet_ids._used) == mids
    assert engine.pending_outbound_messages == len(records)
    expected_bytes = sum(engine.outbound.stored_logical_size(record) for record in records)
    assert engine.pending_outbound_bytes == expected_bytes
    for record in records:
        assert engine.packet_ids.in_use(record.mid)


def assert_inbound_bytes(engine: ProtocolEngine) -> None:
    records = list(engine.store.in_items())
    expected = sum(engine.inbound.stored_logical_size(record) for record in records)
    assert engine.inbound._pending_bytes == expected


# --- store paging: snapshot then lookup --------------------------------------


@STORES
def test_store_replay_pages_skip_rows_deleted_after_the_identifier_snapshot(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """Both stores snapshot identifiers, then re-read. A delete in between is a hole.

    This is the store contract the engine relies on when an ACK completes a
    record after replay has already listed it but before the page is hydrated.
    """
    store = store_factory(tmp_path)
    with store.batch():
        for mid in range(1, 6):
            store.put_in(inbound(mid))

    pages = store.in_replay_pages(max_messages=2, max_bytes=1 << 20)
    first = next(pages)
    assert [message.mid for message in first] == [1, 2]
    assert store.pop_in(4) is not None
    rest = [message.mid for page in pages for message in page]
    assert 4 not in rest
    assert sorted([message.mid for message in first] + rest) == [1, 2, 3, 5]
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_store_outbound_summary_pages_skip_rows_deleted_after_the_identifier_snapshot(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    store = store_factory(tmp_path)
    with store.batch():
        for mid in range(1, 6):
            store.put_out(outbound(mid))

    pages = store.out_summary_pages(page_size=2)
    first = next(pages)
    assert [item.mid for item in first] == [1, 2]
    assert store.delete_out(3) is True
    rest = [item.mid for page in pages for item in page]
    assert 3 not in rest
    assert sorted([item.mid for item in first] + rest) == [1, 2, 4, 5]
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_complete_out_is_atomic_when_the_expected_state_does_not_match(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    store = store_factory(tmp_path)
    store.put_out(outbound(1, state=OutboundQoSState.WAIT_PUBACK))
    assert store.complete_out(1, OutboundQoSState.WAIT_PUBCOMP) is None
    record = store.get_out(1)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBACK
    if isinstance(store, SqliteInflightStore):
        store.close()


# --- hydration vs CONNACK session_present ------------------------------------


@STORES
def test_session_present_false_fails_inflight_keeps_queued_and_releases_ids(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    store = store_factory(tmp_path)
    with store.batch():
        store.put_out(outbound(1, state=OutboundQoSState.WAIT_PUBACK))
        store.put_out(
            outbound(
                2,
                qos=QoS.EXACTLY_ONCE,
                state=OutboundQoSState.WAIT_PUBCOMP,
            )
        )
        store.put_out(outbound(3, state=OutboundQoSState.QUEUED))
        store.put_in(inbound(9))

    engine = engine_for(store)
    assert_outbound_ownership(engine)
    assert engine.packet_ids.in_use(1)
    assert engine.packet_ids.in_use(3)

    effects = resume(engine, session_present=False)
    failed = failed_mids(effects)
    assert set(failed) == {1, 2}
    assert all(isinstance(reason, SessionDiscardedError) for reason in failed.values())
    assert store.get_out(1) is None
    assert store.get_out(2) is None
    assert store.get_out(3) is not None
    assert store.get_in(9) is None
    assert_outbound_ownership(engine)
    assert_inbound_bytes(engine)
    assert engine.inbound._pending_bytes == 0
    assert engine._inbound_inflight == 0
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_session_present_true_replays_without_duplicating_ownership(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    store = store_factory(tmp_path)
    with store.batch():
        store.put_out(outbound(1, state=OutboundQoSState.WAIT_PUBACK, payload=b"one"))
        store.put_out(outbound(2, state=OutboundQoSState.QUEUED, payload=b"two"))

    engine = engine_for(store)
    before = ownership(engine)
    effects = resume(engine, session_present=True)
    sends = [effect for effect in effects if effect.kind is EffectKind.SEND]
    assert len(sends) >= 1
    after = ownership(engine)
    assert after["out_mids"] == before["out_mids"] == [1, 2]
    assert after["packet_ids"] == before["packet_ids"] == [1, 2]
    assert after["pending_messages"] == 2
    assert_outbound_ownership(engine)
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_reconnect_session_present_true_is_semantically_idempotent(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """A second resume retransmits; it must not resurrect completed work or leak ids."""
    store = store_factory(tmp_path)
    store.put_out(outbound(7, state=OutboundQoSState.WAIT_PUBACK))
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    assert_outbound_ownership(engine)

    engine.notify_transport_closed()
    engine.take_effects()
    assert store.get_out(7) is not None
    assert engine.packet_ids.in_use(7)

    effects = resume(engine, session_present=True)
    assert 7 not in failed_mids(effects)
    assert store.get_out(7) is not None
    assert_outbound_ownership(engine)
    assert ownership(engine)["packet_ids"] == [7]
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_connack_is_ignored_after_transport_close_and_leaves_hydrated_ownership(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """Cancellation analogue: CONNECT went out, the socket died, CONNACK is late."""
    store = store_factory(tmp_path)
    store.put_out(outbound(4, state=OutboundQoSState.WAIT_PUBACK))
    engine = engine_for(store)
    engine.begin_connect()
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()
    before = ownership(engine)

    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))
    engine.take_effects()
    assert ownership(engine)["out_mids"] == before["out_mids"] == [4]
    assert engine.packet_ids.in_use(4)
    assert_outbound_ownership(engine)
    if isinstance(store, SqliteInflightStore):
        store.close()


# --- inbound replay vs ACK/PUBREL between batches ----------------------------


@STORES
def test_pubrel_between_inbound_replay_batches_does_not_resurrect_the_row(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """Legitimate runtime interleaving: PUBREL arrives while CONTINUE is pending.

    The first replay page is larger than one effect batch, so mid 70 is already
    hydrated in the cursor when PUBREL completes it. The durable row must stay
    gone; a MESSAGE for a completed exchange is an integration hazard.
    """
    store = store_factory(tmp_path)
    fill_in(store, 100)
    engine = engine_for(store)
    effects = resume(engine, session_present=True)
    first = message_mids(effects)
    assert first == list(range(1, REPLAY_BATCH_MESSAGES + 1))
    assert engine.inbound.replay_pending is True
    assert 70 not in first

    feed_engine(engine, PubRelPacket(mid=70).encode())
    engine.take_effects()
    assert store.get_in(70) is None
    assert_inbound_bytes(engine)

    engine.continue_inbound_replay()
    later = message_mids(engine.take_effects())
    resurrected = store.get_in(70) is not None
    delivered_after_complete = 70 in later
    assert not resurrected, "a completed inbound row must not reappear in the store"
    # Cursor-held copies can still be emitted; that is a runtime-integration
    # finding, not a store bug. Pin the store half here and the emission half
    # in the dedicated test below.
    del delivered_after_complete
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_pubrel_between_batches_does_not_restore_the_store_row_or_desync_bytes(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """Store and byte ledger stay consistent even if the cursor later emits mid 70."""
    store = store_factory(tmp_path)
    fill_in(store, 100)
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    feed_engine(engine, PubRelPacket(mid=70).encode())
    engine.take_effects()
    assert store.get_in(70) is None
    assert engine._inbound_inflight == 99
    assert_inbound_bytes(engine)

    while engine.inbound.replay_pending:
        engine.continue_inbound_replay()
        engine.take_effects()

    assert store.get_in(70) is None
    assert engine._inbound_inflight == 99
    assert_inbound_bytes(engine)
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_inbound_replay_cursor_emits_a_pubrel_completed_record_from_its_stale_copy(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """FINDING (runtime, not store): `_should_redeliver` trusts the cursor copy.

    `in_replay_pages` hydrates a page larger than one effect batch. PUBREL can
    delete mid 70 while that object is still in the cursor. Both stores omit
    the deleted row on a later page fetch; the cursor does not re-read it.
    """
    store = store_factory(tmp_path)
    fill_in(store, 100)
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    feed_engine(engine, PubRelPacket(mid=70).encode())
    engine.take_effects()
    assert store.get_in(70) is None

    delivered: list[int] = []
    while engine.inbound.replay_pending:
        engine.continue_inbound_replay()
        delivered.extend(message_mids(engine.take_effects()))

    assert 70 in delivered
    assert store.get_in(70) is None
    assert_inbound_bytes(engine)
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_out_of_order_manual_ack_does_not_complete_a_later_qos1_record(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """MQTT-4.6.0-2: a later QoS 1 ack stays pending until the arrival prefix is ready."""
    store = store_factory(tmp_path)
    fill_in(store, 80, state=InboundQoSState.WAIT_PUBACK)
    engine = engine_for(store, manual_ack=True)
    resume(engine, session_present=True)
    engine.take_effects()
    inflight_after_resume = engine._inbound_inflight
    assert inflight_after_resume == 80

    engine.ack(70)
    engine.take_effects()
    assert store.get_in(70) is not None
    assert engine._inbound_inflight == inflight_after_resume
    assert_inbound_bytes(engine)
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_manual_ack_of_the_qos1_prefix_between_batches_releases_capacity_once(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """Acking 1..70 (including cursor-held 65..70) must not leak Receive Maximum.

    The cursor may still MESSAGE 65..70 afterwards; that is the same stale-copy
    finding as PUBREL. Capacity and the store must follow the completed rows.
    """
    store = store_factory(tmp_path)
    fill_in(store, 80, state=InboundQoSState.WAIT_PUBACK)
    engine = engine_for(store, manual_ack=True)
    resume(engine, session_present=True)
    engine.take_effects()

    for mid in range(1, 71):
        engine.ack(mid)
        engine.take_effects()

    assert store.get_in(70) is None
    assert engine._inbound_inflight == 10
    assert_inbound_bytes(engine)

    delivered: list[int] = []
    while engine.inbound.replay_pending:
        engine.continue_inbound_replay()
        delivered.extend(message_mids(engine.take_effects()))

    assert store.get_in(70) is None
    assert engine._inbound_inflight == 10
    assert_inbound_bytes(engine)
    assert 70 in delivered
    if isinstance(store, SqliteInflightStore):
        store.close()


@STORES
def test_transport_close_then_connect_abandons_the_replay_cursor(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    store = store_factory(tmp_path)
    fill_in(store, 200)
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    assert engine.inbound.replay_pending is True

    engine.notify_transport_closed()
    engine.take_effects()
    # The cursor is connection-scoped. Close does not by itself drop it; the
    # next CONNECT does. A runtime that pumps CONTINUE after close without an
    # epoch check would still drain the old cursor.
    engine.begin_connect()
    engine.take_effects()
    assert engine.inbound.replay_pending is False
    if isinstance(store, SqliteInflightStore):
        store.close()


def test_continue_after_transport_close_without_new_connect_still_drains_the_cursor() -> None:
    """Engine-level hazard: notify_transport_closed does not clear `_replay`."""
    store = MemoryInflightStore()
    fill_in(store, 200)
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()
    assert engine.inbound.replay_pending is True
    engine.continue_inbound_replay()
    leaked = message_mids(engine.take_effects())
    assert leaked, "without an epoch, CONTINUE after close still emits the old cursor"
    assert engine.inbound.replay_pending is True


# --- injected persistence failures -------------------------------------------


class _Boom(RuntimeError):
    """Injected store failure."""


class _FailingCompleteStore(MemoryInflightStore):
    def complete_out(self, mid: int, expected_state: OutboundQoSState) -> Any:
        raise _Boom("complete_out failed")


class _FailingTransitionStore(MemoryInflightStore):
    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> Any:
        raise _Boom("transition_out failed")


class _VanishOnGetOut(MemoryInflightStore):
    """Delete the row when replay materialises it: snapshot-to-lookup hole."""

    def get_out(self, mid: int) -> OutboundMessage | None:
        self.delete_out(mid)
        return super().get_out(mid)


def test_puback_store_failure_does_not_release_ownership() -> None:
    store = _FailingCompleteStore()
    store.put_out(outbound(1))
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    before = ownership(engine)

    feed_engine(engine, PubAckPacket(mid=1).encode())
    effects = engine.take_effects()
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert store.get_out(1) is not None
    assert engine.packet_ids.in_use(1)
    assert ownership(engine)["pending_messages"] == before["pending_messages"]
    assert ownership(engine)["pending_bytes"] == before["pending_bytes"]
    assert_outbound_ownership(engine)


def test_pubrec_store_failure_does_not_advance_qos2_or_leak_a_packet_id() -> None:
    store = _FailingTransitionStore()
    store.put_out(outbound(2, qos=QoS.EXACTLY_ONCE, state=OutboundQoSState.WAIT_PUBREC))
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()

    feed_engine(engine, encode_frame(PacketType.PUBREC, 0, b"\x00\x02"))
    effects = engine.take_effects()
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    record = store.get_out(2)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBREC
    assert engine.packet_ids.in_use(2)
    assert_outbound_ownership(engine)


def test_replay_materialize_of_a_vanished_row_releases_id_and_budget_once() -> None:
    store = _VanishOnGetOut()
    store.put_out(outbound(5, state=OutboundQoSState.WAIT_PUBACK))
    engine = engine_for(store)
    # Hydration saw the row. Replay then materialises and the row is gone.
    effects = resume(engine, session_present=True)
    assert store.get_out(5) is None
    assert 5 in failed_mids(effects)
    assert not engine.packet_ids.in_use(5)
    assert_outbound_ownership(engine)
    assert engine.pending_outbound_messages == 0
    assert engine.flow.inflight == 0


def test_complete_out_after_mutation_raise_is_a_store_contract_violation() -> None:
    """If complete_out deletes then raises, the engine cannot compensate.

    Built-in stores do not do this. The test pins the engine's assumption:
    a raising complete_out is treated as failure and ownership is not unwound.
    """

    class DeleteThenRaise(MemoryInflightStore):
        def complete_out(self, mid: int, expected_state: OutboundQoSState) -> Any:
            super().complete_out(mid, expected_state)
            raise _Boom("deleted then failed")

    store = DeleteThenRaise()
    store.put_out(outbound(1))
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    feed_engine(engine, PubAckPacket(mid=1).encode())
    engine.take_effects()
    # Store row is gone (the wrapper deleted it) but the engine still thinks
    # it owns the mid/budget — a store-contract violation, not an engine bug.
    assert store.get_out(1) is None
    assert engine.packet_ids.in_use(1)
    assert engine.pending_outbound_messages == 1


# --- puback vs replay observation --------------------------------------------


def test_puback_after_hydration_before_replay_settles_exactly_once() -> None:
    """ACK before CONNACK cannot run: handle_raw ignores packets while CONNECTING.

    The closest legitimate schedule is: hydrate (engine ctor), CONNECT, CONNACK
    session_present=true (replay), then PUBACK. Completes once.
    """
    store = MemoryInflightStore()
    store.put_out(outbound(1))
    engine = engine_for(store)
    assert engine.packet_ids.in_use(1)
    resume(engine, session_present=True)
    engine.take_effects()
    feed_engine(engine, PubAckPacket(mid=1).encode())
    effects = engine.take_effects()
    assert any(
        effect.kind is EffectKind.PUBLISH_COMPLETE and effect.data == 1 for effect in effects
    )
    assert store.get_out(1) is None
    assert not engine.packet_ids.in_use(1)
    assert_outbound_ownership(engine)


def test_duplicate_puback_does_not_double_release_packet_id_or_budget() -> None:
    store = MemoryInflightStore()
    store.put_out(outbound(1))
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    feed_engine(engine, PubAckPacket(mid=1).encode())
    engine.take_effects()
    feed_engine(engine, PubAckPacket(mid=1).encode())
    extras = engine.take_effects()
    assert not any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in extras)
    assert not engine.packet_ids.in_use(1)
    assert engine.pending_outbound_messages == 0
    assert engine.flow.inflight == 0
    # PacketIdPool.release is idempotent: a second release of a free id is a no-op.
    engine.packet_ids.release(1)
    assert len(engine.packet_ids) == 0


# --- memory vs sqlite under one lifecycle schedule ---------------------------


def _lifecycle_schedule(store: Any) -> dict[str, Any]:
    with store.batch():
        store.put_out(outbound(1, state=OutboundQoSState.WAIT_PUBACK, payload=b"a"))
        store.put_out(outbound(2, state=OutboundQoSState.QUEUED, payload=b"bb"))
        store.put_in(inbound(11, payload=b"in"))
    engine = engine_for(store)
    snap = {"after_hydrate": ownership(engine)}
    resume(engine, session_present=True)
    engine.take_effects()
    snap["after_resume_true"] = ownership(engine)
    feed_engine(engine, PubAckPacket(mid=1).encode())
    engine.take_effects()
    snap["after_puback"] = ownership(engine)
    feed_engine(engine, PubRelPacket(mid=11).encode())
    engine.take_effects()
    snap["after_pubrel"] = ownership(engine)
    engine.notify_transport_closed()
    engine.take_effects()
    snap["after_close"] = ownership(engine)
    resume(engine, session_present=False)
    engine.take_effects()
    snap["after_resume_false"] = ownership(engine)
    return snap


def test_memory_and_sqlite_agree_on_a_lifecycle_schedule(tmp_path: Path) -> None:
    memory = MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / "schedule.db")
    left = _lifecycle_schedule(memory)
    right = _lifecycle_schedule(sqlite)
    sqlite.close()
    assert left == right


def test_queued_byte_accounting_survives_session_present_false() -> None:
    store = MemoryInflightStore()
    body = b"x" * 50
    store.put_out(outbound(1, state=OutboundQoSState.QUEUED, payload=body))
    store.put_out(outbound(2, state=OutboundQoSState.WAIT_PUBACK, payload=b"lost"))
    engine = engine_for(store)
    queued_size = engine.outbound.stored_logical_size(store.get_out(1))
    resume(engine, session_present=False)
    engine.take_effects()
    assert engine.pending_outbound_messages == 1
    assert engine.pending_outbound_bytes == queued_size
    assert_outbound_ownership(engine)


def test_unacked_inbound_bytes_match_store_after_partial_replay_and_close() -> None:
    store = MemoryInflightStore()
    fill_in(store, 90)
    engine = engine_for(store)
    resume(engine, session_present=True)
    engine.take_effects()
    assert_inbound_bytes(engine)
    engine.notify_transport_closed()
    engine.begin_connect()
    engine.take_effects()
    # CONNECT resets inbound inflight/cursor; durable rows and byte ledger remain
    # until CONNACK says whether the session survived.
    assert_inbound_bytes(engine)
    assert list(engine.store.in_items())
    assert engine.inbound.replay_pending is False


@STORES
def test_reconnect_during_partial_inbound_replay_starts_a_fresh_store_backed_cursor(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    """A new CONNECT abandons the old cursor; session_present=true replays from the store."""
    store = store_factory(tmp_path)
    fill_in(store, 200)
    engine = engine_for(store)
    first = message_mids(resume(engine, session_present=True))
    assert first == list(range(1, REPLAY_BATCH_MESSAGES + 1))
    assert engine.inbound.replay_pending is True

    engine.notify_transport_closed()
    engine.take_effects()
    second = message_mids(resume(engine, session_present=True))
    assert second == list(range(1, REPLAY_BATCH_MESSAGES + 1))
    assert store.get_in(1) is not None
    assert_inbound_bytes(engine)
    if isinstance(store, SqliteInflightStore):
        store.close()


def test_mark_delivered_of_a_cursor_held_mid_diverges_memory_vs_sqlite(tmp_path: Path) -> None:
    """Memory pages alias live records; SQLite pages are snapshots.

    Sequential store APIs agree. The same engine schedule — mark_delivered on a
    mid the cursor already hydrated but has not yet emitted — does not.
    """

    def emitted_70(store: MemoryInflightStore | SqliteInflightStore) -> bool:
        fill_in(store, 100)
        engine = engine_for(store)
        resume(engine, session_present=True)
        engine.take_effects()
        engine.mark_inbound_delivered(70)
        delivered: list[int] = []
        while engine.inbound.replay_pending:
            engine.continue_inbound_replay()
            delivered.extend(message_mids(engine.take_effects()))
        return 70 in delivered

    memory = MemoryInflightStore()
    sqlite = SqliteInflightStore(tmp_path / "alias.db")
    memory_emits = emitted_70(memory)
    sqlite_emits = emitted_70(sqlite)
    sqlite.close()
    assert memory_emits is False
    assert sqlite_emits is True


async def test_fail_pending_settles_receipts_without_purging_durable_outbound() -> None:
    """Terminal receipt failure is runtime-owned; the store and packet-id pool stay put."""
    store = MemoryInflightStore()
    store.put_out(outbound(1, state=OutboundQoSState.WAIT_PUBACK))
    client = AsyncClient(client_id="term", clean_start=False, store=store)
    assert client._engine.packet_ids.in_use(1)
    receipt = PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE)
    client._register_publish_receipt(1, receipt)

    client._fail_pending(MQTTError("terminal disconnect"))
    assert receipt.is_done()
    with pytest.raises(MQTTError, match="terminal disconnect"):
        await receipt.wait()
    assert store.get_out(1) is not None
    assert client._engine.packet_ids.in_use(1)
    assert_outbound_ownership(client._engine)

    sends = [
        effect
        for effect in resume(client._engine, session_present=True)
        if effect.kind is EffectKind.SEND
    ]
    assert sends
    assert store.get_out(1) is not None
    assert client._pop_publish_receipt(1) is None


async def test_stale_continue_inbound_replay_effect_does_not_reenter_the_engine() -> None:
    """AsyncClient epoch drops CONTINUE after teardown; the engine cursor itself is still live."""
    store = MemoryInflightStore()
    fill_in(store, 200)
    client = AsyncClient(client_id="epoch", clean_start=False, store=store)
    resume(client._engine, session_present=True)
    client._engine.take_effects()
    assert client._engine.inbound.replay_pending is True

    called: list[bool] = []
    original = client._engine.continue_inbound_replay

    def wrapped() -> None:
        called.append(True)
        original()

    client._engine.continue_inbound_replay = wrapped  # type: ignore[method-assign]
    client._connection_epoch = 2
    await client._apply_effect(
        EngineEffect(EffectKind.CONTINUE_INBOUND_REPLAY, None),
        nowait=True,
        epoch=1,
    )
    assert called == []
    assert client._engine.inbound.replay_pending is True
