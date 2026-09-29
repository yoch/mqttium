"""Admission is all-or-nothing: OutboundSession leaks nothing when it fails.

A QoS 1/2 publish acquires four resources — admission budget, packet id, store
row, flow-control slot — and every rollback bug fixed in the previous audits
was one of them surviving a failure. These tests fault-inject immediately after
each acquisition and compare a full snapshot of engine state against the one
taken before the call for each individual publication.

They run against both stores on purpose: the transactional one is what leaked
the byte budget historically, because its batch is already rolled back by the
time the except clause runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, OutboundQoSState, PacketType, QoS
from mqttium.errors import FlowControlError
from mqttium.packets import PublishPacket
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.outbound import OutboundSession


class _Boom(Exception):
    """Injected failure, distinguishable from a real protocol error."""


class _FailingPool:
    """Packet id pool that refuses to allocate, after the budget was reserved."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pool, name)

    def allocate(self) -> int:
        raise _Boom()


class _FailingFlow:
    """Flow window that raises *after* handing out a slot."""

    def __init__(self, flow: Any) -> None:
        self._flow = flow

    def __getattr__(self, name: str) -> Any:
        return getattr(self._flow, name)

    def try_acquire(self, mid: int) -> bool:
        self._flow.try_acquire(mid)
        raise _Boom()


class _FailingStore:
    """Store that fails put_out from the nth call on."""

    def __init__(self, store: Any, fail_from: int = 1) -> None:
        self._store = store
        self._fail_from = fail_from
        self.calls = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)

    def put_out(self, msg: Any) -> None:
        self.calls += 1
        if self.calls >= self._fail_from:
            raise _Boom()
        self._store.put_out(msg)


def _snapshot(engine: ProtocolEngine) -> dict[str, Any]:
    """Every piece of state a publish is allowed to touch."""
    return {
        "pending_messages": engine.unacknowledged_messages,
        "pending_bytes": engine.unacknowledged_bytes,
        "flow_inflight": engine.flow.inflight,
        "queued_mids": [msg.mid for msg in engine.outbound._queued],
        "used_mids": [mid for mid in range(1, 65536) if engine.packet_ids.in_use(mid)],
        "store_mids": sorted(
            msg.mid
            for msg in (
                engine.store.get_out(summary.mid)
                for page in engine.store.out_summary_pages()
                for summary in page
            )
        ),
        "effects": len(engine._effects),
    }


def _engine(tmp_path: Path | None = None, **config: Any) -> ProtocolEngine:
    store = (
        SqliteInflightStore(tmp_path / "inflight.db")
        if tmp_path is not None
        else MemoryInflightStore()
    )
    engine = ProtocolEngine(EngineConfig(**config), store)
    engine.state = ConnectionState.CONNECTED
    return engine


def _stores(tmp_path: Path) -> list[ProtocolEngine]:
    return [_engine(), _engine(tmp_path)]


# --- rejection before any resource is taken -----------------------------------


def test_invalid_publish_is_rejected_without_mutating(tmp_path: Path) -> None:
    """Validation runs before admission, so a rejected publish needs no undo."""
    for engine in _stores(tmp_path):
        before = _snapshot(engine)
        with pytest.raises(Exception):
            engine.queue_publish("a/+", b"x", qos=QoS.AT_LEAST_ONCE)
        assert _snapshot(engine) == before


# --- fault injection after each commit step -----------------------------------


@pytest.mark.parametrize(
    "step",
    ["allocate_mid", "put_out", "launch_encode", "flow_acquire"],
)
def test_rollback_restores_exact_state_after_failure_at_each_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    step: str,
) -> None:
    for index, engine in enumerate(_stores(tmp_path / step)):
        outbound = engine.outbound
        # Pre-load one accepted publish so rollback has to restore a non-empty
        # state rather than a pristine one.
        engine.queue_publish("pre/loaded", b"kept", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        before = _snapshot(engine)

        if step == "allocate_mid":
            outbound.packet_ids = _FailingPool(outbound.packet_ids)
        elif step == "put_out":
            outbound.store = _FailingStore(outbound.store)
        elif step == "launch_encode":
            monkeypatch.setattr(
                OutboundSession,
                "_launch",
                lambda self, msg, **_kwargs: (_ for _ in ()).throw(_Boom()),
            )
        else:  # flow_acquire — fail after the slot has been taken
            outbound.flow = _FailingFlow(outbound.flow)

        with pytest.raises(_Boom):
            engine.queue_publish("a/b", b"payload", qos=QoS.AT_LEAST_ONCE)

        assert _snapshot(engine) == before, f"{step} leaked state (store #{index})"
        monkeypatch.undo()


# --- batch: capacity rejection is mutation-free -------------------------------


# --- commit() succeeds completely ---------------------------------------------


# --- the launch decision is the validation decision ---------------------------


def test_launch_decision_matches_the_validation_snapshot(tmp_path: Path) -> None:
    """`topic_bytes is not None` stands in for `CONNECTED and window free`.

    `queue_publish` decides whether to launch a QoS > 0 message from the topic
    bytes produced during validation, rather than re-reading connection state
    and the flow window a second time. The substitution is only sound while
    nothing between the two observations can change either, so pin both
    outcomes: a free window launches and takes exactly one slot, a full window
    queues and takes none.
    """
    for engine in [
        _engine(max_inbound_inflight=1, max_outbound_inflight=1),
        _engine(tmp_path / "decision", max_inbound_inflight=1, max_outbound_inflight=1),
    ]:
        launched = engine.queue_publish("a/b", b"1", qos=QoS.AT_LEAST_ONCE)
        assert engine.flow.inflight == 1
        assert [e.kind for e in engine.take_effects()] == [EffectKind.SEND]
        assert engine.store.get_out(launched.mid or 0).state is OutboundQoSState.WAIT_PUBACK
        assert [msg.mid for msg in engine.outbound._queued] == []

        # Window now full: same connection state, opposite decision.
        queued = engine.queue_publish("a/b", b"2", qos=QoS.AT_LEAST_ONCE)
        assert engine.flow.inflight == 1, "a queued publish must not take a slot"
        assert engine.take_effects() == []
        assert engine.store.get_out(queued.mid or 0).state is OutboundQoSState.QUEUED
        assert [msg.mid for msg in engine.outbound._queued] == [queued.mid]


# --- one publication larger than the byte budget -------------------------------


def test_an_oversized_publication_is_admitted_alone_into_an_empty_budget(
    tmp_path: Path,
) -> None:
    for engine in [
        _engine(max_unacknowledged_bytes=64),
        _engine(tmp_path, max_unacknowledged_bytes=64),
    ]:
        large = engine.queue_publish("a/b", b"x" * 1000, qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        assert engine.unacknowledged_bytes > 64

        # Nothing else joins it, not even a small publication.
        before = _snapshot(engine)
        with pytest.raises(FlowControlError):
            engine.queue_publish("a/b", b"y", qos=QoS.AT_LEAST_ONCE)
        assert _snapshot(engine) == before

        engine.handle_raw(RawPacket(PacketType.PUBACK, 0, (large.mid or 0).to_bytes(2, "big")))
        engine.take_effects()
        assert engine.unacknowledged_bytes == 0
        engine.queue_publish("a/b", b"y", qos=QoS.AT_LEAST_ONCE)
        # A second oversized publication waits for the budget to empty again.
        with pytest.raises(FlowControlError):
            engine.queue_publish("a/b", b"x" * 1000, qos=QoS.AT_LEAST_ONCE)


# --- a discarded broker session requeues QoS 1 with everything it owns --------


def _clean_connack(engine: ProtocolEngine) -> list[Any]:
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))
    return engine.take_effects()


def _published(effects: list[Any]) -> list[PublishPacket]:
    decoder = IncrementalDecoder()
    for effect in effects:
        if effect.kind is EffectKind.SEND:
            data = effect.data
            decoder.feed(data if isinstance(data, bytes) else data[0] + data[1])
    return [PublishPacket.decode(raw.flags, raw.remaining) for raw in decoder.drain_packets()]


def test_requeued_qos1_keeps_its_id_budget_and_row(tmp_path: Path) -> None:
    for engine in _stores(tmp_path):
        sent = engine.queue_publish("a/b", b"1", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        before = _snapshot(engine)

        effects = _clean_connack(engine)

        assert [(p.mid, p.dup, p.payload) for p in _published(effects)] == [(sent.mid, False, b"1")]
        assert not any(e.kind is EffectKind.PUBLISH_FAILED for e in effects)
        after = _snapshot(engine)
        assert {key: after[key] for key in ("pending_messages", "pending_bytes", "used_mids")} == {
            key: before[key] for key in ("pending_messages", "pending_bytes", "used_mids")
        }
        assert engine.flow.inflight == 1
        assert engine.store.get_out(sent.mid or 0).state is OutboundQoSState.WAIT_PUBACK


def test_requeued_qos1_leaves_as_a_first_attempt_after_a_replay() -> None:
    """A PUBLISH already resent with DUP on a resumed session is new again later."""
    engine = _engine(client_id="c", clean_start=False)
    sent = engine.queue_publish("a/b", b"1", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))
    assert [(p.mid, p.dup) for p in _published(engine.take_effects())] == [(sent.mid, True)]

    assert [(p.mid, p.dup) for p in _published(_clean_connack(engine))] == [(sent.mid, False)]


def test_restarted_process_resends_a_stored_qos1_on_a_new_session(tmp_path: Path) -> None:
    path = tmp_path / "restart.db"
    with SqliteInflightStore(path) as store:
        first = ProtocolEngine(EngineConfig(client_id="c", clean_start=False), store)
        first.state = ConnectionState.CONNECTED
        sent = first.queue_publish("a/b", b"kept", qos=QoS.AT_LEAST_ONCE)
        first.take_effects()
    with SqliteInflightStore(path) as store:
        engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False), store)
        engine.begin_connect()
        engine.take_effects()
        engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))
        effects = engine.take_effects()
        assert [(p.mid, p.dup, p.payload) for p in _published(effects)] == [
            (sent.mid, False, b"kept")
        ]
        assert not any(e.kind is EffectKind.PUBLISH_FAILED for e in effects)
