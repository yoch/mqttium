"""Admission is all-or-nothing: OutboundSession leaks nothing when it fails.

A QoS 1/2 publish acquires four resources — admission budget, packet id, store
row, flow-control slot — and every rollback bug fixed in the previous audits
was one of them surviving a failure. These tests fault-inject immediately after
each acquisition and compare a full snapshot of engine state against the one
taken before the call, for a single publish and for a chunk.

They run against both stores on purpose: the transactional one is what leaked
the byte budget historically, because its batch is already rolled back by the
time the except clause runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mqttium.enums import ConnectionState, QoS
from mqttium.errors import FlowControlError
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
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

    def try_acquire(self) -> bool:
        self._flow.try_acquire()
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
        "pending_messages": engine.pending_outbound_messages,
        "pending_bytes": engine.pending_outbound_bytes,
        "flow_inflight": engine.flow.inflight,
        "queued_mids": [msg.mid for msg in engine._queued],
        "used_mids": sorted(engine.packet_ids._used),
        "store_mids": sorted(msg.mid for msg in engine.store.out_items()),
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
                OutboundSession, "_launch", lambda self, msg: (_ for _ in ()).throw(_Boom())
            )
        else:  # flow_acquire — fail after the slot has been taken
            outbound.flow = _FailingFlow(outbound.flow)

        with pytest.raises(_Boom):
            engine.queue_publish("a/b", b"payload", qos=QoS.AT_LEAST_ONCE)

        assert _snapshot(engine) == before, f"{step} leaked state (store #{index})"
        monkeypatch.undo()


# --- batch: capacity rejection is mutation-free -------------------------------


def test_batch_over_capacity_fails_without_mutating(tmp_path: Path) -> None:
    for engine in [
        _engine(max_pending_outbound_messages=3),
        _engine(tmp_path, max_pending_outbound_messages=3),
    ]:
        before = _snapshot(engine)
        batch = [(f"t/{i}", b"x", QoS.AT_LEAST_ONCE, False, None) for i in range(4)]
        with pytest.raises(FlowControlError, match="message limit"):
            engine.queue_publish_many(batch)
        assert _snapshot(engine) == before
        # No packet id was burned, so the next publish still gets mid 1.
        assert engine.queue_publish("t/0", b"x", qos=QoS.AT_LEAST_ONCE).mid == 1


def test_batch_over_message_limit_does_no_work_before_rejecting(tmp_path: Path) -> None:
    """The count check must cost nothing, not be undone.

    Admitting the prefix and restoring it afterwards is also correct, so a
    state-only assertion cannot tell the two apart. Count the side effects
    instead: AsyncClient retries the same chunk after waiting for space, and
    each retry has to be free.
    """

    class _CountingStore:
        def __init__(self, store: Any) -> None:
            self._store = store
            self.writes = 0

        def __getattr__(self, name: str) -> Any:
            return getattr(self._store, name)

        def put_out(self, msg: Any) -> None:
            self.writes += 1
            self._store.put_out(msg)

    for engine in [
        _engine(max_pending_outbound_messages=3),
        _engine(tmp_path, max_pending_outbound_messages=3),
    ]:
        counting = _CountingStore(engine.outbound.store)
        engine.outbound.store = counting
        batch = [(f"t/{i}", b"x", QoS.AT_LEAST_ONCE, False, None) for i in range(4)]
        with pytest.raises(FlowControlError, match="message limit"):
            engine.queue_publish_many(batch)
        assert counting.writes == 0
        assert engine.packet_ids._used == set()


def test_batch_of_qos0_is_not_counted_against_the_message_limit(tmp_path: Path) -> None:
    """QoS 0 reserves nothing, so a large QoS 0 chunk must not be pre-rejected."""
    for engine in [
        _engine(max_pending_outbound_messages=1),
        _engine(tmp_path, max_pending_outbound_messages=1),
    ]:
        batch = [(f"t/{i}", b"x", QoS.AT_MOST_ONCE, False, None) for i in range(10)]
        handles = engine.queue_publish_many(batch)
        assert [h.mid for h in handles] == [None] * 10


def test_batch_over_byte_capacity_fails_without_mutating(tmp_path: Path) -> None:
    for engine in [
        _engine(max_pending_outbound_bytes=20),
        _engine(tmp_path, max_pending_outbound_bytes=20),
    ]:
        before = _snapshot(engine)
        batch = [("t", b"x" * 8, QoS.AT_LEAST_ONCE, False, None) for _ in range(3)]
        with pytest.raises(FlowControlError, match="byte limit"):
            engine.queue_publish_many(batch)
        assert _snapshot(engine) == before


def test_batch_validation_failure_rolls_back_earlier_messages(tmp_path: Path) -> None:
    for engine in _stores(tmp_path):
        before = _snapshot(engine)
        batch = [
            ("good/one", b"x", QoS.AT_LEAST_ONCE, False, None),
            ("good/two", b"x", QoS.AT_LEAST_ONCE, False, None),
            ("bad/+", b"x", QoS.AT_LEAST_ONCE, False, None),  # invalid filter
        ]
        with pytest.raises(Exception):
            engine.queue_publish_many(batch)
        assert _snapshot(engine) == before


def test_batch_store_failure_rolls_back_every_acquired_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for engine in _stores(tmp_path):
        outbound = engine.outbound
        engine.queue_publish("pre/loaded", b"kept", qos=QoS.AT_LEAST_ONCE)
        engine.take_effects()
        before = _snapshot(engine)

        outbound.store = _FailingStore(outbound.store, fail_from=3)
        batch = [(f"t/{i}", b"payload", QoS.AT_LEAST_ONCE, False, None) for i in range(5)]
        with pytest.raises(_Boom):
            engine.queue_publish_many(batch)

        assert _snapshot(engine) == before
        monkeypatch.undo()


# --- commit() succeeds completely ---------------------------------------------


def test_commit_accounts_exactly_once_per_message(tmp_path: Path) -> None:
    for engine in [
        _engine(local_receive_maximum=2),
        _engine(tmp_path, local_receive_maximum=2),
    ]:
        batch = [(f"t/{i}", b"payload", QoS.AT_LEAST_ONCE, False, None) for i in range(5)]
        handles = engine.queue_publish_many(batch)

        assert [h.mid for h in handles] == [1, 2, 3, 4, 5]
        assert engine.pending_outbound_messages == 5
        assert engine.pending_outbound_bytes == sum(
            len(b"payload") + len(f"t/{i}") for i in range(5)
        )
        # Only the flow window went out; the rest is queued, and every message
        # is in the store exactly once either way.
        assert engine.flow.inflight == 2
        assert [msg.mid for msg in engine._queued] == [3, 4, 5]
        assert sorted(msg.mid for msg in engine.store.out_items()) == [1, 2, 3, 4, 5]


def test_qos0_in_a_batch_acquires_nothing(tmp_path: Path) -> None:
    for engine in _stores(tmp_path):
        before = _snapshot(engine)
        handles = engine.queue_publish_many(
            [
                ("t/0", b"x", QoS.AT_MOST_ONCE, False, None),
                ("t/1", b"x", QoS.AT_MOST_ONCE, False, None),
            ]
        )
        assert [h.mid for h in handles] == [None, None]
        after = _snapshot(engine)
        assert after["pending_messages"] == before["pending_messages"]
        assert after["pending_bytes"] == before["pending_bytes"]
        assert after["used_mids"] == before["used_mids"]
        assert after["store_mids"] == before["store_mids"]
