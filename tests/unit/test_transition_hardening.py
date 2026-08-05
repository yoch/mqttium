"""Adversarial coverage for conditional transition and batch invariants."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mqttium.enums import InboundQoSState, OutboundQoSState, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PubRecPacket, PubRelPacket, PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage
from tests.support import feed_engine


def connack() -> bytes:
    return encode_frame(PacketType.CONNACK, 0, b"\x00\x00")


def connected_engine(store: object, *, manual_ack: bool = False) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="transition-hardening", manual_ack=manual_ack),
        store=store,  # type: ignore[arg-type]
    )
    engine.begin_connect()
    feed_engine(engine, connack())
    engine.take_effects()
    return engine


def send_effects(engine: ProtocolEngine) -> list[object]:
    return [effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND]


class RefusingTransitionStore(MemoryInflightStore):
    """Expose a metadata read, then refuse the following conditional mutation."""

    refuse_out_transition = False
    refuse_in_transition = False
    refuse_in_complete = False

    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ):
        if self.refuse_out_transition:
            return None
        return super().transition_out(mid, expected_state, new_state, compact=compact)

    def transition_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
        new_state: InboundQoSState,
        *,
        user_acked: bool | None = None,
    ):
        if self.refuse_in_transition:
            return None
        return super().transition_in(
            mid,
            expected_state,
            new_state,
            user_acked=user_acked,
        )

    def complete_in(self, mid: int, expected_state: InboundQoSState):
        if self.refuse_in_complete:
            return None
        return super().complete_in(mid, expected_state)


def test_pubrec_emits_no_pubrel_when_the_conditional_transition_is_refused() -> None:
    store = RefusingTransitionStore()
    engine = connected_engine(store)
    handle = engine.queue_publish("a/b", b"payload", qos=QoS.EXACTLY_ONCE)
    mid = handle.mid or 0
    engine.take_effects()
    store.refuse_out_transition = True

    feed_engine(engine, PubRecPacket(mid=mid).encode())

    assert send_effects(engine) == []
    record = store.get_out(mid)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBREC
    assert engine.flow.inflight == 1
    assert engine.pending_outbound_messages == 1


def test_manual_ack_releases_nothing_when_conditional_completion_is_refused() -> None:
    store = RefusingTransitionStore()
    engine = connected_engine(store, manual_ack=True)
    feed_engine(
        engine,
        PublishPacket(
            topic="in/qos1",
            payload=b"body",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=7,
        ).encode(),
    )
    engine.take_effects()
    store.refuse_in_complete = True

    with pytest.raises(ProtocolError, match="changed while acknowledging"):
        engine.ack(7)

    assert send_effects(engine) == []
    assert store.get_in(7) is not None
    assert engine._inbound_inflight == 1


def test_pubrel_rejects_a_qos1_record_without_deleting_or_releasing_it() -> None:
    store = MemoryInflightStore()
    engine = connected_engine(store, manual_ack=True)
    feed_engine(
        engine,
        PublishPacket(
            topic="in/qos1",
            payload=b"body",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=9,
        ).encode(),
    )
    engine.take_effects()

    feed_engine(engine, PubRelPacket(mid=9).encode())
    effects = engine.take_effects()

    assert not any(effect.kind is EffectKind.SEND for effect in effects)
    assert any(
        effect.kind is EffectKind.PROTOCOL_ERROR and "invalid state" in str(effect.data)
        for effect in effects
    )
    record = store.get_in(9)
    assert record is not None
    assert record.state is InboundQoSState.WAIT_PUBACK
    assert engine._inbound_inflight == 1


def outbound(mid: int, state: OutboundQoSState) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic="a/b",
        payload=b"payload",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=state,
        logical_size=10,
    )


def inbound(mid: int, state: InboundQoSState) -> InboundMessage:
    return InboundMessage(
        mid=mid,
        topic="a/b",
        payload=b"payload",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=state,
    )


class ConcurrentOutboundChangeStore(SqliteInflightStore):
    interfere = False

    def _out_transition_row(self, mid: int):
        row = super()._out_transition_row(mid)
        if self.interfere and row is not None:
            self.interfere = False
            other = sqlite3.connect(self._path)
            try:
                other.execute(
                    "UPDATE outbound SET state=? WHERE mid=?",
                    (int(OutboundQoSState.WAIT_PUBCOMP), mid),
                )
                other.commit()
            finally:
                other.close()
        return row


class ConcurrentInboundChangeStore(SqliteInflightStore):
    interfere = False

    def _in_transition_row(self, mid: int):
        row = super()._in_transition_row(mid)
        if self.interfere and row is not None:
            self.interfere = False
            other = sqlite3.connect(self._path)
            try:
                other.execute("UPDATE inbound SET user_acked=1 WHERE mid=?", (mid,))
                other.commit()
            finally:
                other.close()
        return row


def test_sqlite_outbound_transition_refuses_a_stale_cross_connection_read(
    tmp_path: Path,
) -> None:
    store = ConcurrentOutboundChangeStore(tmp_path / "out-race.db")
    store.put_out(outbound(1, OutboundQoSState.WAIT_PUBREC))
    store.interfere = True

    changed = store.transition_out(
        1,
        OutboundQoSState.WAIT_PUBREC,
        OutboundQoSState.WAIT_PUBCOMP,
    )

    assert changed is None
    record = store.get_out(1)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBCOMP
    store.close()


def test_sqlite_inbound_completion_refuses_a_stale_cross_connection_read(
    tmp_path: Path,
) -> None:
    store = ConcurrentInboundChangeStore(tmp_path / "in-race.db")
    store.put_in(inbound(1, InboundQoSState.WAIT_PUBREL))
    store.interfere = True

    completed = store.complete_in(1, InboundQoSState.WAIT_PUBREL)

    assert completed is None
    record = store.get_in(1)
    assert record is not None
    assert record.user_acked is True
    store.close()


def test_caught_nested_batch_failure_makes_the_outer_batch_rollback_only(
    tmp_path: Path,
) -> None:
    store = SqliteInflightStore(tmp_path / "rollback-only.db")

    with pytest.raises(RuntimeError, match="nested batch failure"):
        with store.batch():
            try:
                with store.batch():
                    store.put_out(outbound(1, OutboundQoSState.WAIT_PUBACK))
                    raise ValueError("inner failure")
            except ValueError:
                pass

    assert store.get_out(1) is None
    assert store._transaction_started is False
    assert store._rollback_only is False

    # The failed transaction leaves the store immediately reusable.
    store.put_out(outbound(2, OutboundQoSState.WAIT_PUBACK))
    assert store.get_out(2) is not None
    store.close()
