"""Durable outbound records keep local ownership until deletion is confirmed."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from mqttium.enums import ConnectionState, OutboundQoSState, QoS
from mqttium.packets import PubAckPacket, PubCompPacket, PubRecPacket
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.negotiated import NegotiatedSettings
from tests.support import feed_engine


class _Boom(Exception):
    pass


def _store(kind: str, tmp_path: Path) -> Any:
    if kind == "memory":
        return MemoryInflightStore()
    return SqliteInflightStore(tmp_path / "inflight.db")


def _complete_first(engine: ProtocolEngine, mid: int, qos: QoS) -> None:
    if qos is QoS.AT_LEAST_ONCE:
        feed_engine(engine, PubAckPacket(mid=mid).encode())
        return
    feed_engine(engine, PubRecPacket(mid=mid).encode())
    engine.take_effects()
    feed_engine(engine, PubCompPacket(mid=mid).encode())


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_drain_delete_failure_keeps_fifo_ownership_until_retry(
    kind: str,
    qos: QoS,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(kind, tmp_path / f"{kind}-{int(qos)}")
    engine = ProtocolEngine(
        EngineConfig(local_receive_maximum=1, max_outbound_inflight=1),
        store,
    )
    engine.state = ConnectionState.CONNECTED

    first = engine.queue_publish("t/1", b"one", qos=qos)
    second = engine.queue_publish("t/2", b"two", qos=qos)
    third = engine.queue_publish("t/3", b"three", qos=qos)
    assert first.mid is not None and second.mid is not None and third.mid is not None
    engine.take_effects()
    assert [msg.mid for msg in engine._queued] == [second.mid, third.mid]

    original_transition = type(store).transition_out
    original_delete = type(store).delete_out
    faults = {"transition": 1, "delete": 1}

    def transition_out(
        self: Any,
        mid: int,
        expected: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> Any:
        if mid == second.mid and faults["transition"]:
            faults["transition"] -= 1
            raise _Boom("transition failed")
        return original_transition(self, mid, expected, new_state, compact=compact)

    def delete_out(self: Any, mid: int) -> bool:
        if mid == second.mid and faults["delete"]:
            faults["delete"] -= 1
            raise _Boom("delete failed")
        return original_delete(self, mid)

    monkeypatch.setattr(type(store), "transition_out", transition_out)
    monkeypatch.setattr(type(store), "delete_out", delete_out)

    _complete_first(engine, first.mid, qos)
    effects = engine.take_effects()

    assert any(e.kind is EffectKind.PUBLISH_COMPLETE and e.data == first.mid for e in effects)
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert not any(
        e.kind is EffectKind.PUBLISH_FAILED and getattr(e.data, "mid", None) == second.mid
        for e in effects
    )
    assert store.get_out(second.mid) is not None
    assert engine.pending_outbound_messages == 2
    assert engine.packet_ids.in_use(second.mid)
    assert engine.packet_ids.in_use(third.mid)
    assert [msg.mid for msg in engine._queued] == [second.mid, third.mid]
    assert engine.flow.inflight == 0

    # Both injected faults were transient. Retrying the existing drain resumes
    # from the same head; the later message cannot overtake it.
    engine.outbound.drain()
    retry_effects = engine.take_effects()
    assert any(e.kind is EffectKind.SEND for e in retry_effects)
    assert [msg.mid for msg in engine._queued] == [third.mid]
    assert store.get_out(second.mid).state is (
        OutboundQoSState.WAIT_PUBACK if qos is QoS.AT_LEAST_ONCE else OutboundQoSState.WAIT_PUBREC
    )
    assert engine.packet_ids.in_use(second.mid)
    assert engine.pending_outbound_messages == 2

    if isinstance(store, SqliteInflightStore):
        store.close()


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_replay_delete_failure_keeps_durable_record_and_primary_context(
    kind: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(kind, tmp_path / f"replay-{kind}")
    engine = ProtocolEngine(EngineConfig(clean_start=False), store)
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("t", b"payload", qos=QoS.AT_LEAST_ONCE)
    assert handle.mid is not None
    engine.take_effects()

    original_update = type(store).update_out
    original_delete = type(store).delete_out
    faults = {"update": 1, "delete": 1}

    def update_out(self: Any, msg: Any) -> None:
        if msg.mid == handle.mid and faults["update"]:
            faults["update"] -= 1
            raise _Boom("retransmit update failed")
        original_update(self, msg)

    def delete_out(self: Any, mid: int) -> bool:
        if mid == handle.mid and faults["delete"]:
            faults["delete"] -= 1
            raise _Boom("replay delete failed")
        return original_delete(self, mid)

    monkeypatch.setattr(type(store), "update_out", update_out)
    monkeypatch.setattr(type(store), "delete_out", delete_out)

    with pytest.raises(_Boom, match="replay delete failed") as raised:
        engine.outbound.replay_session()
    assert isinstance(raised.value.__context__, _Boom)
    assert str(raised.value.__context__) == "retransmit update failed"
    assert store.get_out(handle.mid) is not None
    assert engine.packet_ids.in_use(handle.mid)
    assert engine.pending_outbound_messages == 1

    engine.outbound.replay_session()
    assert any(e.kind is EffectKind.SEND for e in engine.take_effects())
    assert store.get_out(handle.mid) is not None
    assert engine.packet_ids.in_use(handle.mid)

    if isinstance(store, SqliteInflightStore):
        store.close()


def test_negotiation_discard_failure_keeps_queue_until_delete_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MemoryInflightStore()
    engine = ProtocolEngine(EngineConfig(), store)
    handle = engine.queue_publish("t", b"payload", qos=QoS.EXACTLY_ONCE)
    assert handle.mid is not None
    assert [msg.mid for msg in engine._queued] == [handle.mid]
    engine.negotiated = NegotiatedSettings(maximum_qos=0)

    original_delete = MemoryInflightStore.delete_out
    faults = 1

    def delete_out(self: MemoryInflightStore, mid: int) -> bool:
        nonlocal faults
        if mid == handle.mid and faults:
            faults -= 1
            raise _Boom("negotiation delete failed")
        return original_delete(self, mid)

    monkeypatch.setattr(MemoryInflightStore, "delete_out", delete_out)

    with pytest.raises(_Boom, match="negotiation delete failed") as raised:
        engine.outbound.fail_queued_violating_negotiation()
    assert raised.value.__context__ is not None
    assert store.get_out(handle.mid) is not None
    assert engine.packet_ids.in_use(handle.mid)
    assert engine.pending_outbound_messages == 1
    assert [msg.mid for msg in engine._queued] == [handle.mid]
    assert engine.take_effects() == []

    engine.outbound.fail_queued_violating_negotiation()
    effects = engine.take_effects()
    assert store.get_out(handle.mid) is None
    assert not engine.packet_ids.in_use(handle.mid)
    assert engine.pending_outbound_messages == 0
    assert list(engine._queued) == []
    assert any(
        e.kind is EffectKind.PUBLISH_FAILED and getattr(e.data, "mid", None) == handle.mid
        for e in effects
    )


def test_admission_rollback_still_preserves_primary_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MemoryInflightStore()
    engine = ProtocolEngine(EngineConfig(), store)
    engine.state = ConnectionState.CONNECTED

    def put_out(_self: MemoryInflightStore, _msg: Any) -> None:
        raise _Boom("primary put failure")

    def delete_out(_self: MemoryInflightStore, _mid: int) -> bool:
        raise _Boom("rollback delete failure")

    monkeypatch.setattr(MemoryInflightStore, "put_out", put_out)
    monkeypatch.setattr(MemoryInflightStore, "delete_out", delete_out)

    with pytest.raises(_Boom, match="primary put failure"):
        engine.queue_publish("t", b"payload", qos=QoS.AT_LEAST_ONCE)

    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0
    assert engine.flow.inflight == 0
    assert engine.packet_ids._used == set()
    assert list(engine._queued) == []
