"""A replay-parked WAIT_* exchange settled while queued must leave the queue.

``replay_session()`` parks a WAIT_* record in ``_queued`` when the send quota
cannot admit its retransmission. The broker still owns the exchange and may
settle it while it is parked; a stale queue entry then made ``drain()``
re-materialise the deleted record, resurrect it through ``update_out`` and
retransmit a settled publication. Found by
``tests/fuzz/test_stateful_invariants.py`` (seed 1, step 188).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.persistence.memory import InflightStore, MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import OutboundMessage, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _sent_packet_types(engine: ProtocolEngine) -> list[int]:
    sends = [effect.data for effect in engine.take_effects() if effect.kind is EffectKind.SEND]
    return [(item if isinstance(item, bytes) else item[0])[0] & 0xF0 for item in sends]


def _store(kind: str, tmp_path: Path) -> InflightStore:
    if kind == "memory":
        return MemoryInflightStore()
    return SqliteInflightStore(tmp_path / "parked.db")


def _resume_with_parked_wait_puback(store: InflightStore) -> ProtocolEngine:
    """Resume a session whose second WAIT_PUBACK cannot fit the send quota."""
    for mid in (1, 2):
        store.put_out(
            OutboundMessage(
                mid=mid,
                topic=f"t/{mid}",
                payload=b"payload",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.WAIT_PUBACK,
            )
        )
    engine = ProtocolEngine(
        EngineConfig(
            client_id="parked-settle",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
        ),
        store,
    )
    engine.begin_connect()
    body = bytearray((0x01, 0x00))
    body.extend(encode_properties(Properties({"receive_maximum": 1}), "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, bytes(body)))

    # Replay retransmitted mid=1 and parked mid=2 behind the exhausted quota.
    assert _sent_packet_types(engine).count(PacketType.PUBLISH.value) == 1
    assert [stored.mid for stored in engine.outbound._queued] == [2]
    assert engine.outbound.pending_messages == 2
    return engine


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_puback_for_parked_exchange_removes_its_queue_entry(
    store_kind: str, tmp_path: Path
) -> None:
    store = _store(store_kind, tmp_path)
    engine = _resume_with_parked_wait_puback(store)

    # The broker settles the parked exchange before a quota slot frees up.
    _feed(engine, encode_frame(PacketType.PUBACK, 0, b"\x00\x02"))
    completions = [
        effect.data
        for effect in engine.take_effects()
        if effect.kind is EffectKind.PUBLISH_COMPLETE
    ]

    assert completions == [2]
    assert [stored.mid for stored in engine.outbound._queued] == []
    assert [record.mid for record in store.out_items()] == [1]
    assert engine.outbound.pending_messages == 1

    # Settling mid=1 drains the queue again: the settled record must stay
    # deleted and nothing may retransmit its PUBLISH.
    _feed(engine, encode_frame(PacketType.PUBACK, 0, b"\x00\x01"))
    assert PacketType.PUBLISH.value not in _sent_packet_types(engine)
    assert list(store.out_items()) == []
    assert engine.outbound.pending_messages == 0
    assert engine.outbound.pending_bytes == 0
    assert len(engine.packet_ids) == 0


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_settled_parked_identifier_can_be_reused_without_collision(
    store_kind: str, tmp_path: Path
) -> None:
    store = _store(store_kind, tmp_path)
    engine = _resume_with_parked_wait_puback(store)

    _feed(engine, encode_frame(PacketType.PUBACK, 0, b"\x00\x02"))
    engine.take_effects()
    # Settling the parked exchange replenished the send quota it never held on
    # this connection — the same §4.9 credit semantics a resumed PUBCOMP has.
    assert engine.outbound.flow.inflight == 0

    # The freed identifier is reallocated by a fresh publication, which
    # launches on the replenished quota without a duplicate queue entry.
    handle = engine.queue_publish("t/new", b"fresh", qos=QoS.AT_LEAST_ONCE)
    assert _sent_packet_types(engine).count(PacketType.PUBLISH.value) == 1
    assert handle.mid == 2
    assert [stored.mid for stored in engine.outbound._queued] == []
    records = {record.mid: record.state for record in store.out_items()}
    assert records == {1: OutboundQoSState.WAIT_PUBACK, 2: OutboundQoSState.WAIT_PUBACK}
    assert engine.outbound.pending_messages == 2

    # Settling mid=1 must not resurrect anything through a stale queue entry.
    _feed(engine, encode_frame(PacketType.PUBACK, 0, b"\x00\x01"))
    assert PacketType.PUBLISH.value not in _sent_packet_types(engine)
    records = {record.mid: record.state for record in store.out_items()}
    assert records == {2: OutboundQoSState.WAIT_PUBACK}
    assert engine.outbound.pending_messages == 1
