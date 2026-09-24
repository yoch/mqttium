"""Successful PUBREC keeps the QoS 2 sender phases explicit.

A successful PUBREC always requires a PUBREL [MQTT-4.3.3-4]. In WAIT_PUBREC
it moves the exchange to WAIT_PUBCOMP; in WAIT_PUBCOMP (a repeated PUBREC) it
resends PUBREL without changing phase or ownership (#503). When the exchange
is replay-parked behind the send quota, its queue entry leaves with the
phase change, so drain() neither resends PUBREL nor spends a quota slot on
it (#497).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.persistence.memory import InflightStore, MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import OutboundMessage, Properties
from tests.support import stored_record


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _sent(engine: ProtocolEngine) -> list[tuple[int, int]]:
    """(packet type, packet id) of every SEND, in order."""
    sent = []
    for effect in engine.take_effects():
        if effect.kind is not EffectKind.SEND:
            continue
        item = effect.data if isinstance(effect.data, bytes) else effect.data[0] + effect.data[1]
        decoder = IncrementalDecoder()
        decoder.feed(item)
        raw = decoder.next_packet()
        assert raw is not None
        if raw.packet_type is PacketType.PUBLISH:
            topic_length = int.from_bytes(raw.remaining[:2], "big")
            mid = int.from_bytes(raw.remaining[2 + topic_length : 4 + topic_length], "big")
        else:
            mid = int.from_bytes(raw.remaining[:2], "big")
        sent.append((raw.packet_type.value, mid))
    return sent


def _pubrec(mid: int) -> bytes:
    return encode_frame(PacketType.PUBREC, 0, pack_u16(mid))


PUBREL = PacketType.PUBREL.value
PUBLISH = PacketType.PUBLISH.value


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
def test_repeated_successful_pubrec_resends_pubrel_without_changing_phase(
    protocol: MQTTProtocolVersion,
) -> None:
    engine = ProtocolEngine(EngineConfig(client_id="pubrec", protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("t", b"x", qos=QoS.EXACTLY_ONCE)
    mid = handle.mid
    assert mid is not None
    engine.take_effects()

    _feed(engine, _pubrec(mid))
    assert _sent(engine) == [(PUBREL, mid)]
    inflight = engine.outbound.flow.inflight

    _feed(engine, _pubrec(mid))
    effects = engine.take_effects()
    assert [e.kind for e in effects] == [EffectKind.SEND]
    assert effects[0].data == encode_frame(PacketType.PUBREL, 2, pack_u16(mid))
    record = engine.store.get_out(mid)
    assert record is not None and record.state is OutboundQoSState.WAIT_PUBCOMP
    assert engine.packet_ids.in_use(mid)
    assert engine.outbound.flow.inflight == inflight
    assert engine.outbound.unacknowledged_messages == 1

    _feed(engine, encode_frame(PacketType.PUBCOMP, 0, pack_u16(mid)))
    assert [e.kind for e in engine.take_effects()] == [EffectKind.PUBLISH_COMPLETE]
    assert engine.store.get_out(mid) is None
    assert not engine.packet_ids.in_use(mid)


def _store(kind: str, tmp_path: Path) -> InflightStore:
    if kind == "memory":
        return MemoryInflightStore()
    return SqliteInflightStore(tmp_path / "parked.db")


def _record(mid: int, qos: QoS, state: OutboundQoSState) -> OutboundMessage:
    return stored_record(
        OutboundMessage(
            mid=mid, topic=f"t/{mid}", payload=b"payload", qos=qos, retain=False, state=state
        )
    )


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_pubrec_for_parked_exchange_removes_its_queue_entry(
    store_kind: str, tmp_path: Path
) -> None:
    store = _store(store_kind, tmp_path)
    store.put_out(_record(1, QoS.AT_LEAST_ONCE, OutboundQoSState.WAIT_PUBACK))
    store.put_out(_record(2, QoS.EXACTLY_ONCE, OutboundQoSState.WAIT_PUBREC))
    engine = ProtocolEngine(
        EngineConfig(client_id="parked", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False),
        store,
    )
    queued = engine.queue_publish("t/3", b"third", qos=QoS.AT_LEAST_ONCE)
    assert queued.mid == 3
    engine.begin_connect()
    engine.take_effects()
    body = bytes((0x01, 0x00)) + encode_properties(Properties({"receive_maximum": 1}), "CONNACK")
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    # mid 1 takes the only slot; mid 2 is parked, then the never-sent mid 3.
    assert _sent(engine) == [(PUBLISH, 1)]
    assert [stored.mid for stored in engine.outbound._queued] == [2, 3]

    _feed(engine, _pubrec(2))
    assert _sent(engine) == [(PUBREL, 2)]
    assert [stored.mid for stored in engine.outbound._queued] == [3]
    record = store.get_out(2)
    assert record is not None and record.state is OutboundQoSState.WAIT_PUBCOMP
    assert engine.outbound.flow.inflight == 1

    # Settling mid 1 frees the slot for mid 3, not for a second PUBREL(2).
    _feed(engine, encode_frame(PacketType.PUBACK, 0, pack_u16(1)))
    assert _sent(engine) == [(PUBLISH, 3)]
    assert engine.outbound._queued == type(engine.outbound._queued)()
    assert engine.outbound.flow.inflight == 1
    assert engine.packet_ids.in_use(2)

    _feed(engine, encode_frame(PacketType.PUBCOMP, 0, pack_u16(2)))
    assert store.get_out(2) is None
    if isinstance(store, SqliteInflightStore):
        store.close()
