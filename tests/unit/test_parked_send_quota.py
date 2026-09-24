"""Settling an exchange never releases a send-quota slot it did not hold.

After a resumed session, an exchange whose PUBLISH was not sent on the
current connection (replay-parked behind Receive Maximum, a replayed PUBREL,
or a parked exchange advanced by PUBREC) holds no slot. Its terminal ACK must
not free a slot another exchange owns, or more PUBLISHes than Receive Maximum
would be unacknowledged on this connection (#545).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16
from mqttium.codec.properties import encode_properties
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
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


def _published(engine: ProtocolEngine) -> list[int]:
    mids = []
    for effect in engine.take_effects():
        if effect.kind is not EffectKind.SEND:
            continue
        item = effect.data if isinstance(effect.data, bytes) else effect.data[0] + effect.data[1]
        if item[0] & 0xF0 == PacketType.PUBLISH.value:
            decoder = IncrementalDecoder()
            decoder.feed(item)
            raw = decoder.next_packet()
            assert raw is not None
            topic_length = int.from_bytes(raw.remaining[:2], "big")
            mids.append(int.from_bytes(raw.remaining[2 + topic_length : 4 + topic_length], "big"))
    return mids


def _resume(kind: str, tmp_path: Path, records: list[tuple[int, QoS, OutboundQoSState]]):
    store: InflightStore = (
        MemoryInflightStore() if kind == "memory" else SqliteInflightStore(tmp_path / "q.db")
    )
    for mid, qos, state in records:
        store.put_out(
            stored_record(
                OutboundMessage(
                    mid=mid, topic=f"t/{mid}", payload=b"p", qos=qos, retain=False, state=state
                )
            )
        )
    engine = ProtocolEngine(
        EngineConfig(client_id="quota", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False),
        store,
    )
    engine.begin_connect()
    body = bytes((0x01, 0x00)) + encode_properties(Properties({"receive_maximum": 1}), "CONNACK")
    _feed(engine, encode_frame(PacketType.CONNACK, 0, body))
    return engine, store


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_settling_a_parked_exchange_does_not_launch_another_publish(
    kind: str, tmp_path: Path
) -> None:
    wait = OutboundQoSState.WAIT_PUBACK
    engine, store = _resume(kind, tmp_path, [(m, QoS.AT_LEAST_ONCE, wait) for m in (1, 2, 3)])
    assert _published(engine) == [1]
    assert [stored.mid for stored in engine.outbound._queued] == [2, 3]

    _feed(engine, encode_frame(PacketType.PUBACK, 0, pack_u16(2)))  # parked
    assert _published(engine) == []  # mid 3 waits: mid 1 still owns the slot
    assert engine.outbound.flow.inflight == 1
    assert [stored.mid for stored in engine.outbound._queued] == [3]

    _feed(engine, encode_frame(PacketType.PUBACK, 0, pack_u16(1)))  # active
    assert _published(engine) == [3]
    assert engine.outbound.flow.inflight == 1
    if isinstance(store, SqliteInflightStore):
        store.close()


def test_negative_pubrec_for_a_parked_exchange_releases_no_slot(tmp_path: Path) -> None:
    wait = OutboundQoSState.WAIT_PUBREC
    engine, _ = _resume("memory", tmp_path, [(m, QoS.EXACTLY_ONCE, wait) for m in (1, 2, 3)])
    assert _published(engine) == [1]

    _feed(engine, encode_frame(PacketType.PUBREC, 0, pack_u16(2) + b"\x80\x00"))
    effects = engine.take_effects()
    assert [e.data.mid for e in effects if e.kind is EffectKind.PUBLISH_FAILED] == [2]
    assert engine.outbound.flow.inflight == 1
    assert [stored.mid for stored in engine.outbound._queued] == [3]


def test_pubcomp_of_an_exchange_unparked_by_pubrec_releases_no_slot(tmp_path: Path) -> None:
    wait = OutboundQoSState.WAIT_PUBREC
    engine, _ = _resume("memory", tmp_path, [(m, QoS.EXACTLY_ONCE, wait) for m in (1, 2, 3)])
    assert _published(engine) == [1]
    _feed(engine, encode_frame(PacketType.PUBREC, 0, pack_u16(2)))  # unparked
    _feed(engine, encode_frame(PacketType.PUBCOMP, 0, pack_u16(2)))
    assert _published(engine) == []
    assert engine.outbound.flow.inflight == 1
