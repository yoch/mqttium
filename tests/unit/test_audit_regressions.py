"""Regression tests from the correctness audit."""

from __future__ import annotations

from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.dispatch.matcher import TopicMatcher
from mqttium.enums import (
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import PubAckPacket, PubCompPacket, PublishPacket, encode_frame
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import OutboundMessage, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(v5: bool = False) -> bytes:
    if not v5:
        return encode_frame(PacketType.CONNACK, 0, bytes((0x00, 0x00)))
    body = bytes((0x00, 0x00)) + encode_properties(Properties(), "CONNACK")
    return encode_frame(PacketType.CONNACK, 0, body)


def test_wrong_puback_ignored_for_qos2() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    engine.begin_connect()
    _feed(engine, _connack())
    engine.take_effects()
    handle = engine.queue_publish("t", b"x", qos=2)
    engine.take_effects()
    _feed(engine, PubAckPacket(mid=handle.mid or 0).encode())
    effects = engine.take_effects()
    assert not any(e.kind is EffectKind.PUBLISH_COMPLETE for e in effects)
    assert engine.store.get_out(handle.mid or 0) is not None


def test_wrong_pubcomp_ignored_before_pubrec() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c1"))
    engine.begin_connect()
    _feed(engine, _connack())
    engine.take_effects()
    handle = engine.queue_publish("t", b"x", qos=2)
    engine.take_effects()
    _feed(engine, PubCompPacket(mid=handle.mid or 0).encode())
    effects = engine.take_effects()
    assert not any(e.kind is EffectKind.PUBLISH_COMPLETE for e in effects)
    assert engine.store.get_out(handle.mid or 0).state is OutboundQoSState.WAIT_PUBREC


def test_matcher_hash_matches_parent_level() -> None:
    m = TopicMatcher()
    m["foo/#"] = "x"
    assert list(m.iter_match("foo")) == ["x"]
    assert list(m.iter_match("foo/bar")) == ["x"]


def test_inbound_alias_rejected_when_maximum_zero() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c1",
            protocol=MQTTProtocolVersion.MQTTv5,
            topic_alias_maximum=0,
        )
    )
    engine.begin_connect()
    _feed(engine, _connack(v5=True))
    engine.take_effects()
    props = Properties()
    props.set("topic_alias", 1)
    pub = PublishPacket(
        topic="sensors/1",
        payload=b"x",
        qos=0,
        retain=False,
        dup=False,
        properties=props,
    )
    _feed(engine, pub.encode(MQTTProtocolVersion.MQTTv5))
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert not any(e.kind is EffectKind.MESSAGE for e in effects)


def test_sqlite_restart_replays_queued(tmp_path: Path) -> None:
    path = tmp_path / "s.db"
    store = SqliteInflightStore(path)
    store.put_out(
        OutboundMessage(
            mid=9,
            topic="offline",
            payload=b"1",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.QUEUED,
        )
    )
    store.close()

    store2 = SqliteInflightStore(path)
    engine = ProtocolEngine(EngineConfig(client_id="c1", clean_start=False), store=store2)
    assert engine.packet_ids.in_use(9)
    engine.begin_connect()
    # A QUEUED record was never sent, so it is not Client Session State.
    _feed(engine, encode_frame(PacketType.CONNACK, 0, bytes((0x00, 0x00))))
    effects = engine.take_effects()
    sends = [e for e in effects if e.kind is EffectKind.SEND]
    assert len(sends) == 1
    store2.close()


def test_drain_packets_beyond_limit() -> None:
    from mqttium.codec.buffer import IncrementalDecoder

    dec = IncrementalDecoder()
    frame = PublishPacket(topic="t", payload=b"x", qos=0, retain=False, dup=False).encode()
    dec.feed(frame * 150)
    first = dec.drain_packets(limit=100)
    assert len(first) == 100
    second = dec.drain_packets(limit=100)
    assert len(second) == 50
