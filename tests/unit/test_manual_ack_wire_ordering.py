"""Manual QoS 1 PUBACK ordering relative to durable transitions (#247)."""

from __future__ import annotations

from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(*, session_present: bool = False) -> bytes:
    body = bytearray([0x01 if session_present else 0x00, 0x00])
    body.extend(encode_properties(None, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def _qos1_publish(mid: int, *, dup: bool = False) -> bytes:
    return PublishPacket(
        topic="a/b",
        payload=b"v",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=dup,
        mid=mid,
        properties=None,
    ).encode(MQTTProtocolVersion.MQTTv5)


def _engine(store: MemoryInflightStore | SqliteInflightStore | None = None) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="bug247",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            manual_ack=True,
            local_receive_maximum=8,
        ),
        store,
    )
    engine.begin_connect()
    _feed(engine, _connack())
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    return engine


def _puback_mids(engine: ProtocolEngine) -> list[int]:
    sends = [e.data for e in engine.take_effects() if e.kind is EffectKind.SEND_ACK]
    return [int.from_bytes(frame[2:4], "big") for frame in sends if frame[0] >> 4 == 4]


def test_manual_ack_pubacks_follow_publish_arrival_order() -> None:
    engine = _engine()
    for mid in (1, 2):
        _feed(engine, _qos1_publish(mid))
    engine.take_effects()

    engine.inbound.ack(2)
    assert _puback_mids(engine) == []
    assert engine.inbound.stats().inflight == 2

    engine.inbound.ack(1)
    assert _puback_mids(engine) == [1, 2]
    assert engine.inbound.stats().inflight == 0


def test_ready_prefix_flushes_once_in_arrival_order() -> None:
    engine = _engine()
    for mid in (5, 3, 9):
        _feed(engine, _qos1_publish(mid))
    engine.take_effects()

    engine.inbound.ack(9)
    engine.inbound.ack(3)
    engine.inbound.ack(3)
    assert _puback_mids(engine) == []

    engine.inbound.ack(5)
    assert _puback_mids(engine) == [5, 3, 9]


def test_duplicate_publish_does_not_duplicate_order_entry() -> None:
    engine = _engine()
    _feed(engine, _qos1_publish(1))
    _feed(engine, _qos1_publish(2))
    engine.take_effects()

    engine.inbound.ack(2)
    _feed(engine, _qos1_publish(2, dup=True))
    engine.take_effects()
    engine.inbound.ack(2)
    engine.inbound.ack(1)

    assert _puback_mids(engine) == [1, 2]


def test_unflushed_ack_intent_is_connection_scoped() -> None:
    engine = _engine()
    _feed(engine, _qos1_publish(1))
    _feed(engine, _qos1_publish(2))
    engine.take_effects()
    engine.inbound.ack(2)
    assert _puback_mids(engine) == []

    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    _feed(engine, _connack(session_present=True))
    engine.take_effects()

    engine.inbound.ack(1)
    assert _puback_mids(engine) == [1]
    engine.inbound.ack(2)
    assert _puback_mids(engine) == [2]


def test_sqlite_recovery_preserves_qos1_arrival_order(tmp_path: Path) -> None:
    path = tmp_path / "ordered-ack.db"
    store = SqliteInflightStore(path)
    engine = _engine(store)
    for mid in (7, 4, 8):
        _feed(engine, _qos1_publish(mid))
    engine.take_effects()
    engine.notify_transport_closed()
    engine.take_effects()
    store.close()

    reopened = SqliteInflightStore(path)
    resumed = ProtocolEngine(
        EngineConfig(
            client_id="bug247",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
            manual_ack=True,
        ),
        reopened,
    )
    resumed.begin_connect()
    _feed(resumed, _connack(session_present=True))
    resumed.take_effects()

    resumed.inbound.ack(8)
    resumed.inbound.ack(4)
    assert _puback_mids(resumed) == []
    resumed.inbound.ack(7)
    assert _puback_mids(resumed) == [7, 4, 8]
    reopened.close()
