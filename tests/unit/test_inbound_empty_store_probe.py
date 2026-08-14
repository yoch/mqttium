"""Automatic QoS 1 must not probe an empty inbound store on every PUBLISH."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, PubRelPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import EffectKind, ProtocolEngine
from tests.support import feed_engine


class CountingMemoryStore(MemoryInflightStore):
    """Count inbound metadata probes without changing store semantics."""

    def __init__(self) -> None:
        super().__init__()
        self.in_meta_calls = 0
        self.contains_in_calls = 0
        self.get_in_calls = 0

    def in_meta(self, mid: int):  # type: ignore[override]
        self.in_meta_calls += 1
        return super().in_meta(mid)

    def contains_in(self, mid: int) -> bool:
        self.contains_in_calls += 1
        return super().contains_in(mid)

    def get_in(self, mid: int):  # type: ignore[override]
        self.get_in_calls += 1
        return super().get_in(mid)


def _connack(
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    *,
    session_present: bool = False,
) -> bytes:
    body = bytes((1 if session_present else 0, 0))
    if protocol is MQTTProtocolVersion.MQTTv5:
        body += b"\x00"
    return encode_frame(PacketType.CONNACK, 0, body)


def _connect(
    store: object,
    *,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    manual_ack: bool = False,
    clean_start: bool = True,
    session_present: bool = False,
) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="empty-probe",
            protocol=protocol,
            manual_ack=manual_ack,
            clean_start=clean_start,
        ),
        store=store,  # type: ignore[arg-type]
    )
    engine.begin_connect()
    feed_engine(engine, _connack(protocol, session_present=session_present))
    engine.take_effects()
    return engine


def _publish(engine: ProtocolEngine, mid: int, qos: QoS, payload: bytes = b"x") -> None:
    feed_engine(
        engine,
        PublishPacket(
            topic="factory/line-4/temp",
            payload=payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=mid,
        ).encode(engine.config.protocol),
    )


@contextmanager
def traced(store: SqliteInflightStore) -> Iterator[list[str]]:
    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)
    try:
        yield statements
    finally:
        store._conn.set_trace_callback(None)


def _inbound_selects(statements: list[str]) -> list[str]:
    return [
        line
        for line in statements
        if "inbound" in line.lower() and line.lstrip().upper().startswith("SELECT")
    ]


def test_auto_qos1_does_not_probe_an_empty_memory_store() -> None:
    store = CountingMemoryStore()
    engine = _connect(store)
    for mid in range(1, 11):
        _publish(engine, mid, QoS.AT_LEAST_ONCE)
        engine.take_effects()

    assert store.in_meta_calls == 0
    assert store.contains_in_calls == 0
    assert store.get_in_calls == 0
    assert engine.inbound._stored_inbound == 0
    assert list(store.in_items()) == []


def test_auto_qos1_does_not_select_from_empty_sqlite_inbound(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "empty-inbound.db")
    engine = _connect(store)
    with traced(store) as statements:
        for mid in range(1, 21):
            _publish(engine, mid, QoS.AT_LEAST_ONCE)
            engine.take_effects()
    assert _inbound_selects(statements) == []
    assert engine.inbound._stored_inbound == 0
    store.close()


def test_qos2_occupancy_restores_probes_then_releases_them() -> None:
    store = CountingMemoryStore()
    engine = _connect(store)

    _publish(engine, 7, QoS.EXACTLY_ONCE, b"held")
    engine.take_effects()
    assert engine.inbound._stored_inbound == 1
    qos2_probes = store.contains_in_calls + store.in_meta_calls
    assert qos2_probes == 0, "the first QoS 2 PUBLISH finds an empty store"

    store.in_meta_calls = 0
    store.contains_in_calls = 0
    _publish(engine, 8, QoS.AT_LEAST_ONCE, b"while-qos2-held")
    engine.take_effects()
    assert store.in_meta_calls == 1
    assert store.contains_in_calls == 0

    feed_engine(engine, PubRelPacket(mid=7).encode(engine.config.protocol))
    engine.take_effects()
    assert engine.inbound._stored_inbound == 0

    store.in_meta_calls = 0
    _publish(engine, 9, QoS.AT_LEAST_ONCE, b"after-qos2")
    engine.take_effects()
    assert store.in_meta_calls == 0


def test_recovered_qos2_occupancy_returns_to_empty_probe_fast_path() -> None:
    store = CountingMemoryStore()
    first = _connect(store)
    _publish(first, 17, QoS.EXACTLY_ONCE, b"persisted")
    first.take_effects()
    assert first.inbound._stored_inbound == 1

    recovered = _connect(store, clean_start=False, session_present=True)
    assert recovered.inbound._stored_inbound == 1

    store.in_meta_calls = 0
    store.contains_in_calls = 0
    store.get_in_calls = 0
    feed_engine(recovered, PubRelPacket(mid=17).encode(recovered.config.protocol))
    recovered.take_effects()
    assert recovered.inbound._stored_inbound == 0

    store.in_meta_calls = 0
    store.contains_in_calls = 0
    store.get_in_calls = 0
    _publish(recovered, 18, QoS.AT_LEAST_ONCE, b"after-recovery")
    recovered.take_effects()
    assert store.in_meta_calls == 0
    assert store.contains_in_calls == 0
    assert store.get_in_calls == 0


def test_qos1_still_collides_with_a_live_qos2_record() -> None:
    store = CountingMemoryStore()
    engine = _connect(store)
    _publish(engine, 5, QoS.EXACTLY_ONCE, b"qos2-holder")
    engine.take_effects()

    _publish(engine, 5, QoS.AT_LEAST_ONCE, b"qos1-collision")
    effects = engine.take_effects()
    held = store.get_in(5)

    assert held is not None and held.payload == b"qos2-holder"
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert store.in_meta_calls >= 1


def test_manual_ack_qos1_still_persists_and_probes_duplicates() -> None:
    store = CountingMemoryStore()
    engine = _connect(store, manual_ack=True)
    _publish(engine, 3, QoS.AT_LEAST_ONCE, b"manual")
    engine.take_effects()
    assert engine.inbound._stored_inbound == 1
    assert store.get_in(3) is not None

    store.in_meta_calls = 0
    _publish(engine, 3, QoS.AT_LEAST_ONCE, b"manual")
    effects = engine.take_effects()
    messages = [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]
    assert len(messages) == 1 and messages[0].dup
    assert store.in_meta_calls == 1

    engine.ack(3)
    assert engine.inbound._stored_inbound == 0
