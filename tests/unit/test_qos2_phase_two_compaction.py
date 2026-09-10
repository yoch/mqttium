"""QoS 2 phase-two records retain settlement state, not PUBLISH data."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.packets import PubCompPacket, PubRecPacket, PubRelPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import OutboundMessage, Properties
from tests.support import feed_engine, write_item_bytes


def connack(*, session_present: bool = False) -> bytes:
    return encode_frame(PacketType.CONNACK, 0, bytes((int(session_present), 0, 0)))


def connected_engine(store: object) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="qos2-compaction",
            clean_start=False,
            protocol=MQTTProtocolVersion.MQTTv5,
        ),
        store=store,  # type: ignore[arg-type]
    )
    engine.begin_connect()
    feed_engine(engine, connack())
    engine.take_effects()
    return engine


def sent_packets(engine: ProtocolEngine) -> list[bytes]:
    return [
        write_item_bytes(effect.data)
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND
    ]


def make_properties() -> Properties:
    properties = Properties()
    properties = Properties({**properties.values, "message_expiry_interval": 60})
    properties = Properties(
        {
            **properties.values,
            "user_property": (*properties.get("user_property", ()), ("trace", "phase-two")),
        }
    )
    return properties


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_pubrec_removes_all_publish_application_data(kind: str, tmp_path: Path) -> None:
    store: MemoryInflightStore | SqliteInflightStore
    store = (
        MemoryInflightStore()
        if kind == "memory"
        else SqliteInflightStore(tmp_path / "phase-two.db")
    )
    try:
        engine = connected_engine(store)
        handle = engine.queue_publish(
            "large/qos2/topic",
            b"x" * (2 * 1024 * 1024),
            qos=QoS.EXACTLY_ONCE,
            properties=make_properties(),
        )
        mid = handle.mid or 0
        logical_size = engine.pending_outbound_bytes
        engine.take_effects()

        feed_engine(engine, PubRecPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv5))

        assert sent_packets(engine) == [PubRelPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv5)]
        stored = store.get_out(mid)
        assert stored is not None
        assert stored.state is OutboundQoSState.WAIT_PUBCOMP
        assert stored.topic == ""
        assert stored.payload == b""
        assert stored.properties is None
        assert stored.logical_size == logical_size
        assert engine.pending_outbound_bytes == logical_size
        assert engine.pending_outbound_messages == 1
        assert engine.flow.inflight == 1

        feed_engine(engine, PubCompPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv5))
        completions = engine.take_effects()
        assert any(
            effect.kind is EffectKind.PUBLISH_COMPLETE and effect.data == mid
            for effect in completions
        )
        assert store.get_out(mid) is None
        assert engine.pending_outbound_bytes == 0
        assert engine.pending_outbound_messages == 0
        assert engine.flow.inflight == 0
    finally:
        if isinstance(store, SqliteInflightStore):
            store.close()


def test_compacted_sqlite_record_restarts_with_pubrel_only(tmp_path: Path) -> None:
    path = tmp_path / "restart.db"
    store = SqliteInflightStore(path)
    engine = connected_engine(store)
    handle = engine.queue_publish(
        "restart/qos2",
        b"r" * (1024 * 1024),
        qos=QoS.EXACTLY_ONCE,
        properties=make_properties(),
    )
    mid = handle.mid or 0
    logical_size = engine.pending_outbound_bytes
    engine.take_effects()
    feed_engine(engine, PubRecPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv5))
    engine.take_effects()
    store.close()

    reopened = SqliteInflightStore(path)
    recovered = ProtocolEngine(
        EngineConfig(
            client_id="qos2-compaction",
            clean_start=False,
            protocol=MQTTProtocolVersion.MQTTv5,
        ),
        store=reopened,
    )
    assert recovered.pending_outbound_bytes == logical_size
    compacted = reopened.get_out(mid)
    assert compacted is not None
    assert compacted.topic == ""
    assert compacted.payload == b""
    assert compacted.properties is None

    recovered.begin_connect()
    feed_engine(recovered, connack(session_present=True))
    assert sent_packets(recovered) == [PubRelPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv5)]
    feed_engine(recovered, PubCompPacket(mid=mid).encode(MQTTProtocolVersion.MQTTv5))
    recovered.take_effects()
    assert reopened.get_out(mid) is None
    assert recovered.pending_outbound_bytes == 0
    reopened.close()


def test_direct_transition_keeps_logical_size_while_compacting() -> None:
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=9,
            topic="direct/topic",
            payload=b"payload",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBREC,
            properties=make_properties(),
            logical_size=999,
        )
    )

    meta = store.transition_out(
        9,
        OutboundQoSState.WAIT_PUBREC,
        OutboundQoSState.WAIT_PUBCOMP,
        compact=True,
    )

    assert meta is not None and meta.logical_size == 999
    record = store.get_out(9)
    assert record is not None
    assert (record.topic, record.payload, record.properties) == ("", b"", None)
