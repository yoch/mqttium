"""Logical memory admission for outbound QoS publications."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import FlowControlError, MQTTError
from mqttium.packets import PubAckPacket, PubCompPacket, PubRecPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import OutboundMessage, Properties


def _make_store(kind: str, tmp_path: Path) -> MemoryInflightStore | SqliteInflightStore:
    """SqliteInflightStore.batch() rolls back, MemoryInflightStore does not."""
    if kind == "sqlite":
        return SqliteInflightStore(tmp_path / "admission.db")
    return MemoryInflightStore()


def _feed_connack_ok(engine: ProtocolEngine) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    engine.take_effects()


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def test_message_limit_rejects_before_packet_id_or_store_mutation() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=1,
            max_pending_outbound_bytes=None,
        )
    )

    first = engine.queue_publish("admission/first", b"one", qos=1)
    with pytest.raises(FlowControlError, match="message limit"):
        engine.queue_publish("admission/second", b"two", qos=1)

    assert first.mid == 1
    assert engine.pending_outbound_messages == 1
    assert engine.pending_outbound_bytes == len(b"one") + len("admission/first")
    assert len(engine.packet_ids) == 1
    assert sum(1 for _ in engine.store.out_items()) == 1


def test_zero_capacity_rejects_without_allocating_state() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=0,
            max_pending_outbound_bytes=0,
        )
    )

    with pytest.raises(FlowControlError):
        engine.queue_publish("admission/zero", b"payload", qos=1)

    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0
    assert len(engine.packet_ids) == 0
    assert list(engine.store.out_items()) == []
    assert engine.take_effects() == []


def test_none_disables_both_admission_limits() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
    )

    for index in range(20):
        engine.queue_publish(f"admission/{index}", b"payload", qos=1)

    assert engine.pending_outbound_messages == 20
    assert len(engine.packet_ids) == 20


def test_byte_limit_counts_payload_topic_and_non_empty_encoded_properties() -> None:
    properties = Properties()
    properties.add_user_property("source", "benchmark")
    topic = "admission/properties"
    payload = b"payload"
    logical_size = len(payload) + len(topic.encode()) + len(encode_properties(properties, PUBLISH))
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv5,
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=logical_size,
        )
    )

    engine.queue_publish(topic, payload, qos=1, properties=properties)
    with pytest.raises(FlowControlError, match="byte limit"):
        engine.queue_publish("x", b"", qos=1)

    assert engine.pending_outbound_bytes == logical_size
    assert len(engine.packet_ids) == 1


def test_puback_releases_message_and_byte_reservations() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=1,
            max_pending_outbound_bytes=128,
        )
    )
    engine.begin_connect()
    _feed_connack_ok(engine)

    handle = engine.queue_publish("admission/release", b"payload", qos=1)
    assert handle.mid is not None
    assert engine.pending_outbound_messages == 1

    _feed(engine, PubAckPacket(mid=handle.mid).encode())

    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0
    replacement = engine.queue_publish("admission/next", b"payload", qos=1)
    assert replacement.mid is not None


def test_qos2_drops_contiguous_frame_on_launch_but_keeps_budget_to_pubcomp() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=1,
            max_pending_outbound_bytes=128,
        )
    )
    engine.begin_connect()
    _feed_connack_ok(engine)

    handle = engine.queue_publish("admission/qos2", b"payload", qos=2)
    assert handle.mid is not None
    stored = engine.store.get_out(handle.mid)
    assert stored is not None
    # Admission tracks logical topic-plus-payload size, not the optional cached
    # transport representation. A small contiguous frame is already gone.
    assert stored.encoded_publish is None
    retained_bytes = engine.pending_outbound_bytes

    _feed(engine, PubRecPacket(mid=handle.mid).encode())

    stored = engine.store.get_out(handle.mid)
    assert stored is not None
    assert stored.state is OutboundQoSState.WAIT_PUBCOMP
    assert stored.encoded_publish is None
    assert engine.pending_outbound_messages == 1
    assert engine.pending_outbound_bytes == retained_bytes

    _feed(engine, PubCompPacket(mid=handle.mid).encode())
    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_publish_many_rollback_restores_admission_counters(store_kind: str, tmp_path: Path) -> None:
    store = _make_store(store_kind, tmp_path)
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=2,
            max_pending_outbound_bytes=None,
        ),
        store=store,
    )

    with pytest.raises(FlowControlError):
        engine.queue_publish_many(
            [
                ("admission/1", b"1", QoS.AT_LEAST_ONCE, False, None),
                ("admission/2", b"2", QoS.AT_LEAST_ONCE, False, None),
                ("admission/3", b"3", QoS.AT_LEAST_ONCE, False, None),
            ]
        )

    assert engine.pending_outbound_messages == 0
    assert engine.pending_outbound_bytes == 0
    assert len(engine.packet_ids) == 0
    assert list(engine.store.out_items()) == []


@pytest.mark.parametrize("store_kind", ["memory", "sqlite"])
def test_publish_many_rollback_after_validation_failure_frees_bytes(
    store_kind: str, tmp_path: Path
) -> None:
    """A transactional store rolls its batch back before the engine's except
    clause runs, so per-record sizes are gone; the byte budget must still clear.
    """
    store = _make_store(store_kind, tmp_path)
    engine = ProtocolEngine(
        EngineConfig(max_pending_outbound_bytes=4096),
        store=store,
    )

    for _ in range(3):
        with pytest.raises(MQTTError):
            engine.queue_publish_many(
                [
                    ("admission/valid", b"payload", QoS.AT_LEAST_ONCE, False, None),
                    ("admission/+/invalid", b"payload", QoS.AT_LEAST_ONCE, False, None),
                ]
            )
        assert engine.pending_outbound_messages == 0
        assert engine.pending_outbound_bytes == 0

    # The budget must still admit a full-size publish after the failed batches.
    handle = engine.queue_publish("admission/after", b"x" * 4000, qos=1)
    assert handle.mid is not None


def test_hydrated_records_are_counted_even_above_new_limits() -> None:
    store = MemoryInflightStore()
    for mid in range(1, 4):
        store.put_out(
            OutboundMessage(
                mid=mid,
                topic="admission/hydrated",
                payload=b"payload",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=OutboundQoSState.QUEUED,
            )
        )
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=1,
            max_pending_outbound_bytes=None,
        ),
        store=store,
    )

    assert engine.pending_outbound_messages == 3
    with pytest.raises(FlowControlError):
        engine.queue_publish("admission/new", b"payload", qos=1)
    assert len(engine.packet_ids) == 3


def test_sustained_qos1_load_returns_admission_counters_to_zero() -> None:
    engine = ProtocolEngine(
        EngineConfig(
            max_pending_outbound_messages=100,
            max_pending_outbound_bytes=1024 * 1024,
        )
    )
    engine.begin_connect()
    _feed_connack_ok(engine)

    for cycle in range(20):
        handles = [
            engine.queue_publish(
                f"admission/sustained/{cycle}/{index}",
                b"payload" * 8,
                qos=1,
            )
            for index in range(50)
        ]
        engine.take_effects()
        for handle in handles:
            assert handle.mid is not None
            _feed(engine, PubAckPacket(mid=handle.mid).encode())
        engine.take_effects()
        assert engine.pending_outbound_messages == 0
        assert engine.pending_outbound_bytes == 0
        assert engine.flow.inflight == 0
        assert len(engine.packet_ids) == 0
        assert list(engine.store.out_items()) == []

    assert engine.outbound.pending_high_water_messages == 50
    assert engine.outbound.pending_high_water_bytes > 0


def test_prepare_qos0_does_not_reconstruct_qos_enum(monkeypatch) -> None:
    engine = ProtocolEngine(EngineConfig(client_id="qos-enum"))
    engine.state = ConnectionState.CONNECTED
    calls = 0
    original_new = QoS.__new__

    def counted_new(cls, value):
        nonlocal calls
        calls += 1
        return original_new(cls, value)

    monkeypatch.setattr(QoS, "__new__", counted_new)
    engine.outbound.prepare_qos0("bench/qos", b"x")
    engine.outbound.queue_publish("bench/qos1", b"x", qos=QoS.AT_LEAST_ONCE)
    assert calls == 0


@pytest.mark.parametrize("qos", [3, -1, 99])
def test_invalid_qos_still_raises_value_error_at_admission(qos: int) -> None:
    engine = ProtocolEngine(EngineConfig(client_id="qos-enum"))
    with pytest.raises(ValueError):
        engine.outbound.queue_publish("bench/invalid", b"x", qos=qos)
