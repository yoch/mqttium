"""Payload-free record transitions and the whole-object fallback path."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from pathlib import Path

import pytest

from mqttium.enums import InboundQoSState, MQTTProtocolVersion, OutboundQoSState, QoS
from mqttium.enums import PacketType
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_frame,
)
from mqttium.persistence.memory import MemoryInflightStore, TransitionInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage
from tests.support import feed_engine, write_item_bytes


class PlainInflightStore:
    """A store predating the transition extension: whole objects only.

    Third-party stores in this shape must keep working, so every session path
    that gained a metadata-only branch is exercised through this one too.
    """

    def __init__(self) -> None:
        self._out: dict[int, OutboundMessage] = {}
        self._in: dict[int, InboundMessage] = {}

    def batch(self) -> AbstractContextManager[None]:
        return nullcontext()

    def put_out(self, msg: OutboundMessage) -> None:
        self._out[msg.mid] = msg

    def get_out(self, mid: int) -> OutboundMessage | None:
        return self._out.get(mid)

    def delete_out(self, mid: int) -> bool:
        return self._out.pop(mid, None) is not None

    def update_out(self, msg: OutboundMessage) -> None:
        if msg.mid not in self._out:
            raise KeyError(msg.mid)
        self._out[msg.mid] = msg

    def out_items(self) -> Iterator[OutboundMessage]:
        return iter(list(self._out.values()))

    def clear_out(self) -> None:
        self._out.clear()

    def put_in(self, msg: InboundMessage) -> None:
        self._in[msg.mid] = msg

    def get_in(self, mid: int) -> InboundMessage | None:
        return self._in.get(mid)

    def pop_in(self, mid: int) -> InboundMessage | None:
        return self._in.pop(mid, None)

    def update_in(self, msg: InboundMessage) -> None:
        if msg.mid not in self._in:
            raise KeyError(msg.mid)
        self._in[msg.mid] = msg

    def in_items(self) -> Iterator[InboundMessage]:
        return iter(list(self._in.values()))

    def clear_in(self) -> None:
        self._in.clear()


def connack(
    session_present: bool = False,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
) -> bytes:
    body = bytes((0x01 if session_present else 0x00, 0x00))
    if protocol == MQTTProtocolVersion.MQTTv5:
        body += b"\x00"  # empty property table
    return encode_frame(PacketType.CONNACK, 0, body)


def connected_engine(
    store: object,
    *,
    manual_ack: bool = False,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    session_present: bool = False,
) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="transitions", manual_ack=manual_ack, protocol=protocol),
        store=store,  # type: ignore[arg-type]
    )
    engine.begin_connect()
    feed_engine(engine, connack(session_present, protocol))
    engine.take_effects()
    return engine


@contextmanager
def traced(store: SqliteInflightStore) -> Iterator[list[str]]:
    """Collect SQL for one engine call only — test assertions query too."""
    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)
    try:
        yield statements
    finally:
        store._conn.set_trace_callback(None)


def assert_no_payload_reads(statements: list[str]) -> None:
    selects = [line for line in statements if line.lstrip().startswith("SELECT")]
    assert selects, "expected the settlement path to read record metadata"
    assert all("payload" not in line and "SELECT *" not in line for line in selects)


def sends(engine: ProtocolEngine) -> list[bytes]:
    return [
        write_item_bytes(effect.data)
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND
    ]


def test_shipped_stores_satisfy_the_transition_protocol(tmp_path: Path) -> None:
    assert isinstance(MemoryInflightStore(), TransitionInflightStore)
    assert isinstance(SqliteInflightStore(tmp_path / "t.db"), TransitionInflightStore)
    assert not isinstance(PlainInflightStore(), TransitionInflightStore)


def test_puback_settles_a_sqlite_record_without_reading_the_payload(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "puback.db")
    engine = connected_engine(store)
    handle = engine.queue_publish("big/topic", b"x" * 4096, qos=1)
    engine.take_effects()
    assert engine.pending_outbound_bytes == 4096 + len("big/topic")

    with traced(store) as trace:
        feed_engine(engine, PubAckPacket(mid=handle.mid or 0).encode())

    assert any(
        effect.kind is EffectKind.PUBLISH_COMPLETE and effect.data == handle.mid
        for effect in engine.take_effects()
    )
    assert store.get_out(handle.mid or 0) is None
    assert engine.pending_outbound_bytes == 0
    assert engine.pending_outbound_messages == 0
    assert_no_payload_reads(trace)
    store.close()


def test_qos2_cycle_settles_on_metadata_only(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "qos2.db")
    engine = connected_engine(store)
    handle = engine.queue_publish("big/topic", b"y" * 4096, qos=2)
    mid = handle.mid or 0
    engine.take_effects()

    with traced(store) as trace:
        feed_engine(engine, PubRecPacket(mid=mid).encode())
        assert sends(engine) == [PubRelPacket(mid=mid).encode()]
        feed_engine(engine, PubCompPacket(mid=mid).encode())
        completions = engine.take_effects()

    assert any(
        effect.kind is EffectKind.PUBLISH_COMPLETE and effect.data == mid for effect in completions
    )
    assert store.get_out(mid) is None
    assert engine.pending_outbound_bytes == 0
    assert engine.flow.inflight == 0
    assert_no_payload_reads(trace)
    store.close()


def test_orphan_pubrec_still_answers_with_pubrel() -> None:
    engine = connected_engine(MemoryInflightStore(), protocol=MQTTProtocolVersion.MQTTv5)

    feed_engine(engine, PubRecPacket(mid=4242).encode(MQTTProtocolVersion.MQTTv5))

    assert sends(engine) == [
        PubRelPacket(mid=4242, reason_code=0x92).encode(MQTTProtocolVersion.MQTTv5)
    ]


def test_failed_pubrec_releases_budget_and_packet_id() -> None:
    engine = connected_engine(MemoryInflightStore(), protocol=MQTTProtocolVersion.MQTTv5)
    handle = engine.queue_publish("a/b", b"payload", qos=2)
    mid = handle.mid or 0
    engine.take_effects()

    feed_engine(
        engine,
        PubRecPacket(mid=mid, reason_code=0x80).encode(MQTTProtocolVersion.MQTTv5),
    )

    assert any(effect.kind is EffectKind.PUBLISH_FAILED for effect in engine.take_effects())
    assert engine.store.get_out(mid) is None
    assert engine.pending_outbound_bytes == 0
    assert not engine.packet_ids.in_use(mid)
    assert engine.flow.inflight == 0


def test_inbound_qos2_duplicate_check_reads_no_payload(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "inbound.db")
    engine = connected_engine(store)
    publish = PublishPacket(
        topic="in/topic",
        payload=b"z" * 4096,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=11,
    ).encode()
    feed_engine(engine, publish)
    engine.take_effects()

    with traced(store) as trace:
        # Both the per-message delivery mark and the duplicate check run on
        # metadata alone.
        engine.mark_inbound_delivered(11)
        feed_engine(engine, publish)  # duplicate delivery of the same PUBLISH
        assert sends(engine) == [PubRecPacket(mid=11).encode()]

    record = store.get_in(11)
    assert record is not None and record.delivered is True
    assert_no_payload_reads(trace)

    feed_engine(engine, PubRelPacket(mid=11).encode())
    assert sends(engine) == [PubCompPacket(mid=11).encode()]
    assert store.get_in(11) is None
    store.close()


def test_manual_ack_paths_use_conditional_transitions() -> None:
    store = MemoryInflightStore()
    engine = connected_engine(store, manual_ack=True)
    feed_engine(
        engine,
        PublishPacket(
            topic="in/topic",
            payload=b"body",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=5,
        ).encode(),
    )
    engine.take_effects()

    engine.ack(5)
    record = store.get_in(5)
    assert record is not None
    assert record.user_acked is True
    assert record.state is InboundQoSState.WAIT_PUBREL

    feed_engine(engine, PubRelPacket(mid=5).encode())
    assert sends(engine) == [PubCompPacket(mid=5).encode()]
    assert store.get_in(5) is None


def test_manual_ack_deferred_until_after_pubrel() -> None:
    store = MemoryInflightStore()
    engine = connected_engine(store, manual_ack=True)
    feed_engine(
        engine,
        PublishPacket(
            topic="in/topic",
            payload=b"body",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            dup=False,
            mid=6,
        ).encode(),
    )
    engine.take_effects()

    feed_engine(engine, PubRelPacket(mid=6).encode())
    assert sends(engine) == []
    record = store.get_in(6)
    assert record is not None
    assert record.state is InboundQoSState.WAIT_USER_ACK

    engine.ack(6)
    assert sends(engine) == [PubCompPacket(mid=6).encode()]
    assert store.get_in(6) is None


def test_hydration_backfills_a_legacy_logical_size(tmp_path: Path) -> None:
    path = tmp_path / "legacy-size.db"
    store = SqliteInflightStore(path)
    store.put_out(
        OutboundMessage(
            mid=3,
            topic="a/b",
            payload=b"p" * 100,
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    # Simulate a record written before the column existed.
    store._conn.execute("UPDATE outbound SET logical_size=0 WHERE mid=3")
    store._conn.commit()
    store.close()

    reopened = SqliteInflightStore(path)
    engine = ProtocolEngine(
        EngineConfig(client_id="transitions", clean_start=False), store=reopened
    )
    expected = 100 + len("a/b")
    assert engine.pending_outbound_bytes == expected

    persisted = reopened.get_out(3)
    assert persisted is not None
    assert persisted.logical_size == expected

    # And the metadata-only settlement now releases the whole reservation.
    engine.begin_connect()
    feed_engine(engine, connack(session_present=True))
    engine.take_effects()
    feed_engine(engine, PubAckPacket(mid=3).encode())
    engine.take_effects()
    assert engine.pending_outbound_bytes == 0
    assert engine.pending_outbound_messages == 0
    reopened.close()


@pytest.mark.parametrize("qos", [1, 2])
def test_a_store_without_the_extension_completes_the_full_cycle(qos: int) -> None:
    store = PlainInflightStore()
    engine = connected_engine(store)
    handle = engine.queue_publish("a/b", b"payload", qos=qos)
    mid = handle.mid or 0
    engine.take_effects()

    if qos == 2:
        feed_engine(engine, PubRecPacket(mid=mid).encode())
        assert sends(engine) == [PubRelPacket(mid=mid).encode()]
        feed_engine(engine, PubCompPacket(mid=mid).encode())
    else:
        feed_engine(engine, PubAckPacket(mid=mid).encode())

    assert any(
        effect.kind is EffectKind.PUBLISH_COMPLETE and effect.data == mid
        for effect in engine.take_effects()
    )
    assert store.get_out(mid) is None
    assert engine.pending_outbound_bytes == 0
    assert not engine.packet_ids.in_use(mid)


def test_a_store_without_the_extension_handles_inbound_qos2_and_manual_ack() -> None:
    store = PlainInflightStore()
    engine = connected_engine(store, manual_ack=True)
    publish = PublishPacket(
        topic="in/topic",
        payload=b"body",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=8,
    ).encode()
    feed_engine(engine, publish)
    engine.take_effects()

    engine.mark_inbound_delivered(8)
    record = store.get_in(8)
    assert record is not None and record.delivered is True

    feed_engine(engine, publish)  # duplicate
    assert sends(engine) == [PubRecPacket(mid=8).encode()]

    feed_engine(engine, PubRelPacket(mid=8).encode())
    assert sends(engine) == []
    engine.ack(8)
    assert sends(engine) == [PubCompPacket(mid=8).encode()]
    assert store.get_in(8) is None


def test_a_store_without_the_extension_completes_inbound_qos1_manual_ack() -> None:
    store = PlainInflightStore()
    engine = connected_engine(store, manual_ack=True)
    feed_engine(
        engine,
        PublishPacket(
            topic="in/topic",
            payload=b"body",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=9,
        ).encode(),
    )

    assert sends(engine) == []
    engine.ack(9)
    assert sends(engine) == [PubAckPacket(mid=9).encode()]
    assert store.get_in(9) is None
