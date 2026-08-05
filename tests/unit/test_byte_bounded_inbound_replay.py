"""Inbound restart replay bounds payload hydration before effect emission."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, PacketType, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import REPLAY_BATCH_BYTES
from mqttium.types import InboundMessage, InboundRecordMeta


def inbound(mid: int, payload: bytes) -> InboundMessage:
    return InboundMessage(
        mid=mid,
        topic=f"recover/{mid}",
        payload=payload,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_PUBREL,
        delivered=False,
    )


def fill(store: object, payloads: list[bytes]) -> None:
    with store.batch():  # type: ignore[attr-defined]
        for mid, payload in enumerate(payloads, 1):
            store.put_in(inbound(mid, payload))  # type: ignore[attr-defined]


def replay_size(page: tuple[InboundMessage, ...]) -> int:
    return sum(len(message.payload) + len(message.topic.encode("utf-8")) for message in page)


def resume_effects(engine: ProtocolEngine) -> list:
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))
    return engine.take_effects()


def message_effects(effects: list) -> list:
    return [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_replay_pages_are_bounded_before_payload_hydration(
    kind: str,
    tmp_path: Path,
) -> None:
    payload = b"x" * 700_000
    store = (
        MemoryInflightStore() if kind == "memory" else SqliteInflightStore(tmp_path / "bounded.db")
    )
    fill(store, [payload, payload, payload])

    pages = list(store.in_replay_pages(64, REPLAY_BATCH_BYTES))  # type: ignore[attr-defined]

    assert [len(page) for page in pages] == [1, 1, 1]
    assert all(replay_size(page) <= REPLAY_BATCH_BYTES for page in pages)
    if isinstance(store, SqliteInflightStore):
        store.close()


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_one_oversized_record_is_yielded_alone(
    kind: str,
    tmp_path: Path,
) -> None:
    store = (
        MemoryInflightStore()
        if kind == "memory"
        else SqliteInflightStore(tmp_path / "oversized.db")
    )
    fill(store, [b"x" * (REPLAY_BATCH_BYTES + 1), b"small"])

    pages = list(store.in_replay_pages(64, REPLAY_BATCH_BYTES))  # type: ignore[attr-defined]

    assert [len(page) for page in pages] == [1, 1]
    assert replay_size(pages[0]) > REPLAY_BATCH_BYTES
    assert replay_size(pages[1]) <= REPLAY_BATCH_BYTES
    if isinstance(store, SqliteInflightStore):
        store.close()


def test_engine_never_emits_two_700k_messages_in_one_replay_batch() -> None:
    payload = b"x" * 700_000
    store = MemoryInflightStore()
    fill(store, [payload, payload, payload])
    engine = ProtocolEngine(
        EngineConfig(client_id="byte-bounded", clean_start=False),
        store=store,
    )

    first = resume_effects(engine)
    first_messages = message_effects(first)

    assert len(first_messages) == 1
    assert len(first_messages[0].payload) + len(first_messages[0].topic.encode("utf-8")) <= (
        REPLAY_BATCH_BYTES
    )
    assert first[-1].kind is EffectKind.CONTINUE_INBOUND_REPLAY

    engine.continue_inbound_replay()
    second_messages = message_effects(engine.take_effects())
    assert len(second_messages) == 1


def test_direct_count_avoids_a_second_inbound_index_scan() -> None:
    class CountingStore(MemoryInflightStore):
        index_calls = 0
        count_calls = 0

        def in_index_pages(
            self,
            page_size: int = 256,
        ) -> Iterator[tuple[InboundRecordMeta, ...]]:
            CountingStore.index_calls += 1
            yield from super().in_index_pages(page_size)

        def in_count(self) -> int:
            CountingStore.count_calls += 1
            return super().in_count()

    store = CountingStore()
    fill(store, [b"body"] * 10)
    engine = ProtocolEngine(
        EngineConfig(client_id="direct-count", clean_start=False),
        store=store,
    )

    assert CountingStore.index_calls == 1
    resume_effects(engine)
    assert CountingStore.index_calls == 1
    assert CountingStore.count_calls == 1


def test_sqlite_sizes_pages_without_selecting_payloads(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "trace.db")
    fill(store, [b"x" * 700_000, b"y" * 700_000])
    trace: list[str] = []
    store._conn.set_trace_callback(trace.append)

    first_page = next(store.in_replay_pages(64, REPLAY_BATCH_BYTES))

    store._conn.set_trace_callback(None)
    assert len(first_page) == 1
    index_selects = [line for line in trace if "FROM inbound ORDER BY seq" in line]
    assert len(index_selects) == 1
    assert "length(payload)" in index_selects[0]
    payload_selects = [
        line for line in trace if "properties, payload FROM inbound WHERE mid IN" in line
    ]
    assert len(payload_selects) == 1
    assert "IN (1)" in payload_selects[0]
    store.close()


@pytest.mark.parametrize(
    ("max_messages", "max_bytes", "message"),
    [(0, 1, "max_messages"), (1, 0, "max_bytes")],
)
def test_replay_page_limits_must_be_positive(
    max_messages: int,
    max_bytes: int,
    message: str,
) -> None:
    store = MemoryInflightStore()
    fill(store, [b"body"])

    with pytest.raises(ValueError, match=message):
        next(store.in_replay_pages(max_messages, max_bytes))
