"""Restart redelivery is paged, bounded and backpressured."""

from __future__ import annotations

import asyncio
import tracemalloc
from collections.abc import Iterator
from pathlib import Path

from mqttium.api.async_client import AsyncClient
from mqttium.enums import InboundQoSState, PacketType, QoS
from mqttium.codec.buffer import RawPacket
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import (
    REPLAY_BATCH_MESSAGES,
    REPLAY_PAGE_SIZE,
)
from mqttium.types import InboundMessage


def inbound(mid: int, payload: bytes = b"body") -> InboundMessage:
    topic = f"recover/{mid}"
    return InboundMessage(
        mid=mid,
        topic=topic,
        payload=payload,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_PUBREL,
        delivered=False,
        logical_size=len(topic) + len(payload),
    )


def fill(store: object, count: int, payload: bytes = b"body") -> None:
    with store.batch():  # type: ignore[attr-defined]
        for mid in range(1, count + 1):
            store.put_in(inbound(mid, payload))  # type: ignore[attr-defined]


def resume(engine: ProtocolEngine) -> None:
    """Negotiate a resumed session, leaving the produced effects in place."""
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))


def resume_effects(engine: ProtocolEngine) -> list:
    resume(engine)
    return engine.take_effects()


def message_mids(effects: list) -> list[int]:
    return [effect.data.mid for effect in effects if effect.kind is EffectKind.MESSAGE]


def test_replay_emits_a_bounded_first_batch_and_asks_for_more() -> None:
    store = MemoryInflightStore()
    fill(store, 500)
    engine = ProtocolEngine(EngineConfig(client_id="replay", clean_start=False), store=store)

    effects = resume_effects(engine)

    assert len(message_mids(effects)) == REPLAY_BATCH_MESSAGES
    assert effects[-1].kind is EffectKind.CONTINUE_INBOUND_REPLAY
    # The receive window is restored in full before the first redelivery, not
    # progressively as batches are emitted.
    assert engine._inbound_inflight == 500
    assert engine.inbound.replay_pending is True


def test_continuations_replay_every_record_exactly_once() -> None:
    store = MemoryInflightStore()
    fill(store, 500)
    engine = ProtocolEngine(EngineConfig(client_id="replay-all", clean_start=False), store=store)

    delivered = message_mids(resume_effects(engine))
    batches = 1
    while engine.inbound.replay_pending:
        engine.continue_inbound_replay()
        delivered.extend(message_mids(engine.take_effects()))
        batches += 1

    assert delivered == list(range(1, 501))
    assert batches >= 500 // REPLAY_BATCH_MESSAGES
    assert engine.inbound.replay_pending is False


def test_replay_reuses_one_bounded_page_across_effect_batches(tmp_path: Path) -> None:
    class CountingStore(SqliteInflightStore):
        pages_fetched = 0

        def in_replay_pages(  # type: ignore[override]
            self,
            max_messages: int = 64,
            max_bytes: int = 1 << 20,
        ) -> Iterator[tuple[InboundMessage, ...]]:
            for page in super().in_replay_pages(max_messages, max_bytes):
                CountingStore.pages_fetched += 1
                yield page

    store = CountingStore(tmp_path / "paged.db")
    fill(store, REPLAY_PAGE_SIZE * 3)
    engine = ProtocolEngine(EngineConfig(client_id="paged", clean_start=False), store=store)

    resume_effects(engine)
    assert CountingStore.pages_fetched == 1

    engine.continue_inbound_replay()
    engine.take_effects()
    assert CountingStore.pages_fetched == 1  # still inside the first bounded page
    store.close()


def test_recovered_identifiers_are_loaded_without_reading_payloads(tmp_path: Path) -> None:
    path = tmp_path / "index.db"
    store = SqliteInflightStore(path)
    fill(store, 10, payload=b"x" * 4096)
    store.close()

    reopened = SqliteInflightStore(path)
    trace: list[str] = []
    reopened._conn.set_trace_callback(trace.append)
    engine = ProtocolEngine(EngineConfig(client_id="index", clean_start=False), store=reopened)
    reopened._conn.set_trace_callback(None)

    assert engine.inbound._recovered_mids == set(range(1, 11))
    selects = [line for line in trace if line.lstrip().startswith("SELECT")]
    assert selects
    # `length(payload)` is the payload-free outbound summary read; a bare
    # payload column or a `SELECT *` would mean a BLOB came back.
    assert all("SELECT *" not in line for line in selects)
    assert all("payload" not in line or "length(payload)" in line for line in selects)
    reopened.close()


def test_a_dropped_connection_abandons_the_replay_cursor() -> None:
    store = MemoryInflightStore()
    fill(store, 500)
    engine = ProtocolEngine(EngineConfig(client_id="dropped", clean_start=False), store=store)
    resume_effects(engine)
    assert engine.inbound.replay_pending is True

    engine.notify_transport_closed()
    engine.begin_connect()  # the next connection attempt resets connection state

    assert engine.inbound.replay_pending is False


def test_a_store_without_paging_keeps_the_eager_replay() -> None:
    class PlainStore(MemoryInflightStore):
        """Drops the opt-in extensions the streaming replay relies on."""

        in_pages = None  # type: ignore[assignment]
        in_index_pages = None  # type: ignore[assignment]

    store = PlainStore()
    fill(store, 300)
    engine = ProtocolEngine(EngineConfig(client_id="eager", clean_start=False), store=store)

    effects = resume_effects(engine)

    assert message_mids(effects) == list(range(1, 301))
    assert not any(e.kind is EffectKind.CONTINUE_INBOUND_REPLAY for e in effects)
    assert engine.inbound.replay_pending is False


async def test_client_replays_every_message_through_the_effect_pump() -> None:
    store = MemoryInflightStore()
    fill(store, 400)
    client = AsyncClient(
        client_id="client-replay",
        clean_start=False,
        store=store,
        message_delivery="iterator",
        max_pending_messages=1024,
    )

    async with client._engine_lock:
        resume(client._engine)
        client._collect_effects_locked()
    await client._drain_effects()

    received = []
    while not client._messages.empty():
        item = client._messages.get_nowait()
        received.append(item.mid if hasattr(item, "mid") else item[0].mid)
    assert received == list(range(1, 401))
    assert client._engine.inbound.replay_pending is False


async def test_replay_peak_memory_stays_proportional_to_one_batch(tmp_path: Path) -> None:
    """10,000 x 1 KiB durable inbound records must not all be materialised."""
    payload = b"p" * 1024
    total = 4_000
    store = SqliteInflightStore(tmp_path / "peak.db")
    fill(store, total, payload)
    store.close()

    reopened = SqliteInflightStore(tmp_path / "peak.db")
    client = AsyncClient(
        client_id="peak",
        clean_start=False,
        store=reopened,
        message_delivery="iterator",
        max_pending_messages=64,
    )
    consumed = 0

    async def consume() -> None:
        nonlocal consumed
        stream = client.messages()
        try:
            while consumed < total:
                await anext(stream)
                consumed += 1
        finally:
            await stream.aclose()

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)

    tracemalloc.start()
    try:
        async with client._engine_lock:
            resume(client._engine)
            client._collect_effects_locked()
        await client._drain_effects()
        await asyncio.wait_for(consumer, timeout=30.0)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        reopened.close()

    assert consumed == total
    # The whole session is ~4 MiB of payload; a bounded replay must stay an
    # order of magnitude below that.
    assert peak < 1_500_000, peak
