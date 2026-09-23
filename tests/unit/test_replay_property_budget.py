"""Property-heavy replay respects the same logical-byte bound in both stores."""

import sqlite3

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol import inbound as inbound_module
from mqttium.protocol._sizing import publish_logical_size
from mqttium.protocol.inbound import REPLAY_BATCH_BYTES
from mqttium.types import InboundMessage, Properties
from tests.support import stored_record


class _LoosePageStore(MemoryInflightStore):
    """Exercise the independent engine guard, not the store's page guard."""

    def in_replay_pages(self, max_messages=64, max_bytes=1 << 20):
        yield tuple(self.get_in(mid) for mid in (1, 2, 3))


@pytest.mark.parametrize("kind", ["memory", "sqlite", "loose"])
@pytest.mark.parametrize("property_count", [12, 20])
def test_property_bytes_bound_pages_and_engine_replay(tmp_path, kind, property_count):
    store = (
        SqliteInflightStore(tmp_path / "replay.db")
        if kind == "sqlite"
        else _LoosePageStore()
        if kind == "loose"
        else MemoryInflightStore()
    )
    properties = Properties(
        {"user_property": tuple((f"key{i}", "x" * 60000) for i in range(property_count))}
    )
    try:
        for mid in (1, 2, 3):
            store.put_in(
                stored_record(
                    InboundMessage(
                        mid=mid,
                        topic="t",
                        payload=b"x",
                        qos=QoS.EXACTLY_ONCE,
                        retain=False,
                        state=InboundQoSState.WAIT_PUBREL,
                        properties=properties,
                    )
                )
            )
        if kind != "loose":
            pages = list(store.in_replay_pages(64, REPLAY_BATCH_BYTES))
            assert [len(page) for page in pages] == [1, 1, 1]
            assert (pages[0][0].logical_size > REPLAY_BATCH_BYTES) == (property_count == 20)
        engine = ProtocolEngine(
            EngineConfig(
                client_id="property-replay",
                protocol=MQTTProtocolVersion.MQTTv5,
                clean_start=False,
            ),
            store=store,
        )
        engine.begin_connect()
        engine.take_effects()
        engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00\x00"))
        seen = []
        for _ in range(4):
            messages = [
                effect.data for effect in engine.take_effects() if effect.kind is EffectKind.MESSAGE
            ]
            assert len(messages) <= 1
            seen.extend(message.mid for message in messages)
            if not engine.inbound.replay_pending:
                break
            engine.continue_inbound_replay()
        assert seen == [1, 2, 3]
        assert not engine.inbound.replay_pending
    finally:
        if isinstance(store, SqliteInflightStore):
            store.close()


def _heavy(mid, topic="t/é"):
    # Properties dominate the logical size; the topic exercises UTF-8 sizing.
    return stored_record(
        InboundMessage(
            mid=mid,
            topic=topic,
            payload=b"p",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBREL,
            properties=Properties({"user_property": (("k", "v" * 300),)}),
        )
    )


def _filled(kind, tmp_path, count=4):
    store = (
        SqliteInflightStore(tmp_path / "pages.db") if kind == "sqlite" else MemoryInflightStore()
    )
    with store.batch():
        for mid in range(1, count + 1):
            store.put_in(_heavy(mid))
    return store


_SIZE = _heavy(1).logical_size


def test_property_bytes_dominate_the_persisted_logical_size():
    record = _heavy(1)
    assert record.logical_size == publish_logical_size(
        True, record.topic, len(record.payload), record.properties
    )
    assert record.logical_size - len(record.payload) - len(record.topic.encode()) >= 300


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize(
    ("max_messages", "max_bytes", "expected"),
    [
        (64, 2 * _SIZE - 1, [[1], [2], [3], [4]]),  # below two records
        (64, 2 * _SIZE, [[1, 2], [3, 4]]),  # exactly two records
        (64, 2 * _SIZE + 1, [[1, 2], [3, 4]]),  # above two, below three
        (64, _SIZE - 1, [[1], [2], [3], [4]]),  # each record alone exceeds the budget
        (3, 1 << 20, [[1, 2, 3], [4]]),  # count bound
        (1, 1 << 20, [[1], [2], [3], [4]]),
    ],
)
def test_store_pages_share_count_and_property_byte_boundaries(
    tmp_path, kind, max_messages, max_bytes, expected
):
    store = _filled(kind, tmp_path)
    try:
        pages = [[m.mid for m in page] for page in store.in_replay_pages(max_messages, max_bytes)]
        assert pages == expected
    finally:
        if kind == "sqlite":
            store.close()


class _WholeSessionPageStore(MemoryInflightStore):
    """Deliberately ignore both page limits to isolate the engine's own bound."""

    def in_replay_pages(self, max_messages=64, max_bytes=1 << 20):
        yield tuple(self._in.values())


def _replay_batches(store, limit):
    engine = ProtocolEngine(
        EngineConfig(
            client_id="engine-bound", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False
        ),
        store=store,
    )
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00\x00"))
    batches = []
    for _ in range(limit):
        batch = [e.data.mid for e in engine.take_effects() if e.kind is EffectKind.MESSAGE]
        batches.append(batch)
        if not engine.inbound.replay_pending:
            return batches
        engine.continue_inbound_replay()
    raise AssertionError(f"replay made no progress within {limit} batches: {batches}")


@pytest.mark.parametrize(
    ("batch_messages", "batch_bytes", "expected"),
    [
        (64, 2 * _SIZE - 1, [[1], [2], [3], [4]]),
        (64, 2 * _SIZE, [[1, 2], [3, 4]]),
        (64, 2 * _SIZE + 1, [[1, 2], [3, 4]]),
        (64, _SIZE - 1, [[1], [2], [3], [4]]),  # oversized records still progress
        (3, 1 << 20, [[1, 2, 3], [4]]),
    ],
)
def test_engine_bounds_an_oversized_store_page(monkeypatch, batch_messages, batch_bytes, expected):
    monkeypatch.setattr(inbound_module, "REPLAY_BATCH_MESSAGES", batch_messages)
    monkeypatch.setattr(inbound_module, "REPLAY_BATCH_BYTES", batch_bytes)
    store = _WholeSessionPageStore()
    for mid in range(1, 5):
        store.put_in(_heavy(mid))
    assert _replay_batches(store, limit=8) == expected


def test_sqlite_sizes_pages_from_logical_size_metadata_only(tmp_path):
    store = _filled("sqlite", tmp_path, count=3)
    events = []

    def authorize(action, table, column, *_):
        if action == sqlite3.SQLITE_READ and table == "inbound":
            events.append(("read", column))
        return sqlite3.SQLITE_OK

    try:
        store._conn.set_authorizer(authorize)
        store._conn.set_trace_callback(lambda sql: events.append(("sql", sql)))
        pages = [[m.mid for m in page] for page in store.in_replay_pages(64, _SIZE)]
    finally:
        store._conn.set_authorizer(None)
        store._conn.set_trace_callback(None)
        store.close()

    assert pages == [[1], [2], [3]]
    statements = [i for i, (kind, _) in enumerate(events) if kind == "sql"]
    index_sql = events[statements[0]][1]
    assert "ORDER BY seq" in index_sql
    # Columns authorized before the index statement runs are the ones it reads.
    assert {column for _, column in events[: statements[0]]} == {"mid", "logical_size", "seq"}
    hydrations = [events[i][1] for i in statements[1:]]
    assert [sql.rsplit("IN ", 1)[1] for sql in hydrations] == ["(1)", "(2)", "(3)"]
