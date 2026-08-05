from pathlib import Path


def replace(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"missing patch anchor in {path}: {old[:80]!r}")
    file.write_text(text.replace(old, new, 1))


replace(
    "src/mqttium/persistence/memory.py",
    """        if compact:
            # Only PUBREL is retransmissible from here on.
            msg.encoded_publish = None
""",
    """        if compact:
            # The PUBLISH phase is over. PUBREL needs only the packet id and
            # protocol version, so no application data should survive PUBREC.
            msg.topic = ""
            msg.payload = b""
            msg.properties = None
            msg.encoded_publish = None
""",
)

replace(
    "src/mqttium/persistence/sqlite.py",
    """SQLITE_SCHEMA_VERSION = 2
\"\"\"Durable schema revision stored in ``PRAGMA user_version``.

* 1 — the original schema, which carried no version marker at all.
* 2 — adds ``outbound.logical_size``, drops the never-read ``extra`` column,
  and moves ``payload`` last so metadata reads never traverse BLOB overflow
  pages.
\"\"\"
""",
    """SQLITE_SCHEMA_VERSION = 3
\"\"\"Durable schema revision stored in ``PRAGMA user_version``.

* 1 — the original schema, which carried no version marker at all.
* 2 — adds ``outbound.logical_size``, drops the never-read ``extra`` column,
  and moves ``payload`` last so metadata reads never traverse BLOB overflow
  pages.
* 3 — compacts outbound QoS 2 records already in ``WAIT_PUBCOMP`` by removing
  topic, payload and PUBLISH properties while preserving settlement metadata.
\"\"\"
""",
)
replace(
    "src/mqttium/persistence/sqlite.py",
    """            if version < 2 and legacy:
                self._migrate_to_v2()
            conn.execute(f"PRAGMA user_version={SQLITE_SCHEMA_VERSION}")
""",
    """            if version < 2 and legacy:
                self._migrate_to_v2()
            if version < 3 and legacy:
                self._migrate_to_v3()
            conn.execute(f"PRAGMA user_version={SQLITE_SCHEMA_VERSION}")
""",
)
replace(
    "src/mqttium/persistence/sqlite.py",
    """    def _rebuild_table(self, table: str, *statements: str) -> None:
""",
    """    def _migrate_to_v3(self) -> None:
        \"\"\"Discard PUBLISH application data after the QoS 2 PUBREC phase.

        A ``WAIT_PUBCOMP`` record only retransmits PUBREL. Its original logical
        size remains durable so the admission budget is released exactly once
        when PUBCOMP eventually settles the record.
        \"\"\"
        self._conn.execute(
            \"\"\"
            UPDATE outbound
            SET topic='', properties=NULL, payload=X''
            WHERE state=?
            \"\"\",
            (int(OutboundQoSState.WAIT_PUBCOMP),),
        )

    def _rebuild_table(self, table: str, *statements: str) -> None:
""",
)
replace(
    "src/mqttium/persistence/sqlite.py",
    """            cursor = self._conn.execute(
                "UPDATE outbound SET state=? WHERE mid=? AND state=? AND seq=?",
                (int(new_state), mid, int(expected_state), int(row["seq"])),
            )
""",
    """            if compact:
                cursor = self._conn.execute(
                    \"\"\"
                    UPDATE outbound
                    SET state=?, topic='', properties=NULL, payload=X''
                    WHERE mid=? AND state=? AND seq=?
                    \"\"\",
                    (int(new_state), mid, int(expected_state), int(row["seq"])),
                )
            else:
                cursor = self._conn.execute(
                    "UPDATE outbound SET state=? WHERE mid=? AND state=? AND seq=?",
                    (int(new_state), mid, int(expected_state), int(row["seq"])),
                )
""",
)
replace(
    "src/mqttium/persistence/sqlite.py",
    """            # `compact` needs no work here: the encoded PUBLISH frame is a
            # memory-only field this store has never persisted.
            return OutboundRecordMeta(
""",
    """            return OutboundRecordMeta(
""",
)

replace(
    "src/mqttium/protocol/outbound.py",
    """        msg.state = OutboundQoSState.WAIT_PUBCOMP
        msg.encoded_publish = None
        if msg.encoded_pubrel is None:
""",
    """        msg.state = OutboundQoSState.WAIT_PUBCOMP
        # Third-party stores without the transition extension still receive
        # the same phase-two compaction as the built-in stores.
        msg.topic = ""
        msg.payload = b""
        msg.properties = None
        msg.encoded_publish = None
        if msg.encoded_pubrel is None:
""",
)

replace(
    "docs/ROADMAP.md",
    """- [ ] Full QoS 2 phase-two compaction (release topic, payload and properties,
      not only the encoded frame).""",
    """- [x] Full QoS 2 phase-two compaction (release topic, payload and properties,
      not only the encoded frame).""",
)
replace(
    "CHANGELOG.md",
    "### Changed\n",
    """### Changed

- Outbound QoS 2 records are fully compacted after PUBREC: topic, payload and
  PUBLISH properties are removed atomically while the original logical size is
  retained until PUBCOMP releases admission accounting. SQLite schema 3 also
  compacts pre-existing WAIT_PUBCOMP rows during migration.
""",
)

Path("tests/unit/test_qos2_phase_two_compaction.py").write_text(r'''"""QoS 2 phase-two records retain settlement state, not PUBLISH data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.packets import PubCompPacket, PubRecPacket, PubRelPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SQLITE_SCHEMA_VERSION, SqliteInflightStore
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
    properties.set("message_expiry_interval", 60)
    properties.add_user_property("trace", "phase-two")
    return properties


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_pubrec_removes_all_publish_application_data(kind: str, tmp_path: Path) -> None:
    store: MemoryInflightStore | SqliteInflightStore
    store = MemoryInflightStore() if kind == "memory" else SqliteInflightStore(tmp_path / "phase-two.db")
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


def test_schema_v2_migration_compacts_existing_wait_pubcomp_row(tmp_path: Path) -> None:
    path = tmp_path / "schema-v2.db"
    conn = sqlite3.connect(path)
    conn.execute(f"CREATE TABLE outbound ({SqliteInflightStore.OUTBOUND_COLUMNS})")
    conn.execute(
        """
        INSERT INTO outbound(
            mid, seq, qos, retain, state, dup, logical_size, topic, properties, payload
        ) VALUES (7, 1, 2, 0, ?, 0, 4242, 'legacy/topic', ?, ?)
        """,
        (
            int(OutboundQoSState.WAIT_PUBCOMP),
            '{"user_property":[["legacy","yes"]]}',
            sqlite3.Binary(b"legacy payload"),
        ),
    )
    conn.execute("PRAGMA user_version=2")
    conn.commit()
    conn.close()

    store = SqliteInflightStore(path)
    assert int(store._conn.execute("PRAGMA user_version").fetchone()[0]) == SQLITE_SCHEMA_VERSION
    record = store.get_out(7)
    assert record is not None
    assert record.state is OutboundQoSState.WAIT_PUBCOMP
    assert record.topic == ""
    assert record.payload == b""
    assert record.properties is None
    assert record.logical_size == 4242
    store.close()


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
''')
