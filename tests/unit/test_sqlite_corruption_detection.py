"""Wrong SQLite storage classes must not become MQTT replay data."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, OutboundQoSState, PacketType, QoS
from mqttium.persistence import SqliteInflightStore
from mqttium.protocol.effects import EffectKind, PublishFailure
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage, OutboundMessage, Properties


def _out(mid: int = 1) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic="original",
        payload=b"original",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBACK,
        properties=Properties({"content_type": "application/octet-stream"}),
    )


def _in(mid: int = 1) -> InboundMessage:
    return InboundMessage(
        mid=mid,
        topic="original",
        payload=b"original",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_PUBREL,
    )


def _corrupt(
    store: SqliteInflightStore,
    column: str,
    value: object,
) -> None:
    # The column is a fixed test parameter, never application input.
    store._conn.execute(  # noqa: SLF001 - intentional durable corruption
        f"UPDATE outbound SET {column}=? WHERE mid=1",
        (value,),
    )
    store._conn.commit()  # noqa: SLF001 - intentional durable corruption


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("topic", b"wrong-topic"),
        ("payload", 5),
        ("qos", b"1"),
        ("retain", 2),
        ("properties", b"{}"),
        ("properties", '{"content_type":{"__mqttium_bytes__":3}}'),
        ("properties", '{"content_type":{"__mqttium_bytes__":"!!!"}}'),
        ("properties", '{"content_type":{"__mqttium_tuple__":3}}'),
        ("logical_size", -1),
    ],
)
def test_public_hydration_rejects_wrong_storage_values(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    store = SqliteInflightStore(tmp_path / "corrupt.sqlite3")
    try:
        store.put_out(_out())
        _corrupt(store, column, value)

        with pytest.raises(ValueError, match=column):
            store.get_out(1)
    finally:
        store.close()


def test_public_hydration_rejects_empty_properties_json(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "corrupt.sqlite3")
    try:
        store.put_out(_out())
        _corrupt(store, "properties", "")

        with pytest.raises(json.JSONDecodeError):
            store.get_out(1)
    finally:
        store.close()


def test_corrupted_topic_is_rejected_while_replay_index_is_built(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "replay.sqlite3")
    try:
        store.put_out(_out())
        _corrupt(store, "topic", b"wrong-topic")

        with pytest.raises(ValueError, match="topic"):
            ProtocolEngine(
                EngineConfig(client_id="corrupt-replay", clean_start=False),
                store=store,
            )
    finally:
        store.close()


def test_corrupted_payload_is_rejected_before_publish_replay(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "replay.sqlite3")
    try:
        store.put_out(_out())
        _corrupt(store, "payload", 5)

        engine = ProtocolEngine(
            EngineConfig(client_id="corrupt-replay", clean_start=False),
            store=store,
        )
        engine.begin_connect()
        engine.take_effects()
        engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))
        effects = engine.take_effects()

        failures = [effect.data for effect in effects if effect.kind is EffectKind.PUBLISH_FAILED]
        assert len(failures) == 1
        failure = failures[0]
        assert isinstance(failure, PublishFailure)
        assert isinstance(failure.reason, ValueError)
        assert "payload" in str(failure.reason)
        assert not [effect for effect in effects if effect.kind is EffectKind.SEND]
    finally:
        store.close()


def test_outbound_metadata_rejects_coercible_state(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "metadata.sqlite3")
    try:
        store.put_out(_out())
        _corrupt(store, "state", b"1")

        with pytest.raises(ValueError, match="state"):
            store.out_meta(1)
    finally:
        store.close()


def test_outbound_transition_rejects_coercible_sequence(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "transition.sqlite3")
    try:
        store.put_out(_out())
        _corrupt(store, "seq", b"1")

        with pytest.raises(ValueError, match="seq"):
            store.complete_out(1, OutboundQoSState.WAIT_PUBACK)
    finally:
        store.close()


def test_inbound_metadata_rejects_non_boolean_ack(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "metadata.sqlite3")
    try:
        store.put_in(_in())
        store._conn.execute(  # noqa: SLF001 - intentional durable corruption
            "UPDATE inbound SET user_acked=2 WHERE mid=1"
        )
        store._conn.commit()  # noqa: SLF001 - intentional durable corruption

        with pytest.raises(ValueError, match="user_acked"):
            store.in_meta(1)
    finally:
        store.close()


def test_reopen_rejects_coercible_sequence(tmp_path: Path) -> None:
    path = tmp_path / "sequence.sqlite3"
    store = SqliteInflightStore(path)
    store.put_out(_out())
    _corrupt(store, "seq", b"1")
    store.close()

    with pytest.raises(ValueError, match="max_seq"):
        SqliteInflightStore(path)


@pytest.mark.parametrize("seq", [-1, 1.5])
@pytest.mark.parametrize("table", ["outbound", "inbound"])
def test_paged_replay_rejects_corrupted_nonmax_sequence(
    tmp_path: Path,
    table: str,
    seq: object,
) -> None:
    store = SqliteInflightStore(tmp_path / "sequence.sqlite3")
    try:
        if table == "outbound":
            store.put_out(_out(1))
            store.put_out(_out(2))
            pages = store.out_pages()
        else:
            store.put_in(_in(1))
            store.put_in(_in(2))
            pages = store.in_pages()
        # The fixed table names are test parameters, never application input.
        store._conn.execute(  # noqa: SLF001 - intentional durable corruption
            f"UPDATE {table} SET seq=? WHERE mid=1",
            (seq,),
        )
        store._conn.commit()  # noqa: SLF001 - intentional durable corruption

        with pytest.raises(ValueError, match="seq"):
            next(pages)
    finally:
        store.close()
