"""Wrong SQLite storage classes must not become MQTT replay data."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.enums import OutboundQoSState, PacketType, QoS
from mqttium.persistence import SqliteInflightStore
from mqttium.protocol.effects import EffectKind, PublishFailure
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import OutboundMessage, Properties


def _out() -> OutboundMessage:
    return OutboundMessage(
        mid=1,
        topic="original",
        payload=b"original",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBACK,
        properties=Properties({"content_type": "application/octet-stream"}),
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
