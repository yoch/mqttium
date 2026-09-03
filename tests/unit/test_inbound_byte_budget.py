"""Byte-bounded persistence for inbound QoS handshakes."""

from __future__ import annotations

from pathlib import Path

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PubRelPacket, PublishPacket, encode_disconnect
from mqttium.persistence import MemoryInflightStore, SqliteInflightStore
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage, Properties


class FailingInboundStore(MemoryInflightStore):
    def put_in(self, msg: InboundMessage) -> None:
        del msg
        raise OSError("persistence unavailable")


def feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def write_bytes(data: object) -> bytes:
    if isinstance(data, bytes):
        return data
    assert isinstance(data, tuple)
    return data[0] + data[1]


def sends(engine: ProtocolEngine) -> list[bytes]:
    return [
        write_bytes(effect.data)
        for effect in engine.take_effects()
        if effect.kind is EffectKind.SEND
    ]


def connected_engine(
    *,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    manual_ack: bool = False,
    byte_limit: int | None = 64 * 1024 * 1024,
    store: MemoryInflightStore | SqliteInflightStore | None = None,
) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=protocol,
            manual_ack=manual_ack,
            max_pending_inbound_bytes=byte_limit,
        ),
        store=store,
    )
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_qos2_duplicate_does_not_reserve_bytes_twice(
    kind: str,
    tmp_path: Path,
) -> None:
    store = MemoryInflightStore() if kind == "memory" else SqliteInflightStore(tmp_path / "qos2.db")
    engine = connected_engine(store=store)
    packet = PublishPacket(
        topic="bounded/qos2",
        payload=b"payload",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=7,
    )
    expected = len("bounded/qos2") + len(b"payload")

    feed(engine, packet.encode())
    engine.take_effects()
    duplicate = PublishPacket(
        topic=packet.topic,
        payload=packet.payload,
        qos=packet.qos,
        retain=packet.retain,
        dup=True,
        mid=packet.mid,
    )
    feed(engine, duplicate.encode())

    assert engine.inbound.stats().pending_bytes == expected
    assert store.in_count() == 1
    stored = store.get_in(7)
    assert stored is not None
    assert stored.logical_size == expected
    if isinstance(store, SqliteInflightStore):
        store.close()


def test_qos2_quota_exceeded_disconnects_with_mqtt5_reason() -> None:
    protocol = MQTTProtocolVersion.MQTTv5
    first = PublishPacket(
        topic="quota",
        payload=b"1234",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=1,
    )
    properties = Properties()
    properties.set("content_type", "text/plain")
    second = PublishPacket(
        topic="quota",
        payload=b"x",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=2,
        properties=properties,
    )
    probe = connected_engine(protocol=protocol)
    limit = probe.inbound.logical_size(first.topic, first.payload, first.properties)
    engine = connected_engine(protocol=protocol, byte_limit=limit)

    feed(engine, first.encode(protocol))
    engine.take_effects()
    feed(engine, second.encode(protocol))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert engine.inbound.stats().pending_bytes == limit
    assert any(
        effect.kind is EffectKind.SEND
        and write_bytes(effect.data) == encode_disconnect(0x97, protocol)
        for effect in effects
    )
    assert any(
        effect.kind is EffectKind.PROTOCOL_ERROR
        and "Pending inbound byte limit reached" in str(effect.data)
        for effect in effects
    )


def test_mqtt5_property_bytes_are_included_in_the_reservation() -> None:
    protocol = MQTTProtocolVersion.MQTTv5
    properties = Properties()
    properties.set("content_type", "application/json")
    packet = PublishPacket(
        topic="sized/v5",
        payload=b"{}",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=3,
        properties=properties,
    )
    expected = len(packet.topic) + len(packet.payload) + len(encode_properties(properties, PUBLISH))
    engine = connected_engine(protocol=protocol, byte_limit=expected)

    feed(engine, packet.encode(protocol))

    assert engine.inbound.stats().pending_bytes == expected
    stored = engine.store.get_in(3)
    assert stored is not None
    assert stored.logical_size == expected


def test_v311_quota_exceeded_closes_without_disconnect_packet() -> None:
    engine = connected_engine(byte_limit=0)
    packet = PublishPacket(
        topic="quota",
        payload=b"x",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=1,
    )

    feed(engine, packet.encode())
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    assert not any(effect.kind is EffectKind.SEND for effect in effects)
    assert engine.inbound.stats().pending_bytes == 0


def test_manual_qos1_ack_releases_reserved_bytes() -> None:
    engine = connected_engine(manual_ack=True)
    packet = PublishPacket(
        topic="manual/qos1",
        payload=b"body",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=11,
    )
    expected = len(packet.topic) + len(packet.payload)

    feed(engine, packet.encode())
    assert engine.inbound.stats().pending_bytes == expected

    engine.ack(11)

    snapshot = engine.inbound.stats()
    assert snapshot.pending_bytes == 0
    assert snapshot.pending_high_water_bytes == expected


def test_qos2_pubrel_releases_reserved_bytes() -> None:
    engine = connected_engine()
    packet = PublishPacket(
        topic="auto/qos2",
        payload=b"body",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=12,
    )

    feed(engine, packet.encode())
    assert engine.inbound.stats().pending_bytes > 0
    engine.take_effects()

    feed(engine, PubRelPacket(mid=12).encode())

    assert engine.inbound.stats().pending_bytes == 0
    assert engine.store.get_in(12) is None


@pytest.mark.parametrize("qos,manual_ack", [(QoS.AT_LEAST_ONCE, True), (QoS.EXACTLY_ONCE, False)])
def test_persistence_failure_rolls_back_reservation_before_delivery(
    qos: QoS,
    manual_ack: bool,
) -> None:
    store = FailingInboundStore()
    engine = connected_engine(manual_ack=manual_ack, store=store)
    feed_target = PublishPacket(
        topic="failure/rollback",
        payload=b"body",
        qos=qos,
        retain=False,
        dup=False,
        mid=31,
    )

    with pytest.raises(OSError, match="persistence unavailable"):
        feed(engine, feed_target.encode())
    effects = engine.take_effects()

    snapshot = engine.inbound.stats()
    assert snapshot.inflight == 0
    assert snapshot.pending_bytes == 0
    assert not any(effect.kind is EffectKind.MESSAGE for effect in effects)
    assert effects == []


def test_sqlite_reopen_restores_and_releases_bytes_without_payload_read(tmp_path: Path) -> None:
    path = tmp_path / "restart.db"
    first_store = SqliteInflightStore(path)
    first = connected_engine(store=first_store)
    packet = PublishPacket(
        topic="restart/qos2",
        payload=b"x" * 4096,
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=19,
    )
    feed(first, packet.encode())
    expected = first.inbound.stats().pending_bytes
    first_store.close()

    reopened = SqliteInflightStore(path)
    trace: list[str] = []
    reopened._conn.set_trace_callback(trace.append)
    second = connected_engine(byte_limit=1, store=reopened)
    reopened._conn.set_trace_callback(None)

    assert second.inbound.stats().pending_bytes == expected
    selects = [line for line in trace if line.lstrip().startswith("SELECT")]
    assert selects
    assert all("payload" not in line for line in selects)

    feed(second, PubRelPacket(mid=19).encode())
    assert second.inbound.stats().pending_bytes == 0
    assert reopened.get_in(19) is None
    reopened.close()


def test_none_disables_limit_and_configuration_is_forwarded() -> None:
    async_client = AsyncClient(max_pending_inbound_bytes=None)
    compat_client = Client(
        CallbackAPIVersion.VERSION2,
        max_pending_inbound_bytes=None,
    )

    assert async_client._engine.config.max_pending_inbound_bytes is None
    assert compat_client._async._engine.config.max_pending_inbound_bytes is None


@pytest.mark.parametrize("value", [-1])
def test_negative_limit_is_rejected(value: int) -> None:
    with pytest.raises(ValueError, match="max_pending_inbound_bytes"):
        AsyncClient(max_pending_inbound_bytes=value)
