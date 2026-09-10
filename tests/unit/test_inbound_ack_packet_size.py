"""MQTT 5 broker Maximum Packet Size and mandatory inbound ACKs."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MandatoryResponseTooLargeError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.packets._ack import encode_pubrel_success
from mqttium.persistence.memory import InflightStore, MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(maximum_packet_size: int, *, session_present: bool = False) -> bytes:
    properties = Properties()
    properties = Properties({**properties.values, "maximum_packet_size": maximum_packet_size})
    body = bytearray((int(session_present), 0))
    body.extend(encode_properties(properties, CONNACK))
    return encode_frame(PacketType.CONNACK, 0, body)


def _new_engine(*, manual_ack: bool = False, store: InflightStore | None = None) -> ProtocolEngine:
    return ProtocolEngine(
        EngineConfig(
            client_id="ack-size",
            protocol=MQTTProtocolVersion.MQTTv5,
            manual_ack=manual_ack,
            clean_start=False,
        ),
        store=store,
    )


def _connected_engine(maximum_packet_size: int = 4, *, manual_ack: bool = False) -> ProtocolEngine:
    engine = _new_engine(manual_ack=manual_ack)
    engine.begin_connect()
    _feed(engine, _connack(maximum_packet_size))
    engine.take_effects()
    return engine


def _publish(qos: QoS, mid: int = 7) -> bytes:
    return PublishPacket(
        topic="inbound/topic",
        payload=b"payload",
        qos=qos,
        retain=False,
        dup=False,
        mid=None if qos is QoS.AT_MOST_ONCE else mid,
    ).encode(MQTTProtocolVersion.MQTTv5)


@pytest.mark.parametrize("maximum_packet_size", [1, 2, 3])
def test_tiny_broker_packet_limit_fails_connection_locally(
    maximum_packet_size: int,
) -> None:
    engine = _new_engine()
    engine.begin_connect()
    with pytest.raises(
        MandatoryResponseTooLargeError,
        match=rf"maximum_packet_size {maximum_packet_size}.*4-byte minimum",
    ):
        _feed(engine, _connack(maximum_packet_size))
    assert engine.state is ConnectionState.DISCONNECTED
    assert engine.negotiated.maximum_packet_size == maximum_packet_size
    assert engine.take_effects() == []


def test_automatic_puback_at_exact_broker_packet_limit_is_emitted() -> None:
    engine = _connected_engine()
    _feed(engine, _publish(QoS.AT_LEAST_ONCE))
    effects = engine.take_effects()
    assert [e.data for e in effects if e.kind is EffectKind.SEND_ACK] == [b"\x40\x02\x00\x07"]
    assert any(e.kind is EffectKind.MESSAGE for e in effects)


def test_qos2_at_exact_broker_packet_limit_completes_exchange() -> None:
    engine = _connected_engine()
    _feed(engine, _publish(QoS.EXACTLY_ONCE))
    first = engine.take_effects()
    assert [e.data for e in first if e.kind is EffectKind.SEND_ACK] == [b"\x50\x02\x00\x07"]
    assert engine.store.get_in(7) is not None
    _feed(engine, encode_pubrel_success(7))
    second = engine.take_effects()
    assert [e.data for e in second if e.kind is EffectKind.SEND_ACK] == [b"\x70\x02\x00\x07"]
    assert engine.store.get_in(7) is None


def test_manual_puback_at_exact_limit_completes_record() -> None:
    engine = _connected_engine(manual_ack=True)
    _feed(engine, _publish(QoS.AT_LEAST_ONCE))
    engine.take_effects()
    engine.ack(7)
    effects = engine.take_effects()
    assert [e.data for e in effects if e.kind is EffectKind.SEND_ACK] == [b"\x40\x02\x00\x07"]
    assert engine.store.get_in(7) is None
    assert engine.inbound._inflight == 0


@pytest.fixture(params=["memory", "sqlite"])
def durable_store(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[MemoryInflightStore | SqliteInflightStore]:
    store: MemoryInflightStore | SqliteInflightStore
    if request.param == "memory":
        store = MemoryInflightStore()
    else:
        store = SqliteInflightStore(tmp_path / "inflight.db")
    try:
        yield store
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()


def test_tiny_limit_rejects_resumed_session_before_mutating_durable_qos2(
    durable_store: MemoryInflightStore | SqliteInflightStore,
) -> None:
    first = _new_engine(store=durable_store)
    first.begin_connect()
    _feed(first, _connack(4))
    first.take_effects()
    _feed(first, _publish(QoS.EXACTLY_ONCE))
    first.take_effects()
    first.notify_transport_closed()
    first.take_effects()

    resumed = _new_engine(store=durable_store)
    resumed.begin_connect()
    with pytest.raises(MandatoryResponseTooLargeError):
        _feed(resumed, _connack(3, session_present=True))
    record = durable_store.get_in(7)
    assert record is not None
    assert record.state.name == "WAIT_PUBREL"
    assert resumed.state is ConnectionState.DISCONNECTED
    assert resumed.take_effects() == []


class _TinyLimitTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closed = False
        self._sent_connack = False
        self.writes: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self.writes.append(bytes(data))
        if not self._sent_connack:
            self._sent_connack = True
            self._rx.put_nowait(_connack(3))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closed


async def test_runtime_tiny_limit_fails_connect_without_reconnect_or_disconnect() -> None:
    reconnect = ReconnectPolicy(
        enabled=True,
        initial_delay=0.0,
        max_delay=0.0,
        stable_after=0.0,
        connect_timeout=0.1,
    )
    client = AsyncClient(
        client_id="tiny-limit-runtime",
        protocol=MQTTProtocolVersion.MQTTv5,
        reconnect=reconnect,
        keepalive=0,
    )
    transport = _TinyLimitTransport()
    connect_calls = 0

    async def factory(host: str, port: int, *, ssl: object = None) -> _TinyLimitTransport:
        nonlocal connect_calls
        connect_calls += 1
        return transport

    client._transport_factory = factory
    with pytest.raises(MandatoryResponseTooLargeError):
        await client.connect("fake", timeout=0.2)
    assert connect_calls == 1
    assert type(client._disconnect_exc) is MandatoryResponseTooLargeError
    assert client._reconnect_task is None
    assert transport.is_closing()
    assert len(transport.writes) == 1
