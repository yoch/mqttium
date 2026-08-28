"""Regression coverage for broker Maximum Packet Size on inbound ACKs."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import CONNACK, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MandatoryResponseTooLargeError, PacketTooLargeError
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
    properties.set("maximum_packet_size", maximum_packet_size)
    body = bytearray((int(session_present), 0))
    body.extend(encode_properties(properties, CONNACK))
    return encode_frame(PacketType.CONNACK, 0, body)


def _connected_engine(
    maximum_packet_size: int,
    *,
    manual_ack: bool = False,
    store: InflightStore | None = None,
) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="ack-size",
            protocol=MQTTProtocolVersion.MQTTv5,
            manual_ack=manual_ack,
        ),
        store=store,
    )
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
def test_tiny_broker_packet_limits_are_accepted(maximum_packet_size: int) -> None:
    engine = _connected_engine(maximum_packet_size)

    assert engine.state is ConnectionState.CONNECTED
    assert engine.negotiated.maximum_packet_size == maximum_packet_size


@pytest.mark.parametrize("maximum_packet_size", [1, 2, 3])
def test_tiny_limit_still_accepts_qos0_receive_only_use(
    maximum_packet_size: int,
) -> None:
    engine = _connected_engine(maximum_packet_size)

    _feed(engine, _publish(QoS.AT_MOST_ONCE))
    effects = engine.take_effects()

    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert engine.state is ConnectionState.CONNECTED


def test_automatic_puback_failure_is_local_and_leaves_no_state() -> None:
    engine = _connected_engine(maximum_packet_size=3)

    with pytest.raises(
        MandatoryResponseTooLargeError,
        match=r"Mandatory PUBACK size 4 exceeds broker maximum_packet_size 3",
    ):
        _feed(engine, _publish(QoS.AT_LEAST_ONCE))

    assert engine.take_effects() == []
    assert engine.store.get_in(7) is None
    assert engine.inbound._inflight == 0
    assert engine.inbound._pending_bytes == 0
    assert engine.inbound._pending_auto_qos1_mids == set()


@pytest.fixture(params=["memory", "sqlite"])
def durable_store(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Iterator[MemoryInflightStore | SqliteInflightStore]:
    if request.param == "memory":
        store: MemoryInflightStore | SqliteInflightStore = MemoryInflightStore()
    else:
        store = SqliteInflightStore(tmp_path / "inflight.db")
    try:
        yield store
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()


def test_automatic_pubrec_failure_does_not_commit_qos2_state(
    durable_store: MemoryInflightStore | SqliteInflightStore,
) -> None:
    engine = _connected_engine(maximum_packet_size=3, store=durable_store)

    with pytest.raises(
        MandatoryResponseTooLargeError,
        match=r"Mandatory PUBREC size 4 exceeds broker maximum_packet_size 3",
    ):
        _feed(engine, _publish(QoS.EXACTLY_ONCE))

    assert engine.take_effects() == []
    assert durable_store.get_in(7) is None
    assert engine.inbound._inflight == 0
    assert engine.inbound._pending_bytes == 0
    assert engine.inbound._session_state_qos2 == 0


def test_automatic_pubcomp_failure_preserves_resumed_qos2_state(
    durable_store: MemoryInflightStore | SqliteInflightStore,
) -> None:
    first = ProtocolEngine(
        EngineConfig(
            client_id="ack-size",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
        ),
        store=durable_store,
    )
    first.begin_connect()
    _feed(first, _connack(4))
    first.take_effects()
    _feed(first, _publish(QoS.EXACTLY_ONCE))
    first.take_effects()
    record = durable_store.get_in(7)
    assert record is not None
    assert record.state.name == "WAIT_PUBREL"
    first.notify_transport_closed()
    first.take_effects()

    resumed = ProtocolEngine(
        EngineConfig(
            client_id="ack-size",
            protocol=MQTTProtocolVersion.MQTTv5,
            clean_start=False,
        ),
        store=durable_store,
    )
    resumed.begin_connect()
    _feed(resumed, _connack(3, session_present=True))
    resumed.take_effects()

    with pytest.raises(
        MandatoryResponseTooLargeError,
        match=r"Mandatory PUBCOMP size 4 exceeds broker maximum_packet_size 3",
    ):
        _feed(resumed, encode_pubrel_success(7))

    assert resumed.take_effects() == []
    preserved = durable_store.get_in(7)
    assert preserved is not None
    assert preserved.state.name == "WAIT_PUBREL"
    assert resumed.inbound._inflight == 1
    assert resumed.inbound._session_state_qos2 == 1


def test_automatic_puback_at_exact_broker_packet_limit_is_emitted() -> None:
    engine = _connected_engine(maximum_packet_size=4)

    _feed(engine, _publish(QoS.AT_LEAST_ONCE))
    effects = engine.take_effects()

    sent = [effect.data for effect in effects if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE]
    assert sent == [b"\x40\x02\x00\x07"]
    assert any(effect.kind is EffectKind.MESSAGE for effect in effects)
    assert not any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_qos2_at_exact_broker_packet_limit_completes_exchange() -> None:
    engine = _connected_engine(maximum_packet_size=4)

    _feed(engine, _publish(QoS.EXACTLY_ONCE))
    first = engine.take_effects()
    assert [
        effect.data for effect in first if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    ] == [b"\x50\x02\x00\x07"]
    assert any(effect.kind is EffectKind.MESSAGE for effect in first)
    assert engine.store.get_in(7) is not None

    _feed(engine, encode_pubrel_success(7))
    second = engine.take_effects()
    assert [
        effect.data for effect in second if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    ] == [b"\x70\x02\x00\x07"]
    assert engine.store.get_in(7) is None
    assert engine.inbound._inflight == 0


def test_manual_puback_size_failure_preserves_durable_record() -> None:
    engine = _connected_engine(maximum_packet_size=3, manual_ack=True)

    _feed(engine, _publish(QoS.AT_LEAST_ONCE))
    effects = engine.take_effects()
    assert any(effect.kind is EffectKind.MESSAGE for effect in effects)
    assert engine.store.get_in(7) is not None
    assert engine.inbound._inflight == 1

    with pytest.raises(PacketTooLargeError, match="maximum_packet_size 3"):
        engine.ack(7)

    assert engine.store.get_in(7) is not None
    assert engine.inbound._inflight == 1
    assert not any(effect.kind is EffectKind.SEND for effect in engine.take_effects())


def test_manual_puback_at_exact_limit_completes_record() -> None:
    engine = _connected_engine(maximum_packet_size=4, manual_ack=True)

    _feed(engine, _publish(QoS.AT_LEAST_ONCE))
    engine.take_effects()
    engine.ack(7)
    effects = engine.take_effects()

    assert [
        effect.data for effect in effects if effect.kind is EffectKind.SEND_PROTOCOL_RESPONSE
    ] == [b"\x40\x02\x00\x07"]
    assert engine.store.get_in(7) is None
    assert engine.inbound._inflight == 0


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

    def feed(self, data: bytes) -> None:
        self._rx.put_nowait(data)


async def test_runtime_autoack_size_failure_is_local_terminal_and_not_retried() -> None:
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

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _TinyLimitTransport:
        nonlocal connect_calls
        connect_calls += 1
        return transport

    client._transport_factory = factory
    await client.connect("fake", timeout=0.2)

    reader = client._reader_task
    assert reader is not None
    transport.feed(_publish(QoS.AT_LEAST_ONCE))
    await asyncio.wait_for(reader, timeout=1.0)

    assert connect_calls == 1
    assert type(client._disconnect_exc) is MandatoryResponseTooLargeError
    assert client._reconnect_task is None
    assert transport.is_closing()
    assert len(transport.writes) == 1  # CONNECT only: no peer-blaming DISCONNECT.
