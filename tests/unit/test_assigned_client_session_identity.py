"""Server-assigned MQTT 5 ClientIDs remain the identity of durable sessions."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import unpack_utf8
from mqttium.codec.properties import CONNECT, CONNACK, decode_properties, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import encode_frame
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.types import OutboundMessage, Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connect_identity(wire: bytes) -> tuple[str, bool]:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None and raw.packet_type is PacketType.CONNECT
    _, pos = unpack_utf8(raw.remaining, 0)
    pos += 1  # protocol level
    connect_flags = raw.remaining[pos]
    pos += 1
    pos += 2  # keepalive
    _, pos = decode_properties(raw.remaining, pos, CONNECT)
    client_id, _ = unpack_utf8(raw.remaining, pos)
    return client_id, bool(connect_flags & 0x02)


def _connack(*, assigned: str | None = None, session_present: bool = False) -> bytes:
    properties = Properties()
    if assigned is not None:
        properties.set("assigned_client_identifier", assigned)
    body = bytearray((int(session_present), 0x00))
    body.extend(encode_properties(properties, CONNACK))
    return encode_frame(PacketType.CONNACK, 0, body)


def _durable_config(*, client_id: str = "", clean_start: bool = True) -> EngineConfig:
    return EngineConfig(
        client_id=client_id,
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=clean_start,
        connect_properties=Properties({"session_expiry_interval": 3600}),
    )


class _AssignedSessionTransport:
    def __init__(self, *, first: bool, identities: list[tuple[str, bool]]) -> None:
        self._first = first
        self._identities = identities
        self._decoder = IncrementalDecoder()
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is not PacketType.CONNECT:
                continue
            connect = encode_frame(PacketType.CONNECT, raw.flags, raw.remaining)
            self._identities.append(_connect_identity(connect))
            self._rx.put_nowait(
                _connack(
                    assigned="assigned-A" if self._first else None,
                    session_present=not self._first,
                )
            )

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


def test_assigned_client_id_is_reused_for_durable_session_resume() -> None:
    engine = ProtocolEngine(_durable_config())

    first = engine.begin_connect()
    assert _connect_identity(first) == ("", True)
    _feed(engine, _connack(assigned="assigned-A"))
    engine.take_effects()
    assert engine._prefer_session_resume is True

    engine.notify_transport_closed()
    engine.take_effects()
    second = engine.begin_connect()
    assert _connect_identity(second) == ("assigned-A", False)

    # The broker does not repeat Assigned Client Identifier because this time
    # the Client supplied it. Session Present must still be accepted.
    _feed(engine, _connack(session_present=True))
    effects = engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    assert not any(str(effect.data).startswith("Successful CONNACK") for effect in effects)
    assert engine.effective_client_id == "assigned-A"


async def test_automatic_reconnect_uses_assigned_session_identity() -> None:
    identities: list[tuple[str, bool]] = []
    transports: list[_AssignedSessionTransport] = []
    client = AsyncClient(
        client_id="",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=True,
        connect_properties=Properties({"session_expiry_interval": 3600}),
        reconnect=ReconnectPolicy(
            enabled=True,
            initial_delay=0.0,
            max_delay=0.0,
            stable_after=0.0,
            connect_timeout=0.2,
        ),
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> _AssignedSessionTransport:
        del host, port, ssl
        transport = _AssignedSessionTransport(first=not transports, identities=identities)
        transports.append(transport)
        return transport

    client._transport_factory = factory
    await client.connect("fake", 1883, timeout=1.0)
    await transports[0].close()

    for _ in range(100):
        if len(transports) >= 2 and client.is_connected:
            break
        await asyncio.sleep(0.01)

    assert identities[:2] == [("", True), ("assigned-A", False)]
    assert client.effective_client_id == "assigned-A"
    await client.disconnect()


def test_non_durable_reconnect_does_not_reuse_stale_assignment() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True)
    )
    engine.begin_connect()
    _feed(engine, _connack(assigned="ephemeral-A"))
    engine.take_effects()
    assert engine._prefer_session_resume is False

    engine.notify_transport_closed()
    engine.take_effects()
    reconnect = engine.begin_connect()
    assert _connect_identity(reconnect) == ("", True)


def test_restart_with_unaddressable_persisted_session_fails_closed(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "assigned-session.db")
    store.put_out(
        OutboundMessage(
            mid=7,
            topic="out",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    engine = ProtocolEngine(_durable_config(clean_start=False), store)

    with pytest.raises(ProtocolError, match="stable non-empty client_id"):
        engine.begin_connect()

    assert engine.state is ConnectionState.NEW
    assert store.get_out(7) is not None
    assert engine.take_effects() == []
    store.close()


def test_restart_with_explicit_stable_client_id_can_resume(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "stable-session.db")
    store.put_out(
        OutboundMessage(
            mid=8,
            topic="out",
            payload=b"payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    engine = ProtocolEngine(_durable_config(client_id="stable-client", clean_start=False), store)

    connect = engine.begin_connect()
    assert _connect_identity(connect) == ("stable-client", False)
    store.close()
