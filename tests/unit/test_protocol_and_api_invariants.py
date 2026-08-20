"""Cross-layer protocol and AsyncClient invariants."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import (
    PublishPacket,
    encode_frame,
)
from mqttium.codec.primitives import pack_u16
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.types import Properties


# --- B1: PacketIdPool must not reissue reserved mids ------------------------


def test_packet_id_pool_skips_reserved() -> None:
    pool = PacketIdPool()
    pool.reserve(1)
    pool.reserve(2)
    pool.reserve(3)
    assert pool.allocate() == 4
    assert pool.allocate() == 5


def test_engine_hydrated_store_no_mid_collision(tmp_path) -> None:
    from mqttium.persistence.sqlite import SqliteInflightStore
    from mqttium.types import OutboundMessage
    from mqttium.enums import OutboundQoSState

    store = SqliteInflightStore(tmp_path / "s.db")
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="t",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    engine = ProtocolEngine(EngineConfig(client_id="c"), store=store)
    assert engine.packet_ids.allocate() != 1


# --- H3/F5: SUBACK for unknown mid must not release foreign mids ------------


def test_suback_unknown_mid_not_released() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c"))
    engine.state = ConnectionState.CONNECTED
    # Allocate a publish mid (in flight).
    pub_mid = engine.packet_ids.allocate()
    # Malicious/buggy broker sends SUBACK for that mid.
    body = pack_u16(pub_mid) + bytes([0])
    engine.handle_raw(RawPacket(PacketType.SUBACK, 0, body))
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    # Publish mid still in use.
    assert engine.packet_ids.in_use(pub_mid)


def test_suback_known_mid_released() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c"))
    engine.state = ConnectionState.CONNECTED
    mid = engine.queue_subscribe("a/b")
    engine.take_effects()  # drop SEND
    body = pack_u16(mid) + bytes([0])
    engine.handle_raw(RawPacket(PacketType.SUBACK, 0, body))
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.SUBACK for e in effects)
    assert not engine.packet_ids.in_use(mid)


# --- F4/M4: duplicate CONNACK rejected --------------------------------------


def test_duplicate_connack_rejected() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c"))
    engine.begin_connect()
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    assert engine.state == ConnectionState.CONNECTED
    # Second CONNACK → protocol error.
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)


# --- B2: deferred replay keeps DUP=1 ----------------------------------------


def test_replay_deferred_reencodes_dup() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", clean_start=False, local_receive_maximum=1))
    engine.begin_connect()
    engine.take_effects()
    # CONNACK session_present=0, then publish 2 QoS1 (second queued).
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    engine.queue_publish("t", b"1", qos=QoS.AT_LEAST_ONCE)
    engine.queue_publish("t", b"2", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    # Simulate disconnect + reconnect with session_present=1, RM=1.
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    engine.take_effects()
    # session_present=1 CONNACK
    engine.handle_raw(RawPacket(PacketType.CONNACK, 0, b"\x01\x00"))
    effects = engine.take_effects()
    sends = [e.data for e in effects if e.kind is EffectKind.SEND]
    # First replayed publish must have DUP=1.
    assert sends, "expected replayed publish"
    first = sends[0]
    if isinstance(first, tuple):
        first = first[0]
    assert first[0] & 0x08, "replayed PUBLISH must carry DUP=1"


# --- B3/F9: empty topic without alias rejected ------------------------------


def test_empty_topic_no_alias_rejected_v5() -> None:
    engine = ProtocolEngine(EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    pkt = PublishPacket(topic="", payload=b"x", qos=QoS.AT_MOST_ONCE, retain=False, dup=False)
    wire = pkt.encode(MQTTProtocolVersion.MQTTv5)
    dec = IncrementalDecoder()
    dec.feed(wire)
    raw = dec.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert not any(e.kind is EffectKind.MESSAGE for e in effects)


# --- C1: on_connect may await subscribe without deadlocking -----------------


async def test_on_connect_await_subscribe_no_deadlock() -> None:
    class FakeTransport:
        def __init__(self) -> None:
            self._rx: asyncio.Queue[bytes] = asyncio.Queue()
            self._decoder = IncrementalDecoder()
            self._closing = False

        async def write(self, data: bytes) -> None:
            self._decoder.feed(data)
            for raw in self._decoder.drain_packets():
                if raw.packet_type is PacketType.CONNECT:
                    self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
                elif raw.packet_type is PacketType.SUBSCRIBE:
                    mid = int.from_bytes(raw.remaining[:2], "big")
                    self._rx.put_nowait(
                        encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + bytes([0]))
                    )

        async def read(self, n: int = 65536) -> bytes:
            return await self._rx.get()

        async def close(self) -> None:
            self._closing = True
            self._rx.put_nowait(b"")

        def is_closing(self) -> bool:
            return self._closing

    client = AsyncClient(client_id="c1")
    fake = FakeTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    done = asyncio.Event()

    async def on_connect(connack) -> None:
        await client.subscribe("a/b")
        done.set()

    client.on_connect = on_connect
    connack = await client.connect("fake", 1883)
    assert connack.reason_code == 0
    await asyncio.wait_for(done.wait(), timeout=3.0)
    assert client.is_connected
    await client.disconnect()


# --- C4: _last_disconnect cleared on reconnect ------------------------------


async def test_last_disconnect_cleared_on_connect() -> None:
    from mqttium.protocol.engine import DisconnectInfo

    client = AsyncClient(client_id="c4")
    client._last_disconnect = DisconnectInfo(reason_code=0x8C, from_broker=True)

    class FakeTransport:
        def __init__(self) -> None:
            self._rx: asyncio.Queue[bytes] = asyncio.Queue()
            self._decoder = IncrementalDecoder()
            self._closing = False

        async def write(self, data: bytes) -> None:
            self._decoder.feed(data)
            for raw in self._decoder.drain_packets():
                if raw.packet_type is PacketType.CONNECT:
                    self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))

        async def read(self, n: int = 65536) -> bytes:
            return await self._rx.get()

        async def close(self) -> None:
            self._closing = True
            self._rx.put_nowait(b"")

        def is_closing(self) -> bool:
            return self._closing

    fake = FakeTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> FakeTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)
    assert client._last_disconnect is None
    await client.disconnect()


# --- WebSocket bounds ---------------------------------------------------------


def test_ws_frame_too_large_rejected() -> None:
    from mqttium.transport.websocket import _parse_frame

    buf = bytearray()
    buf.append(0x82)  # FIN + binary
    buf.append(0xFF)  # 127 → 64-bit length follows
    buf.extend((2**40).to_bytes(8, "big"))
    with pytest.raises(ConnectionError):
        _parse_frame(buf, 16 * 1024 * 1024)


def test_ws_control_payload_too_large_rejected() -> None:
    from mqttium.transport.websocket import _parse_frame

    buf = bytearray()
    buf.append(0x89)  # FIN + ping
    buf.append(126)  # 16-bit length
    buf.extend((60000).to_bytes(2, "big"))
    with pytest.raises(ConnectionError):
        _parse_frame(buf, 16 * 1024 * 1024)


# --- Props 0/1 validation -----------------------------------------------------


def test_property_zero_one_enforced() -> None:
    from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties

    props = Properties()
    props.set("payload_format_indicator", 2)
    with pytest.raises(Exception):
        encode_properties(props, PUBLISH)

    # decode path: PFI=2
    raw = bytes([0x02, 0x01, 0x02])
    with pytest.raises(Exception):
        decode_properties(raw, 0, PUBLISH)


# --- Topic matcher deep topics (no recursion) ---------------------------------


def test_matcher_deep_topic_no_recursion_error() -> None:
    from mqttium.dispatch.matcher import TopicMatcher

    m = TopicMatcher()
    m["/".join(["a"] * 2000)] = "deep"
    topic = "/".join(["a"] * 2000)
    assert list(m.iter_match(topic)) == ["deep"]
