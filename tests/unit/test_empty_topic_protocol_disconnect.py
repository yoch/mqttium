"""MQTT 5 protocol errors should be announced before transport teardown."""

from __future__ import annotations

import asyncio

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _invalid_empty_topic_publish() -> bytes:
    return PublishPacket(
        topic="",
        payload=b"invalid",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=Properties(),
    ).encode(MQTTProtocolVersion.MQTTv5)


def test_empty_topic_without_alias_sends_protocol_error_disconnect() -> None:
    engine = ProtocolEngine(
        EngineConfig(client_id="empty-topic", protocol=MQTTProtocolVersion.MQTTv5),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED

    _feed(engine, _invalid_empty_topic_publish())
    effects = engine.take_effects()

    assert effects[0].kind is EffectKind.SEND
    disconnect_item = effects[0].data
    disconnect = disconnect_item if isinstance(disconnect_item, bytes) else disconnect_item[0]
    assert (disconnect[0] & 0xF0) == PacketType.DISCONNECT.value
    assert disconnect[2] == 0x82
    assert any(effect.kind is EffectKind.DISCONNECTED for effect in effects)
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)
    assert engine.state is ConnectionState.DISCONNECTED


class _EmptyTopicTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self._connected = False
        self.events: list[tuple[str, bytes | None]] = []

    async def write(self, data: bytes) -> None:
        self.events.append(("write", data))
        if not self._connected and data and data[0] == PacketType.CONNECT:
            self._connected = True
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
            self._rx.put_nowait(_invalid_empty_topic_publish())

    async def read(self, _n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self.events.append(("close", None))
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def test_runtime_writes_empty_topic_disconnect_before_close() -> None:
    client = AsyncClient(client_id="empty-topic-runtime", protocol=MQTTProtocolVersion.MQTTv5)
    transport = _EmptyTopicTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> _EmptyTopicTransport:
        return transport

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)

    reader = client._reader_task
    assert reader is not None
    await asyncio.wait_for(reader, timeout=1.0)

    disconnect_index = next(
        index
        for index, (kind, data) in enumerate(transport.events)
        if kind == "write"
        and data is not None
        and (data[0] & 0xF0) == PacketType.DISCONNECT.value
        and data[2] == 0x82
    )
    close_index = next(index for index, (kind, _) in enumerate(transport.events) if kind == "close")
    assert disconnect_index < close_index
    assert transport.is_closing()
    assert not client.is_connected
