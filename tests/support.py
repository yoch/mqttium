"""Shared in-memory transport utilities for unit tests."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import pack_u16
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame, encode_pingresp


class QueueTransport:
    """Queue-backed transport with thread-safe inbound injection."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def push_rx(self, data: bytes) -> None:
        """Inject bytes safely from either the client loop or another thread."""
        loop = self._loop
        if loop is None:
            self._rx.put_nowait(data)
            return
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is loop:
            self._rx.put_nowait(data)
        else:
            loop.call_soon_threadsafe(self._rx.put_nowait, data)

    async def read(self, n: int = 65536) -> bytes:
        del n
        self._loop = asyncio.get_running_loop()
        return await self._rx.get()

    async def close(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._closing = True
        self.push_rx(b"")

    def is_closing(self) -> bool:
        return self._closing


class ScriptedBrokerTransport(QueueTransport):
    """Small packet-aware broker used by API-level unit tests.

    The default script completes CONNECT, QoS 1 PUBLISH, and SUBSCRIBE. Tests
    that need hostile ordering or fault injection should keep a focused local
    double instead of adding scenario-specific switches here.
    """

    def __init__(
        self,
        *,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        suback_reason: int = 0,
        publish_after_subscribe: bytes | None = None,
        auto_pingresp: bool = True,
    ) -> None:
        super().__init__()
        self.protocol = protocol
        self.suback_reason = suback_reason
        self.publish_after_subscribe = publish_after_subscribe
        self.auto_pingresp = auto_pingresp
        self.decoder = IncrementalDecoder()
        self.written: list[bytes] = []
        self.publishes: list[PublishPacket] = []
        self.pingreqs = 0

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            self.handle_packet(raw)

    async def write_many(self, parts: list[bytes]) -> None:
        await self.write(b"".join(parts))

    def handle_packet(self, raw: RawPacket) -> None:
        """Apply the default broker response for one complete packet."""
        if raw.packet_type is PacketType.CONNECT:
            body = b"\x00\x00"
            if self.protocol is MQTTProtocolVersion.MQTTv5:
                body += b"\x00"
            self.push_rx(encode_frame(PacketType.CONNACK, 0, body))
        elif raw.packet_type is PacketType.PUBLISH:
            publish = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
            self.publishes.append(publish)
            if publish.qos is QoS.AT_LEAST_ONCE:
                assert publish.mid is not None
                self.push_rx(PubAckPacket(mid=publish.mid).encode(self.protocol))
        elif raw.packet_type is PacketType.SUBSCRIBE:
            mid = int.from_bytes(raw.remaining[:2], "big")
            properties = b"\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b""
            self.push_rx(
                encode_frame(
                    PacketType.SUBACK,
                    0,
                    pack_u16(mid) + properties + bytes((self.suback_reason,)),
                )
            )
            if self.publish_after_subscribe is not None:
                self.push_rx(self.publish_after_subscribe)
        elif raw.packet_type is PacketType.PINGREQ:
            self.pingreqs += 1
            if self.auto_pingresp:
                self.push_rx(encode_pingresp())


def transport_factory(transport: QueueTransport):
    """Return an AsyncClient-compatible factory for a fixed test transport."""

    async def factory(host: str, port: int, *, ssl: object = None) -> QueueTransport:
        del host, port, ssl
        return transport

    return factory


async def wait_until(
    predicate: Callable[[], bool],
    *,
    attempts: int = 100,
) -> None:
    """Yield until a deterministic asynchronous condition becomes true."""
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("asynchronous condition was not reached")


def feed_engine(engine: object, wire: bytes) -> None:
    """Decode one complete packet and pass it to a protocol engine."""
    from mqttium.codec.buffer import IncrementalDecoder

    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)  # type: ignore[attr-defined]


def write_item_bytes(data: object) -> bytes:
    """Flatten the bytes/segmented-write representation used by SEND effects."""
    if isinstance(data, bytes):
        return data
    assert isinstance(data, tuple)
    return data[0] + data[1]
