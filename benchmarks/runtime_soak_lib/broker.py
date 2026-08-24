"""Deterministic packet-aware broker used by the runtime soak harness.

Inputs stay valid MQTT. The broker completes CONNECT, SUBSCRIBE, UNSUBSCRIBE,
PING, and QoS 0/1/2 PUBLISH, can echo deliveries, and can drop the transport
to force reconnect. Session Present is controlled per connection.
"""

from __future__ import annotations

import asyncio

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import pack_u16
from mqttium.codec.vbi import decode_vbi
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PubRecPacket,
    PubRelPacket,
    PublishPacket,
    encode_frame,
    encode_pingresp,
)

_UTF8_LEN = 2


class SoakTransport:
    """Queue-backed transport bound to one ``SoakBroker`` session."""

    def __init__(self, broker: SoakBroker) -> None:
        self._broker = broker
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def push_rx(self, data: bytes) -> None:
        loop = self._loop
        if loop is None:
            self._rx.put_nowait(data)
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            self._rx.put_nowait(data)
        else:
            loop.call_soon_threadsafe(self._rx.put_nowait, data)

    async def write(self, data: bytes) -> None:
        if self._closing:
            return
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets(limit=10_000):
            self._broker.handle_packet(self, raw)

    async def write_many(self, parts: list[bytes]) -> None:
        if parts:
            await self.write(b"".join(parts))

    async def read(self, n: int = 65536) -> bytes:
        del n
        self._loop = asyncio.get_running_loop()
        return await self._rx.get()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._loop = asyncio.get_running_loop()
        self.push_rx(b"")

    def is_closing(self) -> bool:
        return self._closing


class SoakBroker:
    """In-memory MQTT broker with explicit session and drop controls."""

    def __init__(
        self,
        *,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        echo: bool = True,
        session_present: bool = False,
    ) -> None:
        self.protocol = protocol
        self.echo = echo
        self.session_present = session_present
        self.connections = 0
        self.current: SoakTransport | None = None
        self.subscriptions: set[str] = set()
        self._client_qos2: set[int] = set()
        self._inbound_qos2_wait_pubrec: set[int] = set()
        self._inbound_qos2_wait_pubcomp: set[int] = set()
        self._inbound_publishes: dict[int, PublishPacket] = {}
        self._inbound_mid = 1
        self.seen_connects = 0
        self.last_clean_start = True
        self.drop_count = 0

    def factory(self) -> object:
        async def _factory(host: str, port: int, *, ssl: object = None) -> SoakTransport:
            del host, port, ssl
            transport = SoakTransport(self)
            self.current = transport
            self.connections += 1
            return transport

        return _factory

    def drop_current(self) -> None:
        transport = self.current
        if transport is None or transport.is_closing():
            return
        self.drop_count += 1
        transport._closing = True
        transport.push_rx(b"")

    def handle_packet(self, transport: SoakTransport, raw: RawPacket) -> None:
        packet_type = raw.packet_type
        if packet_type is PacketType.CONNECT:
            self._on_connect(transport, raw)
        elif packet_type is PacketType.PUBLISH:
            self._on_publish(transport, raw)
        elif packet_type is PacketType.PUBREC:
            self._on_pubrec(transport, raw)
        elif packet_type is PacketType.PUBREL:
            self._on_pubrel(transport, raw)
        elif packet_type is PacketType.PUBCOMP:
            self._on_pubcomp(raw)
        elif packet_type is PacketType.SUBSCRIBE:
            self._on_subscribe(transport, raw)
        elif packet_type is PacketType.UNSUBSCRIBE:
            self._on_unsubscribe(transport, raw)
        elif packet_type is PacketType.PINGREQ:
            transport.push_rx(encode_pingresp())
        elif packet_type is PacketType.DISCONNECT:
            return

    def _on_connect(self, transport: SoakTransport, raw: RawPacket) -> None:
        self.last_clean_start = _connect_clean_start(raw.remaining)
        self.seen_connects += 1
        present = self.session_present and not self.last_clean_start and self.seen_connects > 1
        if not present:
            self.subscriptions.clear()
            self._client_qos2.clear()
            self._inbound_qos2_wait_pubrec.clear()
            self._inbound_qos2_wait_pubcomp.clear()
            self._inbound_publishes.clear()
        transport.push_rx(_connack(self.protocol, present))
        if present:
            self._replay_session(transport)

    def _on_publish(self, transport: SoakTransport, raw: RawPacket) -> None:
        publish = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
        if publish.qos is QoS.AT_LEAST_ONCE:
            assert publish.mid is not None
            transport.push_rx(PubAckPacket(mid=publish.mid).encode(self.protocol))
        elif publish.qos is QoS.EXACTLY_ONCE:
            assert publish.mid is not None
            self._client_qos2.add(publish.mid)
            transport.push_rx(PubRecPacket(mid=publish.mid).encode(self.protocol))
        if self.echo and publish.topic in self.subscriptions:
            self._echo_publish(transport, publish)

    def _on_pubrec(self, transport: SoakTransport, raw: RawPacket) -> None:
        rec = PubRecPacket.decode(raw.remaining, self.protocol)
        if rec.mid in self._inbound_qos2_wait_pubrec:
            self._inbound_qos2_wait_pubrec.discard(rec.mid)
            self._inbound_publishes.pop(rec.mid, None)
            self._inbound_qos2_wait_pubcomp.add(rec.mid)
            transport.push_rx(PubRelPacket(mid=rec.mid).encode(self.protocol))

    def _on_pubrel(self, transport: SoakTransport, raw: RawPacket) -> None:
        rel = PubRelPacket.decode(raw.remaining, self.protocol)
        self._client_qos2.discard(rel.mid)
        transport.push_rx(PubCompPacket(mid=rel.mid).encode(self.protocol))

    def _on_pubcomp(self, raw: RawPacket) -> None:
        comp = PubCompPacket.decode(raw.remaining, self.protocol)
        self._inbound_qos2_wait_pubcomp.discard(comp.mid)

    def _on_subscribe(self, transport: SoakTransport, raw: RawPacket) -> None:
        mid = int.from_bytes(raw.remaining[:2], "big")
        topics = _subscribe_topics(raw.remaining, self.protocol)
        self.subscriptions.update(topics)
        reasons = bytes(2 for _ in topics) or b"\x02"
        props = b"\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b""
        transport.push_rx(encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + props + reasons))

    def _on_unsubscribe(self, transport: SoakTransport, raw: RawPacket) -> None:
        mid = int.from_bytes(raw.remaining[:2], "big")
        topics = _unsubscribe_topics(raw.remaining, self.protocol)
        self.subscriptions.difference_update(topics)
        if self.protocol is MQTTProtocolVersion.MQTTv5:
            reasons = bytes(0 for _ in topics) or b"\x00"
            body = pack_u16(mid) + b"\x00" + reasons
        else:
            body = pack_u16(mid)
        transport.push_rx(encode_frame(PacketType.UNSUBACK, 0, body))

    def _echo_publish(self, transport: SoakTransport, publish: PublishPacket) -> None:
        qos = publish.qos
        mid = None
        if qos is not QoS.AT_MOST_ONCE:
            mid = self._next_inbound_mid()
            if qos is QoS.EXACTLY_ONCE:
                self._inbound_qos2_wait_pubrec.add(mid)
        echoed = PublishPacket(
            topic=publish.topic,
            payload=publish.payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=mid,
        )
        if qos is QoS.EXACTLY_ONCE and mid is not None:
            self._inbound_publishes[mid] = echoed
        transport.push_rx(echoed.encode(self.protocol))

    def _replay_session(self, transport: SoakTransport) -> None:
        """Redeliver incomplete inbound QoS 2 after Session Present."""
        for publish in self._inbound_publishes.values():
            replayed = PublishPacket(
                topic=publish.topic,
                payload=publish.payload,
                qos=publish.qos,
                retain=False,
                dup=True,
                mid=publish.mid,
            )
            transport.push_rx(replayed.encode(self.protocol))
        for mid in list(self._inbound_qos2_wait_pubcomp):
            transport.push_rx(PubRelPacket(mid=mid).encode(self.protocol))

    def _next_inbound_mid(self) -> int:
        mid = self._inbound_mid
        self._inbound_mid = 1 if mid >= 65535 else mid + 1
        return mid


def _connack(protocol: MQTTProtocolVersion, session_present: bool) -> bytes:
    flags = 1 if session_present else 0
    body = bytes((flags, 0, 0)) if protocol is MQTTProtocolVersion.MQTTv5 else bytes((flags, 0))
    return encode_frame(PacketType.CONNACK, 0, body)


def _connect_clean_start(remaining: bytes) -> bool:
    name_len = int.from_bytes(remaining[:2], "big")
    flags = remaining[2 + name_len + 1]
    return bool(flags & 0x02)


def _skip_properties(remaining: bytes, pos: int, protocol: MQTTProtocolVersion) -> int:
    if protocol is not MQTTProtocolVersion.MQTTv5:
        return pos
    length, pos = decode_vbi(remaining, pos)
    return pos + length


def _subscribe_topics(remaining: bytes, protocol: MQTTProtocolVersion) -> list[str]:
    pos = _skip_properties(remaining, 2, protocol)
    topics: list[str] = []
    while pos + _UTF8_LEN < len(remaining):
        length = int.from_bytes(remaining[pos : pos + 2], "big")
        pos += 2
        topics.append(remaining[pos : pos + length].decode("utf-8"))
        pos += length + 1
    return topics or ["#"]


def _unsubscribe_topics(remaining: bytes, protocol: MQTTProtocolVersion) -> list[str]:
    pos = _skip_properties(remaining, 2, protocol)
    topics: list[str] = []
    while pos + _UTF8_LEN <= len(remaining):
        length = int.from_bytes(remaining[pos : pos + 2], "big")
        pos += 2
        topics.append(remaining[pos : pos + length].decode("utf-8"))
        pos += length
    return topics
