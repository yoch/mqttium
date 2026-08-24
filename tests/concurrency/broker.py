"""Packet-aware in-memory broker with explicit ACK, hold, and failure controls."""

from __future__ import annotations

import asyncio

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import pack_u16
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubAckPacket, PubRecPacket, PublishPacket, encode_frame, encode_pingresp


class ControllableBroker:
    """Queue-backed broker used by concurrency scenarios.

    Default behaviour completes CONNECT. PUBLISH acknowledgements are optional
    so a schedule can inject PUBACK/PUBREC at a named checkpoint rather than
    racing the writer.
    """

    def __init__(
        self,
        *,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        session_present: bool = False,
        auto_ack: bool = True,
        auto_pingresp: bool = True,
    ) -> None:
        self.protocol = protocol
        self.session_present = session_present
        self.auto_ack = auto_ack
        self.auto_pingresp = auto_pingresp
        self.decoder = IncrementalDecoder()
        self.written: list[bytes] = []
        self.publishes: list[PublishPacket] = []
        self.write_failures = 0
        self.fail_next_write = False
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self._write_error: BaseException | None = None

    def fail_writes(self, exc: BaseException | None = None) -> None:
        self.fail_next_write = True
        self._write_error = exc or ConnectionResetError("injected write failure")

    def _accept_bytes(self, data: bytes) -> None:
        if self._closing:
            raise ConnectionResetError("transport is closed")
        if self.fail_next_write:
            self.fail_next_write = False
            self.write_failures += 1
            assert self._write_error is not None
            raise self._write_error
        self.written.append(data)
        self.decoder.feed(data)
        for raw in self.decoder.drain_packets():
            self.handle_packet(raw)

    async def write(self, data: bytes) -> None:
        self._accept_bytes(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self._accept_bytes(b"".join(parts))

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

    def push_rx(self, data: bytes) -> None:
        self._rx.put_nowait(data)

    def inject_connack(self, *, session_present: bool | None = None, reason_code: int = 0) -> None:
        flags = 1 if (self.session_present if session_present is None else session_present) else 0
        body = bytes((flags, reason_code))
        if self.protocol is MQTTProtocolVersion.MQTTv5:
            body += b"\x00"
        self.push_rx(encode_frame(PacketType.CONNACK, 0, body))

    def inject_puback(self, mid: int) -> None:
        self.push_rx(PubAckPacket(mid=mid).encode(self.protocol))

    def inject_pubrec(self, mid: int) -> None:
        self.push_rx(PubRecPacket(mid=mid).encode(self.protocol))

    def inject_publish(
        self,
        topic: str,
        payload: bytes = b"x",
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        mid: int | None = None,
    ) -> None:
        packet = PublishPacket(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=mid,
        )
        self.push_rx(packet.encode(self.protocol))

    def handle_packet(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.CONNECT:
            self.inject_connack()
        elif raw.packet_type is PacketType.PUBLISH:
            publish = PublishPacket.decode(raw.flags, raw.remaining, self.protocol)
            self.publishes.append(publish)
            if self.auto_ack and publish.qos is QoS.AT_LEAST_ONCE and publish.mid is not None:
                self.inject_puback(publish.mid)
            elif self.auto_ack and publish.qos is QoS.EXACTLY_ONCE and publish.mid is not None:
                self.inject_pubrec(publish.mid)
        elif raw.packet_type is PacketType.SUBSCRIBE:
            mid = int.from_bytes(raw.remaining[:2], "big")
            properties = b"\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b""
            self.push_rx(encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + properties + b"\x00"))
        elif raw.packet_type is PacketType.PINGREQ and self.auto_pingresp:
            self.push_rx(encode_pingresp())


class BrokerFactory:
    """Return a fresh controllable transport on every connect/reconnect."""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.transports: list[ControllableBroker] = []

    async def __call__(self, host: str, port: int, *, ssl: object = None) -> ControllableBroker:
        del host, port, ssl
        transport = ControllableBroker(**self.kwargs)  # type: ignore[arg-type]
        self.transports.append(transport)
        return transport

    @property
    def current(self) -> ControllableBroker:
        if not self.transports:
            raise RuntimeError("no transport has been created yet")
        return self.transports[-1]
