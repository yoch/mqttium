"""Keepalive behaviour via AsyncClient + fake transport."""

from __future__ import annotations

import asyncio

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_u16
from mqttium.enums import PacketType
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame, encode_pingresp


class FakeBrokerTransport:
    def __init__(self, *, auto_pingresp: bool = True) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.written: list[bytes] = []
        self.auto_pingresp = auto_pingresp
        self.pingreqs = 0

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
            elif raw.packet_type is PacketType.PINGREQ:
                self.pingreqs += 1
                if self.auto_pingresp:
                    self._rx.put_nowait(encode_pingresp())
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                if pub.qos and pub.mid is not None:
                    self._rx.put_nowait(PubAckPacket(mid=pub.mid).encode())
            elif raw.packet_type is PacketType.SUBSCRIBE:
                mid = int.from_bytes(raw.remaining[:2], "big")
                self._rx.put_nowait(encode_frame(PacketType.SUBACK, 0, pack_u16(mid) + bytes([0])))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def test_keepalive_sends_pingreq() -> None:
    client = AsyncClient(client_id="ka", keepalive=1, ping_timeout=2.0)
    fake = FakeBrokerTransport(auto_pingresp=True)

    async def factory(host: str, port: int, *, ssl=None) -> FakeBrokerTransport:
        return fake

    client._transport_factory = factory
    await client.connect("fake", 1883, timeout=2.0)
    # Wait long enough for keepalive to fire (K=1s).
    await asyncio.sleep(1.6)
    assert fake.pingreqs >= 1
    await client.disconnect()


async def test_keepalive_zero_disabled() -> None:
    client = AsyncClient(client_id="ka0", keepalive=0)
    fake = FakeBrokerTransport()

    async def factory(host: str, port: int, *, ssl=None) -> FakeBrokerTransport:
        return fake

    client._transport_factory = factory
    await client.connect("fake", 1883, timeout=2.0)
    await asyncio.sleep(0.3)
    assert fake.pingreqs == 0
    await client.disconnect()
