"""AsyncClient reconnect preserves QoS receipts across a transport blip."""

from __future__ import annotations

import asyncio

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy


class Brokerside:
    def __init__(self, *, session_present: bool) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._session_present = session_present
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.hold_pubacks = False
        self._pending_pubacks: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                flags = 0x01 if self._session_present else 0x00
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, bytes((flags, 0x00))))
            elif raw.packet_type is PacketType.PUBLISH:
                pub = PublishPacket.decode(raw.flags, raw.remaining)
                if pub.qos == QoS.AT_LEAST_ONCE and pub.mid is not None:
                    ack = PubAckPacket(mid=pub.mid).encode()
                    if self.hold_pubacks:
                        self._pending_pubacks.append(ack)
                    else:
                        self._rx.put_nowait(ack)

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def test_receipt_survives_and_completes_after_reconnect() -> None:
    brokers: list[Brokerside] = []
    client = AsyncClient(
        "re",
        clean_start=False,
        reconnect=ReconnectPolicy(
            enabled=True, initial_delay=0.05, max_delay=0.1, stable_after=0.05
        ),
    )

    async def factory(host: str, port: int, *, ssl=None):
        b = Brokerside(session_present=bool(brokers))
        brokers.append(b)
        return b

    client._transport_factory = factory
    await client.connect("fake", 1, timeout=2)
    first = brokers[0]
    first.hold_pubacks = True
    receipt = await client.publish("t", b"x", qos=1)
    assert not receipt.is_done()

    await first.close()
    # Immediately after drop, receipt must not have been failed.
    await asyncio.sleep(0)
    assert receipt._error is None

    for _ in range(40):
        if client.is_connected and len(brokers) >= 2:
            break
        await asyncio.sleep(0.05)
    assert client.is_connected
    assert len(brokers) >= 2

    await asyncio.wait_for(receipt.wait(), timeout=2.0)
    assert receipt._error is None
    await client.disconnect()
