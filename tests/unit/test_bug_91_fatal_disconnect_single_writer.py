"""Reproduction for GitHub issue #91.

Fatal DISCONNECT is written directly from the reader task via
``_send_fatal_disconnect()``, violating the single-writer invariant and
allowing overlapping ``transport.write()`` calls.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame


class _RaceTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.active = 0
        self.concurrent = False
        self.block = asyncio.Event()
        self.started = asyncio.Event()

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        payload = data if isinstance(data, bytes) else data[0] + data[1]
        self.active += 1
        if self.active > 1:
            self.concurrent = True
        try:
            self._decoder.feed(payload)
            for raw in self._decoder.drain_packets():
                if raw.packet_type is PacketType.CONNECT:
                    self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
                elif raw.packet_type is PacketType.PUBLISH:
                    self.started.set()
                    await self.block.wait()
        finally:
            self.active -= 1

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self.block.set()
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/91 — fatal DISCONNECT bypasses single writer",
)
async def test_fatal_disconnect_does_not_overlap_writer_transport_write() -> None:
    transport = _RaceTransport()
    client = AsyncClient(
        client_id="bug91",
        protocol=MQTTProtocolVersion.MQTTv5,
        maximum_packet_size=20,
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> _RaceTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)
    pub = asyncio.create_task(client.publish("t", b"payload-long", qos=0))
    await asyncio.wait_for(transport.started.wait(), timeout=2.0)

    big = PublishPacket(
        topic="x",
        payload=b"Z" * 50,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    ).encode(MQTTProtocolVersion.MQTTv5)
    assert len(big) > 20
    transport._rx.put_nowait(big)
    await asyncio.sleep(0.2)

    assert transport.concurrent is False
    transport.block.set()
    pub.cancel()
    await client._force_close()
