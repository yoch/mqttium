"""Reproduction for GitHub issue #90.

``AsyncClient.disconnect()`` can hang forever waiting to enqueue DISCONNECT
when the writer is blocked and outbound byte backpressure is saturated.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame


class _BlockingTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.gate = asyncio.Event()

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        payload = data if isinstance(data, bytes) else data[0] + data[1]
        self._decoder.feed(payload)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
                return
        await self.gate.wait()

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self.gate.set()
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/90 — disconnect hangs on writer backpressure",
)
async def test_disconnect_completes_when_writer_is_wedged() -> None:
    transport = _BlockingTransport()
    client = AsyncClient(
        client_id="bug90",
        protocol=MQTTProtocolVersion.MQTTv5,
        max_outbound_bytes=1,
    )

    async def factory(host: str, port: int, *, ssl: object = None) -> _BlockingTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)
    client.publish_nowait("t", b"ABCDEFGH", qos=0)
    await asyncio.sleep(0.05)

    await asyncio.wait_for(client.disconnect(), timeout=1.0)
