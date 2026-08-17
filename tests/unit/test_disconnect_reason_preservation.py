"""Runtime regressions for terminal ingress handling."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import PacketType
from mqttium.errors import ProtocolError
from mqttium.packets import encode_frame


class _RefusalWithTrailingPacketTransport:
    """Return a refused CONNACK and a trailing packet in the same read batch."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes) -> None:
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                refusal = encode_frame(PacketType.CONNACK, 0, b"\x00\x05")
                trailing = encode_frame(PacketType.PINGRESP, 0, b"")
                self._rx.put_nowait(refusal + trailing)

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def test_refused_connack_keeps_reason_with_trailing_buffered_packet() -> None:
    """Post-terminal transport noise must not replace the CONNACK refusal."""
    client = AsyncClient(client_id="refusal-trailing-packet")
    transport = _RefusalWithTrailingPacketTransport()

    async def factory(
        host: str,
        port: int,
        *,
        ssl: object = None,
    ) -> _RefusalWithTrailingPacketTransport:
        return transport

    client._transport_factory = factory

    with pytest.raises(ProtocolError, match=r"Connection refused: reason_code=5"):
        await client.connect("fake", timeout=1.0)

    assert isinstance(client._disconnect_exc, ProtocolError)
    assert str(client._disconnect_exc) == "Connection refused: reason_code=5"
    assert transport.is_closing()
