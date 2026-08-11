"""Reproduction for GitHub issue #134."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.errors import MQTTTimeoutError, ProtocolError
from mqttium.packets import encode_frame


class _CrossAckTransport:
    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False

    async def write(self, data: bytes | tuple[bytes, bytes]) -> None:
        payload = data if isinstance(data, bytes) else data[0] + data[1]
        self._decoder.feed(payload)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
            elif raw.packet_type is PacketType.UNSUBSCRIBE:
                mid = (raw.remaining[0] << 8) | raw.remaining[1]
                body = bytes([(mid >> 8) & 255, mid & 255]) + b"\x00\x00"
                self._rx.put_nowait(encode_frame(PacketType.SUBACK, 0, body))

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/134 — SUBACK accepted for UNSUBSCRIBE mid",
)
async def test_suback_for_unsubscribe_mid_is_protocol_error() -> None:
    transport = _CrossAckTransport()
    client = AsyncClient(client_id="bug134", protocol=MQTTProtocolVersion.MQTTv5, ack_timeout=0.25)

    async def factory(host: str, port: int, *, ssl: object = None) -> _CrossAckTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)

    with pytest.raises(ProtocolError):
        await client.unsubscribe("t")

    assert client._engine.state is not ConnectionState.CONNECTED
    await client._force_close()
