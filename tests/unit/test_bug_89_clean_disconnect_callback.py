"""Reproduction for GitHub issue #89.

Intentional ``await client.disconnect()`` currently invokes ``on_disconnect``
with ``MQTTError("Connection closed")`` instead of a clean-disconnect signal.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame


class _ConnackTransport:
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
    reason="https://github.com/yoch/mqttium/issues/89 — clean disconnect reports MQTTError",
)
async def test_on_disconnect_is_none_after_intentional_disconnect() -> None:
    transport = _ConnackTransport()
    seen: list[object] = []
    client = AsyncClient(client_id="bug89", protocol=MQTTProtocolVersion.MQTTv5)
    client.on_disconnect = lambda exc: seen.append(exc)

    async def factory(host: str, port: int, *, ssl: object = None) -> _ConnackTransport:
        del host, port, ssl
        return transport

    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("fake", timeout=2.0)
    await client.disconnect()
    await asyncio.sleep(0.05)

    assert seen, "on_disconnect must fire"
    assert seen[0] is None
