"""Normative fatal DISCONNECT (0x95/0x81/0x82) before close."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame


class _FatalTransport:
    """Feeds a valid CONNACK then an oversized packet to trigger 0x95."""

    def __init__(self, proto: MQTTProtocolVersion) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self.written: list[bytes] = []
        self._proto = proto
        self._connected = False

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        if not self._connected and data and data[0] == PacketType.CONNECT:
            self._connected = True
            # CONNACK success (v5: flags, reason, empty props).
            body = b"\x00\x00\x00" if self._proto == MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, body))
            # Then an oversized PUBLISH (remaining length huge) to trip the
            # decoder's max_packet_size (default 16 MiB).
            big = bytearray()
            big.append(PacketType.PUBLISH)
            # VBI for 20 MiB remaining length.
            rl = 20 * 1024 * 1024
            while True:
                d = rl % 128
                rl //= 128
                if rl:
                    d |= 0x80
                big.append(d)
                if not rl:
                    break
            self._rx.put_nowait(bytes(big))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.asyncio
async def test_fatal_oversize_sends_disconnect_0x95_v5() -> None:
    client = AsyncClient(client_id="c", protocol=MQTTProtocolVersion.MQTTv5)
    fake = _FatalTransport(MQTTProtocolVersion.MQTTv5)

    async def factory(host: str, port: int, *, ssl: object = None) -> _FatalTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)
    # Allow reader to process the oversized packet and close.
    await asyncio.sleep(0.2)
    assert not client.is_connected
    # A DISCONNECT with reason 0x95 must have been written.
    disconnects = [w for w in fake.written if w and w[0] == PacketType.DISCONNECT]
    assert disconnects, "expected a DISCONNECT frame"
    assert disconnects[-1][2] == 0x95  # reason code byte


@pytest.mark.asyncio
async def test_fatal_oversize_no_disconnect_v311() -> None:
    client = AsyncClient(client_id="c", protocol=MQTTProtocolVersion.MQTTv311)
    fake = _FatalTransport(MQTTProtocolVersion.MQTTv311)

    async def factory(host: str, port: int, *, ssl: object = None) -> _FatalTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)
    await asyncio.sleep(0.2)
    assert not client.is_connected
    # v3.1.1 has no DISCONNECT reason codes — nothing extra written.
    disconnects = [w for w in fake.written if w and w[0] == PacketType.DISCONNECT]
    assert not disconnects
