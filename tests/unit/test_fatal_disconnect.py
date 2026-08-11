"""Normative fatal DISCONNECT (0x95/0x81/0x82) before close."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame


class _FatalTransport:
    """Feeds a valid CONNACK then an oversized packet to trigger 0x95."""

    def __init__(
        self,
        proto: MQTTProtocolVersion,
        *,
        inject_oversized_on_connect: bool = True,
    ) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self.written: list[bytes] = []
        self._proto = proto
        self._inject_oversized_on_connect = inject_oversized_on_connect
        self._connected = False

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        if not self._connected and data and data[0] == PacketType.CONNECT:
            self._connected = True
            # CONNACK success (v5: flags, reason, empty props).
            body = b"\x00\x00\x00" if self._proto == MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, body))
            if not self._inject_oversized_on_connect:
                return
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


class _BlockedFatalTransport(_FatalTransport):
    """Expose concurrent calls while one application write is blocked."""

    def __init__(self) -> None:
        super().__init__(
            MQTTProtocolVersion.MQTTv5,
            inject_oversized_on_connect=False,
        )
        self.block_application_writes = False
        self.write_started = asyncio.Event()
        self.active_writes = 0
        self.concurrent_write = False

    async def write(self, data: bytes) -> None:
        self.active_writes += 1
        if self.active_writes > 1:
            self.concurrent_write = True
        try:
            if self.block_application_writes and data and data[0] != PacketType.CONNECT:
                self.write_started.set()
                await asyncio.Event().wait()
            await super().write(data)
        finally:
            self.active_writes -= 1

    def trigger_oversized_packet(self) -> None:
        big = bytearray([PacketType.PUBLISH])
        remaining = 20 * 1024 * 1024
        while True:
            digit = remaining % 128
            remaining //= 128
            if remaining:
                digit |= 0x80
            big.append(digit)
            if not remaining:
                break
        self._rx.put_nowait(bytes(big))


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


@pytest.mark.asyncio
async def test_fatal_disconnect_never_writes_concurrently_with_write_pump() -> None:
    client = AsyncClient(client_id="single-writer", protocol=MQTTProtocolVersion.MQTTv5)
    fake = _BlockedFatalTransport()

    async def factory(host: str, port: int, *, ssl: object = None) -> _BlockedFatalTransport:
        return fake

    client._transport_factory = factory  # type: ignore[assignment]
    await client.connect("fake", 1883)
    fake.block_application_writes = True
    await client.publish("blocked", b"payload")
    await asyncio.wait_for(fake.write_started.wait(), timeout=1.0)

    fake.trigger_oversized_packet()
    reader = client._reader_task
    assert reader is not None
    await asyncio.wait_for(reader, timeout=1.0)

    assert not fake.concurrent_write
    assert fake.is_closing()
