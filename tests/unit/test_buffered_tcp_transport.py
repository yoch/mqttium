"""Allocation-stable cleartext TCP receive path."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.transport._buffered import BufferedSocketProtocol, _READ_CHUNK
from mqttium.transport.tcp import TcpTransport


class _FakeTransport:
    def __init__(self) -> None:
        self.paused = False
        self.closed = False

    def pause_reading(self) -> None:
        self.paused = True

    def resume_reading(self) -> None:
        self.paused = False

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_buffered_protocol_reuses_one_receive_buffer() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    fake = _FakeTransport()
    protocol.connection_made(fake)  # type: ignore[arg-type]

    first = protocol.get_buffer(-1)
    second = protocol.get_buffer(4096)
    assert first is second
    assert len(first) == _READ_CHUNK

    first[:5] = b"hello"
    protocol.buffer_updated(5)
    assert await protocol.read(_READ_CHUNK) == b"hello"
    assert protocol.buffered_bytes == 0


@pytest.mark.asyncio
async def test_buffered_protocol_read_can_split_received_chunk() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]
    buffer = protocol.get_buffer(-1)
    buffer[:6] = b"abcdef"
    protocol.buffer_updated(6)

    assert await protocol.read(2) == b"ab"
    assert await protocol.read(4) == b"cdef"
    assert protocol.buffered_bytes == 0


@pytest.mark.asyncio
async def test_cleartext_tcp_roundtrip_uses_buffered_transport() -> None:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(65536)
        writer.write(data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    sock = server.sockets[0]
    host, port = sock.getsockname()[:2]
    try:
        transport = await TcpTransport.connect(str(host), int(port))
        assert isinstance(transport, TcpTransport)
        await transport.write(b"mqttium-buffered-recv")
        assert await transport.read(65536) == b"mqttium-buffered-recv"
        stats = transport.stats()
        assert stats.kind == "TcpTransport"
        assert stats.buffered_read_bytes == 0
        await transport.close()
    finally:
        server.close()
        await server.wait_closed()
