"""Tests for the #446 BufferedProtocol fallback retained beside direct ingress."""

from __future__ import annotations

import asyncio

import pytest

import mqttium.transport.tcp as tcp_module
from mqttium.transport.tcp import (
    _RECEIVE_BUFFER_SIZE,
    _BufferedStreamReaderProtocol,
    TcpTransport,
)


class _ReadTransport:
    def __init__(self) -> None:
        self.pause_calls = 0
        self.resume_calls = 0

    def pause_reading(self) -> None:
        self.pause_calls += 1

    def resume_reading(self) -> None:
        self.resume_calls += 1


async def test_buffered_stream_protocol_reuses_buffer_and_keeps_stream_coalescing() -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = _BufferedStreamReaderProtocol(reader, loop=loop)

    first = protocol.get_buffer(-1)
    second = protocol.get_buffer(1)
    assert first is second
    assert len(first) == _RECEIVE_BUFFER_SIZE
    assert isinstance(protocol, asyncio.StreamReaderProtocol)
    assert isinstance(protocol, asyncio.BufferedProtocol)

    first[:3] = b"abc"
    protocol.buffer_updated(3)
    first[:3] = b"def"
    protocol.buffer_updated(3)
    assert await reader.read(6) == b"abcdef"


async def test_buffered_stream_protocol_keeps_streamreader_backpressure() -> None:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=64 * 1024, loop=loop)
    protocol = _BufferedStreamReaderProtocol(reader, loop=loop)
    transport = _ReadTransport()
    reader.set_transport(transport)  # type: ignore[arg-type]
    buffer = protocol.get_buffer(-1)

    buffer[:] = b"a" * _RECEIVE_BUFFER_SIZE
    protocol.buffer_updated(_RECEIVE_BUFFER_SIZE)
    assert transport.pause_calls == 0

    buffer[:] = b"b" * _RECEIVE_BUFFER_SIZE
    protocol.buffer_updated(_RECEIVE_BUFFER_SIZE)
    assert transport.pause_calls == 1
    assert len(await reader.read(96 * 1024)) == 96 * 1024
    assert transport.resume_calls == 1


async def test_tcp_buffered_fallback_still_uses_recv_into_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the non-promoted selector fallback and verify #446 semantics."""
    calls = {"get_buffer": 0, "buffer_updated": 0, "data_received": 0}
    original_get_buffer = _BufferedStreamReaderProtocol.get_buffer
    original_buffer_updated = _BufferedStreamReaderProtocol.buffer_updated
    original_data_received = asyncio.StreamReaderProtocol.data_received

    def tracked_get_buffer(self: _BufferedStreamReaderProtocol, sizehint: int) -> memoryview:
        calls["get_buffer"] += 1
        return original_get_buffer(self, sizehint)

    def tracked_buffer_updated(self: _BufferedStreamReaderProtocol, nbytes: int) -> None:
        calls["buffer_updated"] += 1
        original_buffer_updated(self, nbytes)

    def tracked_data_received(self: _BufferedStreamReaderProtocol, data: bytes) -> None:
        calls["data_received"] += 1
        original_data_received(self, data)

    monkeypatch.setattr(tcp_module, "_direct_ingress_supported", lambda _ssl, _loop: False)
    monkeypatch.setattr(_BufferedStreamReaderProtocol, "get_buffer", tracked_get_buffer)
    monkeypatch.setattr(_BufferedStreamReaderProtocol, "buffer_updated", tracked_buffer_updated)
    monkeypatch.setattr(_BufferedStreamReaderProtocol, "data_received", tracked_data_received)

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(65536)
        writer.write(data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    sock = server.sockets[0]
    host, port = sock.getsockname()[:2]
    transport: TcpTransport | None = None
    try:
        transport = await TcpTransport.connect(str(host), int(port))
        protocol = transport._writer._protocol  # type: ignore[attr-defined]
        loop = asyncio.get_running_loop()
        if isinstance(loop, asyncio.SelectorEventLoop):
            assert isinstance(protocol, _BufferedStreamReaderProtocol)
        await transport.write(b"mqttium-buffered-stream")
        assert await transport.read(65536) == b"mqttium-buffered-stream"
        if isinstance(loop, asyncio.SelectorEventLoop):
            assert calls["get_buffer"] > 0
            assert calls["buffer_updated"] > 0
            assert calls["data_received"] == 0
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        await server.wait_closed()
