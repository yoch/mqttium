"""Tests for the small BufferedProtocol adapter used by selector TCP streams."""

from __future__ import annotations

import asyncio

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

    # Unlike #445's direct chunk queue, this alternative intentionally keeps
    # StreamReader semantics, including coalescing data already buffered.
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

    # StreamReader owns the historical 128/64-KiB high/low-water behavior.
    assert len(await reader.read(96 * 1024)) == 96 * 1024
    assert transport.resume_calls == 1


async def test_tcp_roundtrip_uses_buffered_stream_protocol_only_on_selector_loop() -> None:
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
        protocol = transport._writer._protocol  # type: ignore[attr-defined]
        loop = asyncio.get_running_loop()
        if isinstance(loop, asyncio.SelectorEventLoop):
            assert isinstance(protocol, _BufferedStreamReaderProtocol)
        else:
            assert not isinstance(protocol, _BufferedStreamReaderProtocol)

        await transport.write(b"mqttium-buffered-stream")
        assert await transport.read(65536) == b"mqttium-buffered-stream"
        await transport.close()
    finally:
        server.close()
        await server.wait_closed()
