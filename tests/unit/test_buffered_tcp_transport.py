"""Allocation-stable cleartext TCP receive path."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.transport._buffered import (
    _READ_CHUNK,
    _READ_HIGH_WATER,
    _READ_LOW_WATER,
    BufferedSocketProtocol,
)
from mqttium.transport.tcp import TcpTransport, _BufferedTcpTransport


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


class _ClosingTransport(_FakeTransport):
    def is_closing(self) -> bool:
        return True

    def get_write_buffer_size(self) -> int:
        return 0


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


async def test_buffered_protocol_does_not_coalesce_queued_chunks() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]
    buffer = protocol.get_buffer(-1)
    buffer[:3] = b"abc"
    protocol.buffer_updated(3)
    buffer[:3] = b"def"
    protocol.buffer_updated(3)

    # read(n) promises at most n bytes, not coalescing. Preserving received
    # chunks avoids another copy and is an intentional difference from
    # StreamReader's aggregate bytearray.
    assert await protocol.read(6) == b"abc"
    assert await protocol.read(6) == b"def"


async def test_buffered_protocol_bounds_unread_data_with_independent_watermarks() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    fake = _FakeTransport()
    protocol.connection_made(fake)  # type: ignore[arg-type]

    assert _READ_LOW_WATER < _READ_HIGH_WATER
    assert _READ_CHUNK < _READ_HIGH_WATER

    # pause_reading() happens after the callback that crosses high-water, so the
    # unread queue may overshoot by at most one receive chunk.
    protocol.buffer_updated(_READ_CHUNK)
    assert fake.paused is False
    protocol.buffer_updated(_READ_CHUNK)
    assert _READ_HIGH_WATER < protocol.buffered_bytes <= _READ_HIGH_WATER + _READ_CHUNK
    assert fake.paused is True

    assert len(await protocol.read(_READ_CHUNK)) == _READ_CHUNK
    assert protocol.buffered_bytes > _READ_LOW_WATER
    assert fake.paused is True
    assert len(await protocol.read(_READ_CHUNK)) == _READ_CHUNK
    assert protocol.buffered_bytes <= _READ_LOW_WATER
    assert fake.paused is False


async def test_eof_preserves_buffered_data_then_becomes_terminal() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]
    buffer = protocol.get_buffer(-1)
    buffer[:5] = b"hello"
    protocol.buffer_updated(5)

    # MQTTium intentionally treats peer EOF as terminal rather than preserving
    # TCP half-close semantics; asyncio closes the transport when this is false.
    assert protocol.eof_received() is None
    assert await protocol.read(1024) == b"hello"
    assert await protocol.read(1024) == b""


async def test_clean_connection_loss_wakes_waiting_reader_with_eof() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]

    pending_read = asyncio.create_task(protocol.read())
    await asyncio.sleep(0)
    protocol.connection_lost(None)

    assert await pending_read == b""
    await protocol.wait_closed()


async def test_connection_loss_wakes_reader_and_reports_error_on_close() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]

    pending_read = asyncio.create_task(protocol.read())
    await asyncio.sleep(0)
    error = ConnectionResetError("peer reset")
    protocol.connection_lost(error)

    with pytest.raises(ConnectionResetError, match="peer reset"):
        await pending_read
    with pytest.raises(ConnectionResetError, match="peer reset"):
        await protocol.wait_closed()


async def test_cancelled_drain_does_not_cancel_another_waiter() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]
    protocol.pause_writing()

    first = asyncio.create_task(protocol.drain())
    second = asyncio.create_task(protocol.drain())
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    assert not second.done()
    protocol.resume_writing()
    await second


async def test_clean_connection_loss_releases_existing_drain_but_refuses_future_drain() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]
    protocol.pause_writing()

    pending = asyncio.create_task(protocol.drain())
    await asyncio.sleep(0)
    protocol.connection_lost(None)

    # asyncio.streams.FlowControlMixin releases a drain that was already
    # waiting on a clean close, while every drain begun afterwards fails.
    await pending
    with pytest.raises(ConnectionResetError, match="Connection lost"):
        await protocol.drain()


async def test_error_connection_loss_fails_pending_and_future_drains() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    protocol.connection_made(_FakeTransport())  # type: ignore[arg-type]
    protocol.pause_writing()

    pending = asyncio.create_task(protocol.drain())
    await asyncio.sleep(0)
    error = ConnectionResetError("peer reset")
    protocol.connection_lost(error)

    with pytest.raises(ConnectionResetError, match="peer reset"):
        await pending
    with pytest.raises(ConnectionResetError, match="Connection lost"):
        await protocol.drain()


async def test_transport_drain_yields_to_scheduled_connection_loss_when_closing() -> None:
    loop = asyncio.get_running_loop()
    protocol = BufferedSocketProtocol(loop)
    fake = _ClosingTransport()
    protocol.connection_made(fake)  # type: ignore[arg-type]
    transport = _BufferedTcpTransport(fake, protocol)  # type: ignore[arg-type]

    # StreamWriter.drain() yields once in this exact window because close() can
    # mark the transport closing before connection_lost() runs on the next turn.
    loop.call_soon(protocol.connection_lost, None)
    with pytest.raises(ConnectionResetError, match="Connection lost"):
        await transport.drain()


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
