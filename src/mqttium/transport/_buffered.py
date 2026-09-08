"""Reusable-buffer asyncio protocol for allocation-stable socket reads."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
from typing import cast

# Receive capacity and application backpressure are intentionally independent.
# 80 KiB is large enough to avoid the systematic split observed for a 64-KiB
# MQTT PUBLISH on the ARM receive-path probes, while staying well below the
# allocation region where copying full received chunks began to show layout-
# dependent minor-fault regimes. The read queue keeps the historical 128/64 KiB
# high/low-water budget rather than scaling buffering with the receive chunk.
_READ_CHUNK = 80 * 1024
_READ_HIGH_WATER = 128 * 1024
_READ_LOW_WATER = 64 * 1024


class BufferedSocketProtocol(asyncio.BufferedProtocol):
    """Single-reader buffered protocol backed by one reusable receive buffer.

    CPython's normal selector ``Protocol.data_received`` path calls
    ``socket.recv(256 KiB)`` and therefore allocates a temporary bytes object of
    that requested size before shrinking it to the bytes actually received.
    ``BufferedProtocol`` instead lets the event loop call ``recv_into`` on a
    buffer owned by the protocol. We keep one 80-KiB receive buffer for the
    lifetime of the connection and copy only the bytes actually received into
    immutable chunks handed to the MQTT read loop.

    The public transport contract still exposes ``read(n) -> bytes``. MQTTium's
    reader currently asks for up to 256 KiB, but this protocol deliberately
    returns at most one received chunk per call instead of coalescing queued
    chunks. Each received byte therefore pays one explicit copy here rather than
    StreamReader's feed-data copy plus a second copy out of its bytearray.
    """

    __slots__ = (
        "_buffer",
        "_buffered_bytes",
        "_chunks",
        "_closed_waiter",
        "_connection_lost",
        "_eof",
        "_exception",
        "_loop",
        "_paused_reading",
        "_paused_writing",
        "_read_waiter",
        "_transport",
        "_write_waiters",
    )

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._buffer = bytearray(_READ_CHUNK)
        self._chunks: deque[bytes] = deque()
        self._buffered_bytes = 0
        self._transport: asyncio.Transport | None = None
        self._read_waiter: asyncio.Future[None] | None = None
        self._write_waiters: deque[asyncio.Future[None]] = deque()
        self._closed_waiter: asyncio.Future[None] = loop.create_future()
        self._paused_reading = False
        self._paused_writing = False
        self._connection_lost = False
        self._eof = False
        self._exception: BaseException | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast(asyncio.Transport, transport)

    def connection_lost(self, exc: Exception | None) -> None:
        self._eof = True
        self._connection_lost = True
        if exc is not None:
            self._exception = exc
        self._wake_reader()
        # Match asyncio.streams.FlowControlMixin: an already-waiting drain is
        # released on a clean close (or receives the transport exception), while
        # every drain started after connection_lost() fails deterministically.
        if self._paused_writing:
            self._wake_writers(exc)
        # Keep the close-notification future successful and surface the stored
        # transport exception from wait_closed(). A connection that nobody
        # explicitly waits closed then cannot emit an unobserved-Future warning.
        if not self._closed_waiter.done():
            self._closed_waiter.set_result(None)

    def eof_received(self) -> bool | None:
        # MQTTium treats transport EOF as terminal; returning false lets asyncio
        # close the write side too instead of preserving TCP half-close semantics
        # that the MQTT connection lifecycle never exposes.
        self._eof = True
        self._wake_reader()
        return None

    def get_buffer(self, sizehint: int) -> bytearray:
        del sizehint
        return self._buffer

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        chunk = bytes(memoryview(self._buffer)[:nbytes])
        self._chunks.append(chunk)
        self._buffered_bytes += nbytes
        transport = self._transport
        if (
            transport is not None
            and not self._paused_reading
            and self._buffered_bytes > _READ_HIGH_WATER
        ):
            transport.pause_reading()
            self._paused_reading = True
        self._wake_reader()

    def pause_writing(self) -> None:
        self._paused_writing = True

    def resume_writing(self) -> None:
        self._paused_writing = False
        self._wake_writers(None)

    @property
    def buffered_bytes(self) -> int:
        return self._buffered_bytes

    async def read(self, n: int = _READ_CHUNK) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            return await self._read_to_eof()

        while not self._chunks:
            if self._exception is not None:
                raise self._exception
            if self._eof:
                return b""
            if self._read_waiter is not None:
                raise RuntimeError("read() called while another read coroutine is waiting")
            waiter = self._loop.create_future()
            self._read_waiter = waiter
            try:
                await waiter
            finally:
                if self._read_waiter is waiter:
                    self._read_waiter = None

        if self._exception is not None:
            raise self._exception

        chunk = self._chunks.popleft()
        if len(chunk) <= n:
            self._buffered_bytes -= len(chunk)
            self._maybe_resume_reading()
            return chunk

        head = chunk[:n]
        self._chunks.appendleft(chunk[n:])
        self._buffered_bytes -= len(head)
        self._maybe_resume_reading()
        return head

    async def drain(self) -> None:
        # Keep the same observable contract as asyncio.streams.FlowControlMixin:
        # a drain begun after connection_lost() fails even when the close itself
        # was clean. Existing paused waiters are independent Futures so
        # cancellation of one producer cannot cancel another producer's drain.
        if self._connection_lost:
            raise ConnectionResetError("Connection lost")
        if not self._paused_writing:
            return
        waiter = self._loop.create_future()
        self._write_waiters.append(waiter)
        try:
            await waiter
        finally:
            self._write_waiters.remove(waiter)

    async def wait_closed(self) -> None:
        await self._closed_waiter
        if self._exception is not None:
            raise self._exception

    def _wake_reader(self) -> None:
        waiter = self._read_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _wake_writers(self, exc: BaseException | None) -> None:
        for waiter in self._write_waiters:
            if waiter.done():
                continue
            if exc is None:
                waiter.set_result(None)
            else:
                waiter.set_exception(exc)

    def _maybe_resume_reading(self) -> None:
        transport = self._transport
        if (
            transport is not None
            and self._paused_reading
            and self._buffered_bytes <= _READ_LOW_WATER
        ):
            transport.resume_reading()
            self._paused_reading = False

    async def _read_to_eof(self) -> bytes:
        parts: list[bytes] = []
        while True:
            chunk = await self.read(_READ_CHUNK)
            if not chunk:
                break
            parts.append(chunk)
        return b"".join(parts)

    def close_transport(self) -> None:
        transport = self._transport
        if transport is not None:
            transport.close()

    async def close(self) -> None:
        self.close_transport()
        with suppress(Exception):
            await self.wait_closed()
