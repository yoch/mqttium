"""Shared asyncio stream transport primitives."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Protocol

from mqttium.transport.stats import TransportStats

_WRITE_BUFFER_HIGH_WATER = 64 * 1024


def write_buffer_needs_drain(writer: asyncio.StreamWriter) -> bool:
    transport = writer.transport
    return transport is not None and transport.get_write_buffer_size() > _WRITE_BUFFER_HIGH_WATER


class AsyncTransport(Protocol):
    async def write(self, data: bytes) -> None: ...
    async def write_many(self, parts: list[bytes]) -> None: ...
    async def read(self, n: int = 65536) -> bytes: ...
    async def close(self) -> None: ...
    def is_closing(self) -> bool: ...


class StreamTransport:
    """Common StreamReader/StreamWriter transport implementation."""

    __slots__ = ("_reader", "_writer")

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._drain_if_needed()

    async def write_many(self, parts: list[bytes]) -> None:
        if not parts:
            return
        if len(parts) == 1:
            await self.write(parts[0])
            return
        self._writer.writelines(parts)
        await self._drain_if_needed()

    async def drain(self) -> None:
        await self._writer.drain()

    async def read(self, n: int = 65536) -> bytes:
        return await self._reader.read(n)

    async def close(self) -> None:
        self._writer.close()
        with suppress(Exception):
            await self._writer.wait_closed()

    def is_closing(self) -> bool:
        return self._writer.is_closing()

    @property
    def pending_write_bytes(self) -> int:
        transport = self._writer.transport
        return 0 if transport is None else transport.get_write_buffer_size()

    def stats(self) -> TransportStats:
        return TransportStats(
            kind=type(self).__name__,
            closing=self.is_closing(),
            pending_write_bytes=self.pending_write_bytes,
            buffered_read_bytes=0,
            fragmented_read_bytes=0,
            pending_control_frames=0,
            pending_control_bytes=0,
        )

    async def _drain_if_needed(self) -> None:
        if write_buffer_needs_drain(self._writer):
            await self._writer.drain()
