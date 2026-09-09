"""Shared asyncio stream transport primitives."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Protocol, runtime_checkable

from mqttium.transport.stats import TransportStats

_WRITE_BUFFER_HIGH_WATER = 64 * 1024


def write_buffer_needs_drain(writer: asyncio.StreamWriter) -> bool:
    transport = writer.transport
    return transport is not None and transport.get_write_buffer_size() > _WRITE_BUFFER_HIGH_WATER


@runtime_checkable
class AsyncTransport(Protocol):
    """What every transport can do. Receiving is a separate capability."""

    async def write(self, data: bytes) -> None: ...
    async def write_many(self, parts: list[bytes]) -> None: ...
    async def close(self) -> None: ...
    def is_closing(self) -> bool: ...


@runtime_checkable
class PullTransport(Protocol):
    """Receive capability: the caller asks for the next bytes."""

    async def read(self, n: int = 65536) -> bytes: ...


@runtime_checkable
class DecoderPushTransport(Protocol):
    """Receive capability: the transport commits into the decoder's storage.

    Exclusive with :class:`PullTransport`. A transport offering this has no
    ``read()`` at all, so neither capability check can misclassify it.
    """

    def attach_decoder(self, decoder: object) -> None: ...
    async def receive(self) -> bool: ...


class StreamTransportBase:
    """Write and lifecycle half, shared by both receive capabilities.

    Deliberately has no ``read()``: a push transport that inherited one would
    satisfy the pull capability structurally while refusing to honour it.
    """

    __slots__ = ("_reader", "_writer")

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._drain_if_needed()

    def write_nowait(self, data: bytes) -> bool:
        """Buffer one frame without awaiting when it fits below high-water.

        The eager path cannot await a drain, so it is admitted only when the
        *resulting* socket-buffer size stays at or below the local high-water
        mark. Checking the post-write size matters because a contiguous MQTT 5
        frame can be much larger than the publish payload segmentation threshold
        (for example, because of large properties).

        Returning ``False`` means nothing was written and the caller still owns
        the frame. It can therefore fall back to the writer task, which is able
        to await drain/backpressure. This method is deliberately absent from
        :class:`AsyncTransport`: it is an optimisation a transport may offer,
        not an obligation, and a transport whose write is more than a buffer
        append (WebSocket masks and may flush control frames first) must not
        provide it.
        """
        transport = self._writer.transport
        if (
            transport is not None
            and transport.get_write_buffer_size() + len(data) > _WRITE_BUFFER_HIGH_WATER
        ):
            return False
        self._writer.write(data)
        return True

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


class StreamTransport(StreamTransportBase):
    """Pull-capable asyncio stream transport."""

    __slots__ = ()

    async def read(self, n: int = 65536) -> bytes:
        return await self._reader.read(n)
