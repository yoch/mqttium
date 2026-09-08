"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
import sys
import weakref
from contextlib import suppress
from typing import Any

from mqttium.transport._push import DecoderPushProtocol, PushStreamTransport
from mqttium.transport._stream import AsyncTransport, StreamTransport

# #446's allocation-stable StreamReader fallback remains available for selector
# runtimes outside the deliberately narrow production direct-ingress scope.
_RECEIVE_BUFFER_SIZE = 80 * 1024


class _BufferedStreamReaderProtocol(asyncio.StreamReaderProtocol, asyncio.BufferedProtocol):
    """StreamReaderProtocol with one reusable 80-KiB receive buffer."""

    def __init__(self, reader: asyncio.StreamReader, *, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(reader, loop=loop)
        self._reader_ref = weakref.ref(reader)
        self._receive_buffer = memoryview(bytearray(_RECEIVE_BUFFER_SIZE))

    def get_buffer(self, sizehint: int) -> memoryview:
        del sizehint
        return self._receive_buffer

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        reader = self._reader_ref()
        if reader is not None:
            reader.feed_data(self._receive_buffer[:nbytes])


async def _open_buffered_selector_connection(
    host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = _BufferedStreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


def _direct_ingress_supported(ssl: Any, loop: asyncio.AbstractEventLoop) -> bool:
    """Whether the runtime is inside the evidence-backed production scope."""
    return (
        ssl is None
        and sys.implementation.name == "cpython"
        and isinstance(loop, asyncio.SelectorEventLoop)
        # Explicitly require a stdlib loop implementation.  Third-party loops
        # may mimic SelectorEventLoop without sharing its BufferedProtocol
        # callback/lifetime contract.
        and type(loop).__module__.startswith("asyncio.")
    )


class TcpTransport(StreamTransport):
    """TCP transport selecting direct decoder ingress only in its proven scope."""

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> AsyncTransport:
        loop = asyncio.get_running_loop()
        if _direct_ingress_supported(ssl, loop):
            transport: AsyncTransport = await _connect_push(loop, host, port)
        elif ssl is None and isinstance(loop, asyncio.SelectorEventLoop):
            # Preserve #446's reusable BufferedProtocol StreamReader path for a
            # selector runtime that is intentionally outside direct-ingress
            # promotion scope (notably non-CPython).
            reader, writer = await _open_buffered_selector_connection(host, port)
            transport = cls(reader, writer)
        else:
            # TLS receives through SSLProtocol; Proactor and third-party loops
            # stay on their established stream path.
            reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
            transport = cls(reader, writer)
        _set_nodelay(transport)
        return transport


async def _connect_push(
    loop: asyncio.AbstractEventLoop, host: str, port: int
) -> PushStreamTransport:
    reader = asyncio.StreamReader(loop=loop)
    protocol = DecoderPushProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return PushStreamTransport(reader, writer, protocol)


def _set_nodelay(transport: AsyncTransport) -> None:
    writer = getattr(transport, "_writer", None)
    if writer is None:
        return
    sock = writer.get_extra_info("socket")
    if sock is not None:
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
