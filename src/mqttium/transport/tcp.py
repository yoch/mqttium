"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
import weakref
from contextlib import suppress
from typing import Any

from mqttium.transport._stream import StreamTransport

_RECEIVE_BUFFER_SIZE = 80 * 1024


class _BufferedStreamReaderProtocol(asyncio.StreamReaderProtocol, asyncio.BufferedProtocol):
    """StreamReaderProtocol with one reusable receive buffer.

    This is the local, deliberately small variant of the idea explored in
    CPython gh-85451 / PR #21446. Modern selector transports already switch to
    ``recv_into()`` whenever the protocol is a ``BufferedProtocol``, so MQTTium
    only needs to provide the buffer and forward the received slice to the
    existing ``StreamReader``. All EOF, drain, close and read-backpressure
    semantics remain owned by asyncio streams.

    ``sizehint`` is intentionally ignored. The old CPython review called out
    that a dynamic first hint could otherwise accidentally make a reused buffer
    permanently small; MQTTium instead uses the measured 80-KiB receive size.
    """

    def __init__(self, reader: asyncio.StreamReader, *, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(reader, loop=loop)
        # StreamReaderProtocol itself deliberately keeps a weak reference to its
        # reader. Keep our own typed weakref rather than reaching into the
        # protocol's private `_stream_reader` property or introducing a strong
        # reference that changes the stdlib lifecycle.
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
    host: str,
    port: int,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Build normal asyncio streams on top of a BufferedProtocol selector read path."""

    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = _BufferedStreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


class TcpTransport(StreamTransport):
    """asyncio stream transport with allocation-stable selector reads."""

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> TcpTransport:
        loop = asyncio.get_running_loop()
        if ssl is None and isinstance(loop, asyncio.SelectorEventLoop):
            reader, writer = await _open_buffered_selector_connection(host, port)
        else:
            # TLS already has buffered receive semantics in asyncio SSLProtocol.
            # Proactor and third-party loops stay on the mature stdlib stream
            # path: current Proactor feeds BufferedProtocol through an internal
            # 64-KiB buffer, so forcing this adapter there mainly adds a copy.
            reader, writer = await asyncio.open_connection(host, port, ssl=ssl)

        sock = writer.get_extra_info("socket")
        if sock is not None:
            with suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(reader, writer)
