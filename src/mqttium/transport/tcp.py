"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
import sys
from contextlib import suppress
from typing import Any

from mqttium.transport._push import DecoderPushProtocol, PushStreamTransport
from mqttium.transport._stream import StreamTransport, StreamTransportBase


class TcpTransport(StreamTransport):
    """asyncio stream transport with TCP_NODELAY enabled when available."""

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> StreamTransportBase:
        loop = asyncio.get_running_loop()
        if ssl is None and _is_stdlib_selector_loop(loop):
            transport: StreamTransportBase = await _connect_push(loop, host, port)
        else:
            # TLS receives through SSLProtocol, which does not expose the raw
            # socket to a BufferedProtocol; Proactor and third-party loops keep
            # the mature stdlib path. Both still use the decoder's feed().
            reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
            transport = cls(reader, writer)
        _set_nodelay(transport)
        return transport


def _is_stdlib_selector_loop(loop: asyncio.AbstractEventLoop) -> bool:
    """Whether this loop is a stdlib selector loop with the measured recv_into path.

    Deliberately narrower than ``isinstance(loop, SelectorEventLoop)``. A
    third-party subclass may install its own transport, and the allocator
    evidence behind this path is CPython-only, so neither is generalised to.
    """
    return (
        sys.implementation.name == "cpython"
        and isinstance(loop, asyncio.SelectorEventLoop)
        and type(loop).__module__.startswith("asyncio.")
    )


async def _connect_push(
    loop: asyncio.AbstractEventLoop, host: str, port: int
) -> PushStreamTransport:
    reader = asyncio.StreamReader(loop=loop)
    protocol = DecoderPushProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return PushStreamTransport(reader, writer, protocol)


def _set_nodelay(transport: StreamTransportBase) -> None:
    sock = transport._writer.get_extra_info("socket")
    if sock is not None:
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
