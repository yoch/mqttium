"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
from contextlib import suppress
from typing import Any

from mqttium.transport._push import DecoderPushProtocol, PushStreamTransport
from mqttium.transport._stream import StreamTransport


class TcpTransport(StreamTransport):
    """asyncio stream transport with TCP_NODELAY enabled when available."""

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> StreamTransport:
        loop = asyncio.get_running_loop()
        if ssl is None and isinstance(loop, asyncio.SelectorEventLoop):
            transport: StreamTransport = await _connect_push(loop, host, port)
        else:
            # TLS receives through SSLProtocol, which does not expose the raw
            # socket to a BufferedProtocol; Proactor and third-party loops keep
            # the mature stdlib path. Both still use the decoder's feed().
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


def _set_nodelay(transport: StreamTransport) -> None:
    sock = transport._writer.get_extra_info("socket")
    if sock is not None:
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
