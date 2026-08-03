"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
from contextlib import suppress
from typing import Any

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
    ) -> TcpTransport:
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            with suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(reader, writer)
