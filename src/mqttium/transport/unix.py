"""Unix domain socket transport for MQTT."""

from __future__ import annotations

import asyncio

from mqttium.transport._stream import StreamTransport


class UnixSocketTransport(StreamTransport):
    """asyncio stream transport over an AF_UNIX socket."""

    @classmethod
    async def connect(cls, path: str) -> UnixSocketTransport:
        reader, writer = await asyncio.open_unix_connection(path)
        return cls(reader, writer)
