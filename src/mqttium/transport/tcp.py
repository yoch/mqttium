"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
from contextlib import suppress
from typing import Any

from mqttium.transport._buffered import BufferedSocketProtocol
from mqttium.transport._stream import StreamTransport, _WRITE_BUFFER_HIGH_WATER
from mqttium.transport.stats import TransportStats


class TcpTransport(StreamTransport):
    """TCP transport with allocation-stable cleartext reads.

    TLS deliberately stays on ``asyncio.open_connection``: asyncio's
    ``SSLProtocol`` is already a ``BufferedProtocol`` and therefore receives
    encrypted bytes through ``recv_into``.  Plain TCP uses our reusable-buffer
    protocol so CPython's selector never allocates the normal 256-KiB temporary
    ``bytes`` object on every readable callback.
    """

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> TcpTransport:
        if ssl is not None:
            reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
            sock = writer.get_extra_info("socket")
            if sock is not None:
                with suppress(OSError):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return cls(reader, writer)

        loop = asyncio.get_running_loop()
        protocol = BufferedSocketProtocol(loop)
        transport, _ = await loop.create_connection(lambda: protocol, host, port)
        sock = transport.get_extra_info("socket")
        if sock is not None:
            with suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return _BufferedTcpTransport(transport, protocol)


class _BufferedTcpTransport(TcpTransport):
    """TcpTransport implementation backed by :class:`BufferedSocketProtocol`."""

    __slots__ = ("_protocol", "_transport")

    def __init__(
        self,
        transport: asyncio.Transport,
        protocol: BufferedSocketProtocol,
    ) -> None:
        # StreamTransport's reader/writer pair is intentionally absent on this
        # cleartext path. Every inherited operation is overridden below.
        self._transport = transport
        self._protocol = protocol

    async def write(self, data: bytes) -> None:
        self._transport.write(data)
        await self._drain_if_needed()

    def write_nowait(self, data: bytes) -> bool:
        if self._transport.get_write_buffer_size() + len(data) > _WRITE_BUFFER_HIGH_WATER:
            return False
        self._transport.write(data)
        return True

    async def write_many(self, parts: list[bytes]) -> None:
        if not parts:
            return
        if len(parts) == 1:
            await self.write(parts[0])
            return
        self._transport.writelines(parts)
        await self._drain_if_needed()

    async def drain(self) -> None:
        # StreamWriter checks the reader's exception before its flow-control
        # helper can replace it with a generic "Connection lost" error.
        exc = self._protocol.exception()
        if exc is not None:
            raise exc
        if self._transport.is_closing():
            # Match StreamWriter.drain(): close() may mark the transport closing
            # before protocol.connection_lost() runs on the next loop turn.
            # Yield once so the protocol can publish connection loss rather than
            # letting an unpaused drain return successfully in that window.
            await asyncio.sleep(0)
        await self._protocol.drain()

    async def read(self, n: int = 65536) -> bytes:
        return await self._protocol.read(n)

    async def close(self) -> None:
        self._transport.close()
        with suppress(Exception):
            await self._protocol.wait_closed()

    def is_closing(self) -> bool:
        return self._transport.is_closing()

    @property
    def pending_write_bytes(self) -> int:
        return self._transport.get_write_buffer_size()

    def stats(self) -> TransportStats:
        return TransportStats(
            kind="TcpTransport",
            closing=self.is_closing(),
            pending_write_bytes=self.pending_write_bytes,
            buffered_read_bytes=self._protocol.buffered_bytes,
            fragmented_read_bytes=0,
            pending_control_frames=0,
            pending_control_bytes=0,
        )

    async def _drain_if_needed(self) -> None:
        if self._transport.get_write_buffer_size() > _WRITE_BUFFER_HIGH_WATER:
            await self.drain()
