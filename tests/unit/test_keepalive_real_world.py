"""Keepalive detects dead peers without blaming application backpressure."""

from __future__ import annotations

import asyncio
import struct

from mqttium.api.async_client import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.errors import MQTTTimeoutError

_CONNACK = b"\x20\x02\x00\x00"
_PINGRESP = b"\xd0\x00"


def _publish_qos1(mid: int) -> bytes:
    body = struct.pack("!H", 3) + b"t/x" + struct.pack("!H", mid) + b"payload"
    return bytes((0x32, len(body))) + body


class _Server:
    """One-connection-at-a-time peer whose sockets are always closed."""

    def __init__(self, handler) -> None:  # noqa: ANN001
        self._handler = handler
        self._tasks: set[asyncio.Task[None]] = set()
        self._server: asyncio.AbstractServer | None = None

    async def __aenter__(self) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self._server.sockets[0].getsockname()[1]

    async def __aexit__(self, *exc_info: object) -> None:
        assert self._server is not None
        self._server.close()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        try:
            await self._handler(reader, writer)
        except (asyncio.IncompleteReadError, OSError):
            pass
        finally:
            writer.close()
            self._tasks.discard(task)


async def _answer_pings(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        header = await reader.readexactly(2)
        length = header[1]
        if length:
            await reader.readexactly(length)
        if header[0] == 0xC0:
            writer.write(_PINGRESP)


async def test_steady_publisher_detects_a_silent_peer() -> None:
    """A half-open link: the peer reads everything and answers nothing."""

    async def silent(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(4096)
        writer.write(_CONNACK)
        while await reader.read(65536):
            pass

    causes: list[BaseException | None] = []
    client = AsyncClient("half-open", keepalive=1, ping_timeout=1.0)
    client.on_disconnect = causes.append
    async with _Server(silent) as port:
        try:
            await client.connect("127.0.0.1", port)
            async with asyncio.timeout(8):
                while client.is_connected:
                    await client.publish("t/telemetry", b"x", qos=0)
                    await asyncio.sleep(0.1)
            await asyncio.sleep(0)
        finally:
            await client.disconnect()
    assert causes and isinstance(causes[0], MQTTTimeoutError)


async def test_full_iterator_does_not_time_out_a_live_connection() -> None:
    """The reader waits for application capacity; the broker still answers pings."""

    async def fill_then_answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(4096)
        writer.write(_CONNACK)
        writer.write(b"".join(_publish_qos1(mid) for mid in range(1, 6)))
        await _answer_pings(reader, writer)

    causes: list[BaseException | None] = []
    client = AsyncClient("slow-consumer", keepalive=1, ping_timeout=1.0, max_iterator_messages=1)
    client.on_disconnect = causes.append
    async with _Server(fill_then_answer) as port:
        await _consume_slowly(client, port, causes)


async def _consume_slowly(client: AsyncClient, port: int, causes: list) -> None:  # noqa: ANN001
    try:
        await client.connect("127.0.0.1", port)
        # Nobody consumes for several keepalive and ping windows.
        await asyncio.sleep(4.0)
        assert client.state is ConnectionState.CONNECTED
        assert causes == []
        received = []
        async with asyncio.timeout(5):
            async for message in client.messages():
                received.append(message.mid)
                if len(received) == 5:
                    break
        assert received == [1, 2, 3, 4, 5]
    finally:
        await client.disconnect()
