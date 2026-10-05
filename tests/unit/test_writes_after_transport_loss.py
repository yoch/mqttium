"""Writes into a closed or lost transport fail instead of being dropped.

asyncio silently discards writes on a transport whose connection is lost and
logs ``socket.send() raised exception`` once the count passes a threshold. The
client must retire the connection through its writer failure path instead.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.errors import MQTTError
from mqttium.transport._stream import StreamTransport

_WARNING = "socket.send() raised exception"


async def _server(handler) -> tuple[asyncio.AbstractServer, int]:  # noqa: ANN001
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def test_stream_transport_refuses_writes_after_loss(caplog: pytest.LogCaptureFixture) -> None:
    accepted: asyncio.Future[asyncio.StreamWriter] = asyncio.get_running_loop().create_future()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.set_result(writer)

    server, port = await _server(handler)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        transport = StreamTransport(reader, writer)
        (await accepted).transport.abort()
        writer.transport.abort()
        await asyncio.sleep(0)

        caplog.set_level(logging.WARNING, logger="asyncio")
        assert transport.write_nowait(b"x") is False
        for _ in range(10):
            with pytest.raises(ConnectionResetError):
                await transport.write(b"x")
            with pytest.raises(ConnectionResetError):
                await transport.write_many([b"a", b"b"])
        assert _WARNING not in caplog.text
    finally:
        server.close()
        await server.wait_closed()


async def test_client_burst_after_broker_reset_logs_no_dropped_writes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(4096)  # CONNECT
        writer.write(b"\x20\x02\x00\x00")  # CONNACK accepted

        async def consume() -> None:
            while await reader.read(65536):
                pass

        consumer = asyncio.create_task(consume())
        await asyncio.sleep(0.005)
        writer.transport.abort()  # connection reset under a publishing burst
        consumer.cancel()

    server, port = await _server(handler)
    caplog.set_level(logging.WARNING, logger="asyncio")
    client = AsyncClient("writes-after-loss")
    try:
        await client.connect("127.0.0.1", port)
        for index in range(12_000):
            try:
                await client.publish("t/burst", b"x" * 512, qos=1)
            except (MQTTError, OSError):
                # Refusals after the loss are expected; this test only checks
                # that nothing is written into the dead transport.
                pass
            if index % 20 == 0:
                await asyncio.sleep(0)
        await asyncio.sleep(0.3)
    finally:
        await client.disconnect()
        server.close()
        await server.wait_closed()
    assert _WARNING not in caplog.text
