"""Unix domain socket transport smoke tests."""

from __future__ import annotations

import asyncio
import socket
import sys
import tempfile
from pathlib import Path

import pytest

from mqttium.transport.unix import UnixSocketTransport


pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="asyncio Unix domain socket APIs are unavailable on Windows",
)


@pytest.mark.asyncio
async def test_unix_socket_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "mqtt.sock")
        received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            data = await reader.read(1024)
            received.set_result(data)
            writer.write(b"pong")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_unix_server(handle, path=path)
        try:
            transport = await UnixSocketTransport.connect(path)
            await transport.write(b"ping")
            await transport.drain()
            reply = await transport.read(1024)
            assert reply == b"pong"
            assert await received == b"ping"
            await transport.close()
        finally:
            server.close()
            await server.wait_closed()


def test_unix_socket_path_usable() -> None:
    assert hasattr(socket, "AF_UNIX")
