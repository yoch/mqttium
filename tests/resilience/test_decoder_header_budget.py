"""A slow peer cannot reserve a maximum packet from its length header alone."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from mqttium.api import AsyncClient
from mqttium.codec.buffer import RECEIVE_QUANTUM
from mqttium.codec.vbi import encode_vbi


async def test_large_length_announcement_only_allocates_for_receive_progress() -> None:
    send_header = asyncio.Event()
    send_body = asyncio.Event()
    finished = asyncio.Event()
    handler_done = asyncio.Event()
    header = b"\x30" + encode_vbi(16 * 1024 * 1024 - 5)

    async def peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.read(4096)
            writer.write(b"\x20\x02\x00\x00")
            await writer.drain()
            await send_header.wait()
            writer.write(header)
            await writer.drain()
            await send_body.wait()
            writer.write(b"\x00")
            await writer.drain()
            await finished.wait()
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()
            handler_done.set()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    client = AsyncClient("header-budget", keepalive=0)

    async def await_ingress(size: int) -> None:
        async with asyncio.timeout(2):
            while client._decoder.buffered < size:
                await asyncio.sleep(0.001)

    try:
        await client.connect("127.0.0.1", server.sockets[0].getsockname()[1], timeout=2)
        send_header.set()
        await await_ingress(len(header))
        send_body.set()
        await await_ingress(len(header) + 1)
        assert client.is_connected
        assert client._decoder.next_packet() is None
        assert client._decoder.capacity <= 2 * RECEIVE_QUANTUM
    finally:
        send_header.set()
        send_body.set()
        finished.set()
        await client.disconnect()
        server.close()
        await server.wait_closed()
        await asyncio.wait_for(handler_done.wait(), 2)
