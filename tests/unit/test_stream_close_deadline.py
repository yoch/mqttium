"""Stream shutdown must not depend on a peer continuing to read."""

import asyncio
import socket
from unittest.mock import AsyncMock, Mock

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.transport._stream import StreamTransport, StreamTransportBase
from mqttium.transport.websocket import WebSocketTransport
from tests.support import wait_until


@pytest.mark.parametrize(
    "transport_type", [StreamTransportBase, StreamTransport, WebSocketTransport]
)
@pytest.mark.parametrize("outcome", ["normal", "timeout", "cancel", "error"])
async def test_stream_close_owns_cleanup(monkeypatch, transport_type, outcome):
    # Exercise exactly one shutdown cause. A short real timeout can beat the
    # caller's cancellation on coarse or loaded event loops.
    monkeypatch.setattr(
        "mqttium.transport._stream._STREAM_CLOSE_TIMEOUT",
        0.0 if outcome == "timeout" else 60.0,
    )
    started = asyncio.Event()
    finished = asyncio.Event()
    aborted = asyncio.Event()

    async def wait_closed():
        started.set()
        try:
            if outcome == "error":
                raise ConnectionResetError("peer reset during close")
            if outcome != "normal":
                await aborted.wait()
        finally:
            finished.set()

    writer = Mock()
    writer.wait_closed = AsyncMock(side_effect=wait_closed)
    writer.transport.abort.side_effect = aborted.set
    transport = transport_type(asyncio.StreamReader(), writer)
    if isinstance(transport, WebSocketTransport):
        transport._recv_buf.extend(b"buffered")
        transport._pending_control.append(b"pending")
        transport._fragment = bytearray(b"fragment")
    task = asyncio.create_task(transport.close())
    try:
        await asyncio.wait_for(started.wait(), 1)
        if outcome == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await asyncio.wait_for(task, 1)
        writer.close.assert_called_once()
        assert finished.is_set()
        if outcome == "normal":
            writer.transport.abort.assert_not_called()
        else:
            writer.transport.abort.assert_called_once()
        if isinstance(transport, WebSocketTransport):
            assert not transport._recv_buf
            assert not transport._pending_control
            assert transport._fragment is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("mode", ["transport", "client", "cancel"])
async def test_real_nonreading_peer_cannot_hold_shutdown(monkeypatch, mode):
    public_disconnect = mode == "client"
    monkeypatch.setattr(
        "mqttium.transport._stream._STREAM_CLOSE_TIMEOUT", 60.0 if mode == "cancel" else 0.02
    )
    monkeypatch.setattr("mqttium.api.async_client._GRACEFUL_DISCONNECT_DRAIN_TIMEOUT", 0.02)
    peers = []
    peer_tasks = []
    paused = asyncio.Event()

    async def broker(reader, writer):
        peers.append(writer)
        if public_disconnect:
            # Consume the small CONNECT before refusing any further input.
            header = await reader.readexactly(2)
            assert header[0] == 0x10 and header[1] < 128
            await reader.readexactly(header[1])
            writer.write(b"\x20\x02\x00\x00")
            await writer.drain()
        writer.transport.pause_reading()
        paused.set()

    def connected(reader, writer):
        peer_tasks.append(asyncio.create_task(broker(reader, writer)))

    server = await asyncio.start_server(connected, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    transport = StreamTransport(reader, writer)
    client = AsyncClient("bounded-close", keepalive=0)

    async def factory(host, port, *, ssl=None):
        return transport

    client._transport_factory = factory
    try:
        if public_disconnect:
            await asyncio.wait_for(client.connect("unused"), 1)
        await asyncio.wait_for(paused.wait(), 1)
        writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        payload = b"x" * (8 * 1024 * 1024)
        if public_disconnect:
            client.publish_nowait("blocked", payload)
        else:
            # Proactor starts the first write directly as overlapped I/O; a
            # second write exercises its queued buffer, just like publish().
            middle = len(payload) // 2
            writer.write(payload[:middle])
            writer.write(payload[middle:])
        await wait_until(lambda: writer.transport.get_write_buffer_size() > 0)
        shutdown = asyncio.create_task(
            client.disconnect() if public_disconnect else transport.close()
        )
        try:
            if mode == "cancel":
                await wait_until(writer.is_closing)
                shutdown.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await shutdown
                # A cancelled caller must not poison the shared stream waiter.
                await asyncio.wait_for(transport.close(), 1)
            else:
                await asyncio.wait_for(shutdown, 2)
        finally:
            shutdown.cancel()
            await asyncio.gather(shutdown, return_exceptions=True)
        assert writer.is_closing()
        assert writer.transport.get_write_buffer_size() == 0
        if public_disconnect:
            assert client.state is ConnectionState.DISCONNECTED
            assert client._transport is None
            await wait_until(lambda: not any(client._running_tasks().values()))
            await asyncio.wait_for(client.disconnect(), 1)
    finally:
        writer.transport.abort()
        for peer in peers:
            peer.transport.abort()
        for task in peer_tasks:
            task.cancel()
        await asyncio.gather(*peer_tasks, return_exceptions=True)
        await client.disconnect()
        server.close()
        await server.wait_closed()
        # Run the connection_lost callbacks before the event loop closes.
        await asyncio.sleep(0)
