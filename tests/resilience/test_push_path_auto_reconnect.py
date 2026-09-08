"""Automatic reconnect over the real cleartext transport.

Every other automatic-reconnect test uses a transport double, so none of them
reach `TcpTransport.connect()` and therefore none exercise the push receive
path across a reconnect. That path re-attaches the decoder to a *new* protocol
on every connection while reusing the same decoder object, which is exactly the
state a reconnect disturbs.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.transport._push import PushStreamTransport

CONNACK = bytes((0x20, 2, 0, 0))


def _publish(topic: bytes, payload: bytes) -> bytes:
    body = len(topic).to_bytes(2, "big") + topic + payload
    out = bytearray([0x30])
    remaining = len(body)
    while True:
        digit = remaining % 128
        remaining //= 128
        out.append(digit | 0x80 if remaining else digit)
        if not remaining:
            break
    return bytes(out) + body


class _DroppingBroker:
    """Answers CONNECT, publishes, then drops the first connection outright."""

    def __init__(self, payload_size: int) -> None:
        self.connections = 0
        self.payload_size = payload_size
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            with suppress(TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(self._server.wait_closed(), timeout=2)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        first = self.connections == 1
        try:
            await asyncio.wait_for(reader.read(4096), timeout=5)  # CONNECT
            writer.write(CONNACK)
            # Split the frame so the reconnect lands mid-stream on attempt one.
            frame = _publish(b"t/auto", b"p" * self.payload_size)
            if first:
                writer.write(frame[: len(frame) // 2])
                await writer.drain()
                writer.close()  # abrupt drop, mid-frame
                return
            writer.write(frame)
            await writer.drain()
            await asyncio.sleep(3)
        except (OSError, TimeoutError, asyncio.CancelledError):
            pass
        finally:
            try:
                writer.close()
            except OSError:
                pass


@pytest.mark.parametrize("payload_size", [16, 300_000])
async def test_auto_reconnect_reattaches_the_push_decoder(payload_size: int) -> None:
    # 300_000 also drives the reconnect through a frame larger than the receive
    # high water, i.e. the incomplete-head-frame path.
    broker = _DroppingBroker(payload_size)
    port = await broker.start()
    received: list[bytes] = []
    delivered = asyncio.Event()

    client = AsyncClient(
        client_id="push-reconnect",
        protocol=MQTTProtocolVersion.MQTTv311,
        reconnect=ReconnectPolicy(enabled=True, initial_delay=0.01, max_delay=0.1),
    )

    def on_message(message: object) -> None:
        received.append(message.payload)  # type: ignore[attr-defined]
        delivered.set()

    client.on_message = on_message
    # Windows defaults to ProactorEventLoop, which keeps the read()+feed()
    # path; the reconnect assertions hold on both, the push ones only here.
    push_path = isinstance(asyncio.get_running_loop(), asyncio.SelectorEventLoop)
    try:
        await client.connect("127.0.0.1", port, timeout=5)
        assert isinstance(client._transport, PushStreamTransport) is push_path
        first_decoder = client._decoder

        await asyncio.wait_for(delivered.wait(), timeout=10)

        assert broker.connections >= 2, "the client did not reconnect on its own"
        assert received == [b"p" * payload_size]
        # Same decoder object across connections; the truncated first frame must
        # not have leaked into the second connection's stream.
        assert client._decoder is first_decoder
        transport = client._transport
        if push_path:
            assert isinstance(transport, PushStreamTransport)
            assert transport.receive_stats()["recv_callbacks"] >= 1
    finally:
        await client.disconnect()
        await broker.stop()
