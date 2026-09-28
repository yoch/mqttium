"""Messages the client already acknowledged are never discarded undelivered.

An automatically acknowledged QoS 1 message (and any QoS 0 message) has no
further protocol owner once its PUBACK leaves: the broker will not resend it.
Connection retirement, an explicit reconnect or ``disconnect()`` must therefore
still hand it to the application instead of dropping it with the connection.
"""

from __future__ import annotations

import asyncio
import struct

from mqttium.api.async_client import AsyncClient
from mqttium.types import Message

_CONNACK = b"\x20\x02\x00\x00"


def _publish_qos1(mid: int, topic: str = "t/x") -> bytes:
    name = topic.encode()
    payload = f"m{mid}".encode()
    body = struct.pack("!H", len(name)) + name + struct.pack("!H", mid) + payload
    return bytes((0x32, len(body))) + body


def _publish_qos0(index: int, topic: str = "t/x") -> bytes:
    name = topic.encode()
    payload = f"m{index}".encode()
    body = struct.pack("!H", len(name)) + name + payload
    return bytes((0x30, len(body))) + body


class _Broker:
    """Accept connections; each one runs the next script."""

    def __init__(self, *scripts) -> None:  # noqa: ANN002
        self._scripts = list(scripts)
        self.connections = 0
        self.pubacks = 0
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        script = self._scripts[min(self.connections, len(self._scripts) - 1)]
        self.connections += 1
        try:
            await reader.read(4096)  # CONNECT
            writer.write(_CONNACK)
            await script(self, reader, writer)
        except (OSError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def read_pubacks(self, reader: asyncio.StreamReader, count: int) -> None:
        while self.pubacks < count:
            header = await reader.readexactly(2)
            await reader.readexactly(header[1])
            if header[0] == 0x40:
                self.pubacks += 1


async def _idle(
    broker: _Broker, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    while await reader.read(4096):
        pass


async def _drain(messages, count: int) -> list[str]:  # noqa: ANN001
    received: list[str] = []
    async with asyncio.timeout(5):
        async for message in messages:
            received.append(message.payload.decode())
            if len(received) == count:
                break
    return received


async def test_explicit_reconnect_keeps_already_acknowledged_messages() -> None:
    async def ten_then_close(broker, reader, writer) -> None:  # noqa: ANN001
        writer.write(b"".join(_publish_qos1(mid) for mid in range(1, 11)))
        await broker.read_pubacks(reader, 10)

    broker = _Broker(ten_then_close, _idle)
    await broker.start()
    client = AsyncClient("keep-on-reconnect")
    try:
        await client.connect("127.0.0.1", broker.port)
        async with asyncio.timeout(5):
            while broker.connections < 1 or client.is_connected:
                await asyncio.sleep(0.01)
        messages = client.messages()
        first = await _drain(messages, 3)
        await client.connect("127.0.0.1", broker.port)
        rest = await _drain(client.messages(), 7)
    finally:
        await client.disconnect()
        await broker.stop()
    assert first + rest == [f"m{mid}" for mid in range(1, 11)]


async def test_disconnect_while_iterator_is_full_keeps_acknowledged_messages() -> None:
    async def ten_then_wait(broker, reader, writer) -> None:  # noqa: ANN001
        writer.write(b"".join(_publish_qos1(mid) for mid in range(1, 11)))
        await broker.read_pubacks(reader, 10)
        await _idle(broker, reader, writer)

    broker = _Broker(ten_then_wait)
    await broker.start()
    client = AsyncClient("keep-on-disconnect", max_iterator_messages=2)
    try:
        await client.connect("127.0.0.1", broker.port)
        async with asyncio.timeout(5):
            while broker.pubacks < 10:
                await asyncio.sleep(0.01)
        await client.disconnect()
        received = await _drain(client.messages(), 10)
    finally:
        await broker.stop()
    assert received == [f"m{mid}" for mid in range(1, 11)]


async def test_disconnect_between_callback_quanta_keeps_the_rest_of_the_lot() -> None:
    received: list[str] = []
    stopping: list[asyncio.Task[None]] = []

    async def burst(broker, reader, writer) -> None:  # noqa: ANN001
        writer.write(b"".join(_publish_qos0(index) for index in range(200)))
        await _idle(broker, reader, writer)

    broker = _Broker(burst)
    await broker.start()
    client = AsyncClient("keep-callback-lot", message_delivery="callback")

    def on_message(message: Message) -> None:
        received.append(message.payload.decode())
        if len(received) == 1:
            stopping.append(asyncio.ensure_future(client.disconnect()))

    client.on_message = on_message
    try:
        await client.connect("127.0.0.1", broker.port)
        async with asyncio.timeout(5):
            while not stopping:
                await asyncio.sleep(0.01)
            await stopping[0]
    finally:
        await broker.stop()
    assert received == [f"m{index}" for index in range(200)]
