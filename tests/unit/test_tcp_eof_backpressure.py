"""TCP EOF must not discard input queued behind application backpressure."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame


@pytest.mark.parametrize("version", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("half_close", [False, True], ids=["close", "half-close"])
async def test_tcp_eof_preserves_input_behind_delivery_backpressure(
    monkeypatch: pytest.MonkeyPatch,
    version: MQTTProtocolVersion,
    half_close: bool,
) -> None:
    loop = asyncio.get_running_loop()
    delivery_blocked = asyncio.Event()
    eof_seen = asyncio.Event()
    connected = asyncio.Event()
    disconnected = asyncio.Event()
    handlers: list[asyncio.Task[None]] = []
    original_connect = loop.create_connection

    async def observed_connect(*args, **kwargs):
        transport, protocol = await original_connect(*args, **kwargs)
        original_eof = type(protocol).eof_received

        def observed_eof(self):
            result = original_eof(self)
            if self is protocol:
                # Wake after the transport acts on the return value, so a
                # premature close is observable before delivery resumes.
                loop.call_soon(eof_seen.set)
            return result

        monkeypatch.setattr(type(protocol), "eof_received", observed_eof)
        return transport, protocol

    monkeypatch.setattr(loop, "create_connection", observed_connect)

    async def broker(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            decoder = IncrementalDecoder()
            while (connect := decoder.next_packet()) is None:
                data = await reader.read(4096)
                assert data, "client closed before CONNECT"
                decoder.feed(data)
            assert connect.packet_type is PacketType.CONNECT
            body = b"\x00\x00" + (b"\x00" if version is MQTTProtocolVersion.MQTTv5 else b"")
            writer.write(encode_frame(PacketType.CONNACK, 0, body))
            await writer.drain()
            await connected.wait()
            publications = [
                PublishPacket(
                    topic="eof/t", payload=payload, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
                ).encode(version)
                for payload in (b"one", b"two", b"three")
            ]
            writer.write(publications[0] + publications[1])
            await writer.drain()
            await delivery_blocked.wait()
            # This final chunk arrives while the second message is blocked on
            # the one-message iterator queue. It must be read even after FIN.
            tail = publications[2]
            if version is MQTTProtocolVersion.MQTTv5:
                tail += encode_frame(PacketType.DISCONNECT, 0, b"\x89\x00")
            writer.write(tail)
            await writer.drain()
            if half_close:
                writer.write_eof()
                await reader.read()
        finally:
            writer.close()
            await writer.wait_closed()

    def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        handlers.append(asyncio.create_task(broker(reader, writer)))

    client = AsyncClient(
        client_id="tcp-eof-backpressure",
        protocol=version,
        keepalive=0,
        message_delivery="iterator",
        max_pending_messages=1,
        delivery_timeout=10,
    )
    client.on_disconnect = lambda _exc: disconnected.set()
    original_put = client._messages.put

    async def observed_put(item):
        assert client._messages.full()
        delivery_blocked.set()
        await original_put(item)

    monkeypatch.setattr(client._messages, "put", observed_put)
    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        async with asyncio.timeout(10):
            await client.connect("127.0.0.1", server.sockets[0].getsockname()[1])
            connected.set()
            await eof_seen.wait()
            received = [message.payload async for message in client.messages()]
            await disconnected.wait()
            assert received == [b"one", b"two", b"three"]
            assert not client.is_connected
            if version is MQTTProtocolVersion.MQTTv5:
                info = client._last_disconnect_info
                assert info is not None and info.from_broker
                assert info.reason_code == 0x89
    finally:
        await client.disconnect()
        server.close()
        for handler in handlers:
            if not handler.done():
                handler.cancel()
        results = await asyncio.gather(*handlers, return_exceptions=True)
        await server.wait_closed()
        for result in results:
            if isinstance(result, Exception):
                raise result
