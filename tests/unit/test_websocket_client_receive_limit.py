"""The public WebSocket path must honor the advertised MQTT receive limit."""

import asyncio
import base64
import hashlib
import struct

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.transport.websocket import _parse_frame


class _Reader(asyncio.StreamReader):
    def __init__(self):
        super().__init__()
        self.waiting = asyncio.Event()

    async def read(self, n=-1):
        if not self._buffer:
            self.waiting.set()
        return await super().read(n)


class _Writer:
    def __init__(self, reader):
        self.reader = reader
        self.closed = False
        self.transport = self

    def get_write_buffer_size(self):
        return 0

    def is_closing(self):
        return self.closed

    async def drain(self):
        pass

    def close(self):
        self.closed = True
        self.reader.feed_eof()

    async def wait_closed(self):
        pass

    def write(self, data):
        if data.startswith(b"GET "):
            key = next(
                line.split(b": ", 1)[1]
                for line in data.split(b"\r\n")
                if line.startswith(b"Sec-WebSocket-Key:")
            )
            accept = base64.b64encode(
                hashlib.sha1(
                    key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11", usedforsecurity=False
                ).digest()
            )
            self.reader.feed_data(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                b"Connection: Upgrade\r\nSec-WebSocket-Accept: "
                + accept
                + b"\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
            )
        else:
            parsed = _parse_frame(bytearray(data), 1 << 20, expect_masked=True)
            if parsed and parsed[1] == 2 and parsed[2][:1] == b"\x10":
                self.reader.feed_data(b"\x82\x05\x20\x03\x00\x00\x00")

    def writelines(self, parts):
        self.write(b"".join(parts))


@pytest.mark.parametrize("packet_limit", [1024, 32 * 1024 * 1024])
async def test_websocket_limit_covers_mqtt_packets_and_retains_coalescing(
    monkeypatch, packet_limit
):
    reader = _Reader()
    writer = _Writer(reader)

    async def open_connection(*args, **kwargs):
        return reader, writer

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    client = AsyncClient(
        "ws-bound",
        protocol=MQTTProtocolVersion.MQTTv5,
        maximum_packet_size=packet_limit,
        keepalive=0,
    )
    try:
        await client.connect_ws("ws://unused/mqtt")
        assert client._engine._sent_maximum_packet_size == packet_limit
        expected = max(16 * 1024 * 1024, packet_limit)
        assert client._transport._max_frame_size == expected
        await asyncio.wait_for(reader.waiting.wait(), 1)
        reader.waiting.clear()
        # A header alone is enough to test early length validation, without
        # allocating a giant payload. The reader must wait for its body.
        reader.feed_data(b"\x82\x7f" + struct.pack("!Q", expected))
        await asyncio.wait_for(reader.waiting.wait(), 1)
        assert client.is_connected
    finally:
        await client.disconnect()
