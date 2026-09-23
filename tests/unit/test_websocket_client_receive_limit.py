"""The public WebSocket path must honor the advertised MQTT receive limit."""

import asyncio
import base64
import hashlib
import struct

import pytest

from mqttium.api import AsyncClient
from mqttium.types import Properties
from tests.support import wait_until
from mqttium.enums import MQTTProtocolVersion
from mqttium.errors import PacketTooLargeError, ProtocolError
from mqttium.packets import PublishPacket
from mqttium.enums import QoS
from mqttium.transport.websocket import WebSocketTransport, _parse_frame


_FLOOR = 16 * 1024 * 1024


def _frame(payload, *, opcode=0x2, fin=True):
    """An unmasked server frame; only its header is built for large lengths."""
    first = (0x80 if fin else 0) | opcode
    return _header(len(payload), first) + payload


def _header(length, first=0x82):
    if length < 126:
        return bytes([first, length])
    if length < 1 << 16:
        return bytes([first, 126]) + struct.pack("!H", length)
    return bytes([first, 127]) + struct.pack("!Q", length)


class _Reader(asyncio.StreamReader):
    def __init__(self):
        super().__init__()
        self.waiting = asyncio.Event()

    async def read(self, n=-1):
        if not self._buffer:
            self.waiting.set()
        return await super().read(n)


class _Writer:
    def __init__(self, reader, protocol=MQTTProtocolVersion.MQTTv5):
        self.reader = reader
        self.closed = False
        self.transport = self
        self.connect = b""
        body = b"\x00\x00\x00" if protocol is MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
        self.connack = _frame(b"\x20" + bytes([len(body)]) + body)

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
                self.connect = bytes(parsed[2])
                self.reader.feed_data(self.connack)

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


async def _connected_ws(monkeypatch, protocol, packet_limit, on_message=None, **options):
    reader = _Reader()
    writer = _Writer(reader, protocol)

    async def open_connection(*args, **kwargs):
        return reader, writer

    monkeypatch.setattr(asyncio, "open_connection", open_connection)
    client = AsyncClient(
        "ws-bound", protocol=protocol, maximum_packet_size=packet_limit, keepalive=0, **options
    )
    causes = []
    client.on_disconnect = causes.append
    if on_message is not None:
        client.on_message = on_message
    await client.connect_ws("ws://unused/mqtt")
    await asyncio.wait_for(reader.waiting.wait(), 1)
    reader.waiting.clear()
    return client, reader, writer, causes


async def _closed_by(client, causes):
    await wait_until(lambda: not client.is_connected and causes)
    return causes[0]


@pytest.mark.parametrize(
    ("protocol", "packet_limit"),
    [
        (MQTTProtocolVersion.MQTTv5, None),
        (MQTTProtocolVersion.MQTTv5, 1024),
        (MQTTProtocolVersion.MQTTv5, 32 * 1024 * 1024),
        (MQTTProtocolVersion.MQTTv311, 1024),
        (MQTTProtocolVersion.MQTTv311, 32 * 1024 * 1024),
    ],
)
async def test_factory_decoder_and_advertised_limits_agree(monkeypatch, protocol, packet_limit):
    client, _, writer, _ = await _connected_ws(monkeypatch, protocol, packet_limit)
    try:
        effective = _FLOOR if packet_limit is None else packet_limit
        assert client._decoder.max_packet_size == effective
        assert client._transport._max_frame_size == max(_FLOOR, effective)
        advertised = b"\x27" + struct.pack("!I", effective)
        # MQTT 5 CONNECT always advertises the effective limit; MQTT 3.1.1 cannot.
        assert (advertised in writer.connect) is (protocol is MQTTProtocolVersion.MQTTv5)
    finally:
        await client.disconnect()


@pytest.mark.parametrize("packet_limit", [1024, 32 * 1024 * 1024])
async def test_one_byte_above_the_websocket_ceiling_is_refused_from_the_header(
    monkeypatch, packet_limit
):
    protocol = MQTTProtocolVersion.MQTTv5
    client, reader, _, causes = await _connected_ws(monkeypatch, protocol, packet_limit)
    try:
        ceiling = max(_FLOOR, packet_limit)
        reader.feed_data(_header(ceiling + 1))
        cause = await _closed_by(client, causes)
        assert isinstance(cause, ConnectionError)
        assert f"{ceiling + 1} exceeds max {ceiling}" in str(cause)
    finally:
        await client.disconnect()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_one_message_carries_several_packets_each_under_the_packet_limit(
    monkeypatch, protocol
):
    received = []
    client, reader, _, causes = await _connected_ws(
        monkeypatch,
        protocol,
        1024,
        on_message=lambda message: received.append(message.topic),
        message_delivery="callback",
    )
    try:
        packets = [
            PublishPacket(
                topic=f"t/{i}", payload=b"x" * 700, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
            ).encode(protocol)
            for i in range(2)
        ]
        assert all(len(packet) <= 1024 for packet in packets)
        assert sum(map(len, packets)) > 1024
        reader.feed_data(_frame(b"".join(packets)))
        await wait_until(lambda: len(received) == 2)
        assert received == ["t/0", "t/1"]
        assert client.is_connected and not causes
    finally:
        await client.disconnect()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_oversized_packet_inside_an_acceptable_frame_hits_the_packet_limit(
    monkeypatch, protocol
):
    client, reader, _, causes = await _connected_ws(monkeypatch, protocol, 1024)
    try:
        packet = PublishPacket(
            topic="big", payload=b"x" * 2000, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        ).encode(protocol)
        assert 1024 < len(packet) < _FLOOR
        reader.feed_data(_frame(packet))
        cause = await _closed_by(client, causes)
        assert isinstance(cause, PacketTooLargeError)
    finally:
        await client.disconnect()


def test_connect_properties_cannot_override_the_packet_limit():
    with pytest.raises(ProtocolError, match="dedicated constructor"):
        AsyncClient(
            "ws-bound",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=Properties({"maximum_packet_size": 64 * 1024 * 1024}),
        )


@pytest.mark.parametrize(
    ("sizes", "accepted"), [((512, 512), True), ((600, 600), False), ((1024, 1), False)]
)
async def test_fragmented_message_is_bounded_by_its_cumulative_size(sizes, accepted):
    reader = asyncio.StreamReader()
    transport = WebSocketTransport(reader, _Writer(reader), max_frame_size=1024)
    first, last = sizes
    reader.feed_data(_frame(b"a" * first, fin=False) + _frame(b"b" * last, opcode=0x0))
    if accepted:
        assert await transport.read() == b"a" * first + b"b" * last
    else:
        with pytest.raises(ConnectionError, match="fragmented message too large"):
            await transport.read()
