"""Executable conformance checks for MQTT over WebSocket, and its handshake.

Same convention as ``test_conformance_qos_flows.py``: one test per numbered
statement and specification version, with the statement quoted from the
vendored index as its docstring. Each test runs the client against a WebSocket
server written here from RFC 6455, over a real local socket.

The handshake tests at the end carry no MQTT statement: they are the evidence
for the WebSocket entry of the threat model.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import Awaitable, Callable

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.transport.websocket import (
    _MAX_HANDSHAKE_BYTES,
    WebSocketTransport,
    _build_handshake_request,
    _read_handshake_response,
    _validate_handshake_response,
)
from mqttium.types import Message
from tests.support import wait_until

V311 = MQTTProtocolVersion.MQTTv311
V5 = MQTTProtocolVersion.MQTTv5
_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_TEXT, _BINARY = 0x1, 0x2


def _accept(key: str) -> str:
    digest = hashlib.sha1((key + _GUID).encode(), usedforsecurity=False).digest()
    return base64.b64encode(digest).decode("ascii")


def _frame(opcode: int, payload: bytes) -> bytes:
    """One unfragmented, unmasked server frame; payloads here stay under 126 bytes."""
    assert len(payload) < 126
    return bytes((0x80 | opcode, len(payload))) + payload


class _Peer:
    """The server side of one WebSocket connection."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.request = b""
        self.opcodes: list[int] = []
        self.closed_by_client = asyncio.Event()

    async def upgrade(self) -> None:
        while b"\r\n\r\n" not in self.request:
            self.request += await self.reader.read(4096)
        key = next(
            line.split(b":", 1)[1].strip().decode()
            for line in self.request.split(b"\r\n")
            if line.lower().startswith(b"sec-websocket-key:")
        )
        self.writer.write(
            (
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                f"Connection: Upgrade\r\nSec-WebSocket-Accept: {_accept(key)}\r\n"
                "Sec-WebSocket-Protocol: mqtt\r\n\r\n"
            ).encode()
        )
        await self.writer.drain()

    async def read_frame(self) -> bytes:
        """The unmasked payload of the next client frame."""
        head = await self.reader.readexactly(2)
        self.opcodes.append(head[0] & 0x0F)
        assert head[1] & 0x80, "client frames must be masked"
        size = head[1] & 0x7F
        if size == 126:
            size = int.from_bytes(await self.reader.readexactly(2), "big")
        mask = await self.reader.readexactly(4)
        payload = await self.reader.readexactly(size)
        return bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))

    async def send(self, *frames: bytes) -> None:
        self.writer.write(b"".join(frames))
        await self.writer.drain()

    async def wait_closed_by_client(self) -> None:
        while await self.reader.read(4096):
            pass
        self.closed_by_client.set()


def _connack(protocol: MQTTProtocolVersion) -> bytes:
    return encode_frame(PacketType.CONNACK, 0, b"\x00\x00" + (b"\x00" if protocol is V5 else b""))


def _publish(protocol: MQTTProtocolVersion, index: int) -> bytes:
    return PublishPacket(
        topic=f"in/{index}", payload=b"payload", qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    ).encode(protocol)


async def _run(
    protocol: MQTTProtocolVersion,
    script: Callable[[_Peer], Awaitable[None]],
    received: list[Message] | None = None,
) -> tuple[_Peer, AsyncClient]:
    """Connect a client to a server that upgrades, then follows ``script``."""
    peers: list[_Peer] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = _Peer(reader, writer)
        peers.append(peer)
        try:
            await peer.upgrade()
            await script(peer)
            await peer.wait_closed_by_client()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = AsyncClient(
        client_id="websocket", protocol=protocol, keepalive=0, message_delivery="callback"
    )
    if received is not None:
        client.on_message = received.append
    async with server:
        try:
            await client.connect_ws(f"ws://127.0.0.1:{port}/mqtt", timeout=2.0)
        except MQTTError:
            pass
        await wait_until(lambda: bool(peers), timeout=2.0)
        peer = peers[0]
        try:
            await _settle(peer, client, received)
        finally:
            if client.is_connected:
                await client.disconnect()
            await asyncio.wait_for(peer.closed_by_client.wait(), timeout=2.0)
    return peer, client


async def _settle(peer: _Peer, client: AsyncClient, received: list[Message] | None) -> None:
    if received is None:
        await asyncio.wait_for(peer.closed_by_client.wait(), timeout=2.0)
    else:
        await wait_until(lambda: len(received) >= 3 or not client.is_connected, timeout=2.0)


async def _packets_travel_in_binary_frames(protocol: MQTTProtocolVersion) -> None:
    async def text_frame_after_connack(peer: _Peer) -> None:
        connect = await peer.read_frame()
        assert connect[0] == 0x10
        await peer.send(_frame(_BINARY, _connack(protocol)))
        # A complete, valid MQTT packet, but in a text frame.
        await peer.send(_frame(_TEXT, _publish(protocol, 0)))

    peer, client = await _run(protocol, text_frame_after_connack, None)
    # Everything the client sent was binary, and it closed the connection
    # rather than read the text frame.
    assert peer.opcodes and set(peer.opcodes) <= {_BINARY, 0x8}
    assert peer.closed_by_client.is_set()
    assert not client.is_connected
    await _both_write_paths_send_binary_frames()


async def _both_write_paths_send_binary_frames() -> None:
    """The transport has two ways to write, one packet or several at once."""
    received: list[tuple[int, bytes]] = []
    done = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = _Peer(reader, writer)
        try:
            await peer.upgrade()
            # Payloads past 125 bytes use the extended length that
            # read_frame decodes; anything else would desynchronise it.
            while sum(len(payload) for _opcode, payload in received) < 600:
                payload = await peer.read_frame()
                received.append((peer.opcodes[-1], payload))
            done.set()
            await peer.wait_closed_by_client()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        transport = await WebSocketTransport.connect(f"ws://127.0.0.1:{port}/mqtt", timeout=2.0)
        try:
            await transport.write(b"a" * 200)
            await transport.write_many([b"b" * 200, b"c" * 200])
            await asyncio.wait_for(done.wait(), timeout=2.0)
        finally:
            await transport.close()
    assert {opcode for opcode, _payload in received} == {_BINARY}
    assert b"".join(payload for _opcode, payload in received) == (
        b"a" * 200 + b"b" * 200 + b"c" * 200
    )


async def _packets_are_not_aligned_on_frames(protocol: MQTTProtocolVersion) -> None:
    async def split_and_joined(peer: _Peer) -> None:
        await peer.read_frame()
        connack = _connack(protocol)
        first, second, third = (_publish(protocol, index) for index in range(3))
        await peer.send(
            # CONNACK cut after its first byte.
            _frame(_BINARY, connack[:1]),
            _frame(_BINARY, connack[1:]),
            # Two packets and the start of a third in one frame.
            _frame(_BINARY, first + second + third[:4]),
            _frame(_BINARY, third[4:]),
        )

    received: list[Message] = []
    _peer, _client = await _run(protocol, split_and_joined, received)
    assert [message.topic for message in received] == ["in/0", "in/1", "in/2"]


async def _client_offers_the_mqtt_subprotocol(protocol: MQTTProtocolVersion) -> None:
    async def connack_only(peer: _Peer) -> None:
        await peer.read_frame()
        await peer.send(_frame(_BINARY, _connack(protocol)))
        await peer.send(_frame(_BINARY, b"".join(_publish(protocol, i) for i in range(3))))

    peer, _client = await _run(protocol, connack_only, [])
    offered = [
        line.split(b":", 1)[1]
        for line in peer.request.split(b"\r\n")
        if line.lower().startswith(b"sec-websocket-protocol:")
    ]
    assert len(offered) == 1
    assert "mqtt" in [name.strip() for name in offered[0].decode().split(",")]


async def test_mqtt311_6_0_0_1_packets_travel_in_binary_frames() -> None:
    """[MQTT-6.0.0-1] MQTT Control Packets MUST be sent in WebSocket binary data
    frames. If any other type of data frame is received the recipient MUST close the
    Network Connection"""
    await _packets_travel_in_binary_frames(V311)


async def test_mqtt5_6_0_0_1_packets_travel_in_binary_frames() -> None:
    """[MQTT-6.0.0-1] MQTT Control Packets MUST be sent in WebSocket binary data
    frames. If any other type of data frame is received the recipient MUST close the
    Network Connection"""
    await _packets_travel_in_binary_frames(V5)


async def test_mqtt311_6_0_0_2_packets_are_not_aligned_on_frames() -> None:
    """[MQTT-6.0.0-2] A single WebSocket data frame can contain multiple or partial
    MQTT Control Packets. The receiver MUST NOT assume that MQTT Control Packets are
    aligned on WebSocket frame boundaries"""
    await _packets_are_not_aligned_on_frames(V311)


async def test_mqtt5_6_0_0_2_packets_are_not_aligned_on_frames() -> None:
    """[MQTT-6.0.0-2] A single WebSocket data frame can contain multiple or partial
    MQTT Control Packets. The receiver MUST NOT assume that MQTT Control Packets are
    aligned on WebSocket frame boundaries"""
    await _packets_are_not_aligned_on_frames(V5)


async def test_mqtt311_6_0_0_3_client_offers_the_mqtt_subprotocol() -> None:
    """[MQTT-6.0.0-3] The client MUST include “mqtt” in the list of WebSocket Sub
    Protocols it offers"""
    await _client_offers_the_mqtt_subprotocol(V311)


async def test_mqtt5_6_0_0_3_client_offers_the_mqtt_subprotocol() -> None:
    """[MQTT-6.0.0-3] The Client MUST include “mqtt” in the list of WebSocket Sub
    Protocols it offers"""
    await _client_offers_the_mqtt_subprotocol(V5)


# --- Handshake validation (threat model, WebSocket endpoint) -----------------


def _response(key: str, *, drop: str | None = None, accept: str | None = None) -> bytes:
    headers = {
        "Upgrade": "websocket",
        "Connection": "Upgrade",
        "Sec-WebSocket-Accept": accept if accept is not None else _accept(key),
    }
    if drop is not None:
        del headers[drop]
    lines = ["HTTP/1.1 101 Switching Protocols", *(f"{k}: {v}" for k, v in headers.items())]
    return "\r\n".join(lines).encode()


def test_handshake_accepts_only_the_answer_to_its_own_key() -> None:
    _validate_handshake_response(_response("key-a"), "key-a")
    with pytest.raises(ConnectionError, match="accept key"):
        _validate_handshake_response(_response("key-b"), "key-a")
    with pytest.raises(ConnectionError, match="accept key"):
        _validate_handshake_response(_response("key-a", accept=""), "key-a")
    with pytest.raises(ConnectionError, match="accept key"):
        _validate_handshake_response(_response("key-a", drop="Sec-WebSocket-Accept"), "key-a")


@pytest.mark.parametrize("header", ["Upgrade", "Connection"])
def test_handshake_requires_the_upgrade_headers(header: str) -> None:
    with pytest.raises(ConnectionError, match=header):
        _validate_handshake_response(_response("key-a", drop=header), "key-a")


def test_handshake_requires_status_101() -> None:
    ok = _response("key-a")
    for status in (b"HTTP/1.1 200 OK", b"HTTP/1.1 403 Forbidden", b"garbage"):
        with pytest.raises(ConnectionError, match="handshake failed"):
            _validate_handshake_response(
                ok.replace(b"HTTP/1.1 101 Switching Protocols", status), "key-a"
            )


async def test_handshake_response_size_is_bounded() -> None:
    reader = asyncio.StreamReader()
    # Headers that never end: the limit, not the peer, ends the read.
    reader.feed_data(
        b"HTTP/1.1 101 Switching Protocols\r\n" + b"X-Pad: " + b"a" * _MAX_HANDSHAKE_BYTES
    )
    with pytest.raises(ConnectionError, match="too large"):
        await _read_handshake_response(reader, 2.0)


async def test_a_server_that_fails_the_handshake_is_not_connected_to() -> None:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        # A well-formed 101 that answers some other key.
        writer.write(_response("not-the-key-that-was-sent") + b"\r\n\r\n")
        await writer.drain()
        await reader.read()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        with pytest.raises(ConnectionError, match="accept key"):
            await WebSocketTransport.connect(f"ws://127.0.0.1:{port}/mqtt", timeout=2.0)


def test_handshake_request_offers_only_mqtt() -> None:
    request = _build_handshake_request("broker.example", 80, "/mqtt", "key", None)
    assert request.count(b"Sec-WebSocket-Protocol:") == 1
    assert b"\r\nSec-WebSocket-Protocol: mqtt\r\n" in request
