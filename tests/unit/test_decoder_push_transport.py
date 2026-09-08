"""Production decoder-ingress transport invariants."""

from __future__ import annotations

import asyncio
import socket
import sys

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.transport._push import DecoderPushProtocol, PushStreamTransport
from mqttium.transport._stream import DecoderPushTransport, PullTransport, StreamTransport
from mqttium.transport.tcp import TcpTransport, _direct_ingress_supported


class _FakeTransport(asyncio.Transport):
    def __init__(self) -> None:
        super().__init__()
        self.paused = False
        self.pause_calls = 0
        self.resume_calls = 0
        self.closing = False

    def pause_reading(self) -> None:
        self.paused = True
        self.pause_calls += 1

    def resume_reading(self) -> None:
        self.paused = False
        self.resume_calls += 1

    def is_closing(self) -> bool:
        return self.closing


def _publish(payload_size: int) -> bytes:
    body = b"\x00\x01t" + b"p" * payload_size
    remaining = len(body)
    out = bytearray([0x30])
    while True:
        digit = remaining % 128
        remaining //= 128
        out.append(digit | 0x80 if remaining else digit)
        if not remaining:
            break
    return bytes(out) + body


def _wire(decoder: IncrementalDecoder | None = None):
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = DecoderPushProtocol(reader, loop=loop)
    fake = _FakeTransport()
    protocol.connection_made(fake)
    if decoder is not None:
        protocol.attach(decoder)
    return protocol, fake


def _deliver(protocol: DecoderPushProtocol, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        window = protocol.get_buffer(-1)
        take = min(len(window), len(data) - offset)
        window[:take] = data[offset : offset + take]
        window.release()
        protocol.buffer_updated(take)
        offset += take


async def test_push_is_an_honest_receive_capability_not_a_fake_stream_read() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    assert isinstance(transport, DecoderPushTransport)
    assert not isinstance(transport, PullTransport)
    assert not hasattr(transport, "read")

    plain = StreamTransport(asyncio.StreamReader(), None)  # type: ignore[arg-type]
    assert isinstance(plain, PullTransport)
    assert not isinstance(plain, DecoderPushTransport)


async def test_reading_stays_paused_until_exact_decoder_generation_is_attached() -> None:
    protocol, fake = _wire()
    assert fake.paused is True
    decoder = IncrementalDecoder()
    protocol.attach(decoder)
    assert fake.paused is False


async def test_receive_is_generation_gated_not_level_triggered_by_partial_bytes() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    frame = _publish(4000)

    _deliver(protocol, frame[:100])
    assert await transport.receive() is True
    assert decoder.next_packet() is None
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(transport.receive(), timeout=0.03)

    _deliver(protocol, frame[100:])
    assert await transport.receive() is True
    assert decoder.next_packet() is not None


async def test_clean_eof_drains_last_generation_and_reset_wins_over_bytes() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    _deliver(protocol, _publish(32))
    protocol.connection_lost(None)
    assert await transport.receive() is True
    assert decoder.next_packet() is not None
    assert await transport.receive() is False

    decoder.clear()
    protocol2, _ = _wire(decoder)
    transport2 = PushStreamTransport(asyncio.StreamReader(), None, protocol2)  # type: ignore[arg-type]
    _deliver(protocol2, _publish(32))
    protocol2.connection_lost(ConnectionResetError("peer reset"))
    with pytest.raises(ConnectionResetError, match="peer reset"):
        await transport2.receive()


async def test_detach_drops_late_old_generation_bytes_before_reconnect() -> None:
    decoder = IncrementalDecoder()
    old, old_transport = _wire(decoder)
    decoder.clear()
    old.detach()
    assert old_transport.paused is True

    window = old.get_buffer(-1)
    window[:4] = b"\x30\x02ab"
    window.release()
    old.buffer_updated(4)
    assert decoder.buffered == 0

    new, _ = _wire(decoder)
    _deliver(new, _publish(16))
    assert decoder.next_packet() is not None


async def test_large_incomplete_head_is_not_paused_into_deadlock() -> None:
    decoder = IncrementalDecoder(max_packet_size=2 * 1024 * 1024)
    protocol, fake = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    frame = _publish(900 * 1024)
    split = 600 * 1024

    _deliver(protocol, frame[:split])
    assert decoder.buffered > 512 * 1024
    assert decoder.next_packet() is None
    assert fake.paused is False
    assert await transport.receive() is True

    _deliver(protocol, frame[split:])
    assert await transport.receive() is True
    packet = decoder.next_packet()
    assert packet is not None
    assert fake.pause_calls >= 1


async def test_product_tcp_selects_direct_only_inside_promoted_runtime_scope() -> None:
    loop = asyncio.get_running_loop()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    host, port = listener.getsockname()
    accepted: list[socket.socket] = []
    frame = _publish(32)

    async def serve() -> None:
        conn, _ = await loop.sock_accept(listener)
        accepted.append(conn)
        await loop.sock_sendall(conn, frame)

    server = asyncio.create_task(serve())
    transport = await TcpTransport.connect(str(host), int(port))
    decoder = IncrementalDecoder()
    try:
        supported = _direct_ingress_supported(None, loop)
        assert supported is (
            sys.implementation.name == "cpython"
            and isinstance(loop, asyncio.SelectorEventLoop)
            and type(loop).__module__.startswith("asyncio.")
        )
        if supported:
            assert isinstance(transport, DecoderPushTransport)
            assert not isinstance(transport, PullTransport)
            transport.attach_decoder(decoder)
            assert await transport.receive() is True
        else:
            assert isinstance(transport, PullTransport)
            decoder.feed(await transport.read(65536))
        assert decoder.next_packet() is not None
    finally:
        await server
        await transport.close()
        for conn in accepted:
            conn.close()
        listener.close()
