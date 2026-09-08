"""Receive path that writes directly into the decoder's storage."""

from __future__ import annotations

import asyncio
import socket

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.transport._push import (
    _HIGH_WATER,
    _LOW_WATER,
    DecoderPushProtocol,
    PushStreamTransport,
)
from mqttium.transport.tcp import TcpTransport


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

    def get_extra_info(self, name: str, default: object = None) -> object:
        return default


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
    window = protocol.get_buffer(-1)
    assert len(window) >= len(data)
    window[: len(data)] = data
    protocol.buffer_updated(len(data))


async def test_reading_is_paused_until_a_decoder_is_attached() -> None:
    # get_buffer() may not return an empty buffer, so there must be no read at
    # all before the decoder that owns the storage is known.
    protocol, fake = _wire()
    assert fake.paused is True

    protocol.attach(IncrementalDecoder())
    assert fake.paused is False


async def test_receive_waits_for_new_bytes_rather_than_for_buffered_bytes() -> None:
    # The anti-livelock property. A partial frame leaves bytes in the decoder
    # but yields no packet; if receive() returned on "bytes are present" the
    # reader would spin without ever awaiting, and the event loop could never
    # deliver the rest of the frame.
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    frame = _publish(4000)
    _deliver(protocol, frame[:100])
    assert await transport.receive() is True
    assert decoder.buffered == 100
    assert decoder.next_packet() is None

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(transport.receive(), timeout=0.05)

    _deliver(protocol, frame[100:])
    assert await transport.receive() is True
    packet = decoder.next_packet()
    assert packet is not None
    assert len(packet.remaining) == len(frame) - 3


async def test_one_reader_wakeup_per_receive_callback() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    frame = _publish(64)
    wakeups = 0
    for _ in range(50):
        _deliver(protocol, frame)
        assert await transport.receive() is True
        wakeups += 1
        while decoder.next_packet() is not None:
            pass

    assert wakeups == protocol.received == 50


async def test_backpressure_pauses_above_high_water_and_resumes_below_low_water() -> None:
    decoder = IncrementalDecoder()
    protocol, fake = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    assert _LOW_WATER < _HIGH_WATER
    chunk = b"\x00" * (_LOW_WATER // 2)
    while decoder.buffered <= _HIGH_WATER:
        _deliver(protocol, chunk)
    assert fake.paused is True

    # Still paused while the reader is behind, even though it asked for more.
    assert await transport.receive() is True
    assert fake.paused is True

    # Once the reader has caught up, the next request for data resumes first.
    resumes_before = fake.resume_calls
    decoder.clear()
    _deliver(protocol, _publish(8))
    assert await transport.receive() is True
    assert fake.paused is False
    assert fake.resume_calls == resumes_before + 1


async def test_eof_delivers_buffered_bytes_before_becoming_terminal() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    _deliver(protocol, _publish(8))
    protocol.eof_received()

    assert await transport.receive() is True
    assert decoder.next_packet() is not None
    assert await transport.receive() is False


async def test_read_is_refused_so_a_stale_caller_cannot_hang() -> None:
    protocol, _ = _wire(IncrementalDecoder())
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="use receive"):
        await transport.read(1024)


async def test_cleartext_selector_connect_uses_the_push_path() -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    host, port = listener.getsockname()

    frame = _publish(32)
    accepted: list[socket.socket] = []

    async def serve() -> None:
        loop = asyncio.get_running_loop()
        listener.setblocking(False)
        conn, _ = await loop.sock_accept(listener)
        accepted.append(conn)
        await loop.sock_sendall(conn, frame)

    server = asyncio.create_task(serve())
    transport = await TcpTransport.connect(host, port)
    try:
        assert isinstance(transport, PushStreamTransport)
        decoder = IncrementalDecoder()
        transport.attach_decoder(decoder)
        assert await transport.receive() is True
        packet = decoder.next_packet()
        assert packet is not None
        assert len(packet.remaining) == len(frame) - 2
    finally:
        await server
        await transport.close()
        for conn in accepted:
            conn.close()
        listener.close()


async def test_tls_keeps_the_stream_path() -> None:
    # TLS receives through SSLProtocol, which never exposes the socket to a
    # BufferedProtocol, so it must keep feed().
    from mqttium.transport._stream import StreamTransport

    assert PushStreamTransport.__mro__[1] is StreamTransport
    assert not hasattr(StreamTransport, "attach_decoder")
    assert not hasattr(StreamTransport, "receive")
