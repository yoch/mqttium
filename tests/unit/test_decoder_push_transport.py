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
from mqttium.transport._stream import (
    AsyncTransport,
    DecoderPushTransport,
    PullTransport,
    StreamTransport,
    StreamTransportBase,
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


def test_a_push_transport_is_not_pull_capable() -> None:
    # A capability that passes a structural check but refuses its own operation
    # is not a contract. A push transport has no read() at all.
    assert not hasattr(PushStreamTransport, "read")
    assert issubclass(PushStreamTransport, StreamTransportBase)
    assert not issubclass(PushStreamTransport, StreamTransport)


async def test_the_two_receive_capabilities_are_exclusive() -> None:
    protocol, _ = _wire(IncrementalDecoder())
    push = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    pull = StreamTransport(asyncio.StreamReader(), None)  # type: ignore[arg-type]

    assert isinstance(push, DecoderPushTransport)
    assert not isinstance(push, PullTransport)
    assert isinstance(pull, PullTransport)
    assert not isinstance(pull, DecoderPushTransport)

    # Both still satisfy the common contract.
    for transport in (push, pull):
        assert isinstance(transport, AsyncTransport)


async def test_cleartext_connect_uses_the_push_path_only_on_a_selector_loop() -> None:
    # Windows defaults to ProactorEventLoop, which feeds BufferedProtocol
    # through its own internal buffer, so the push path must not be selected
    # there. Assert whichever branch this platform is supposed to take.
    loop = asyncio.get_running_loop()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    host, port = listener.getsockname()

    frame = _publish(32)
    accepted: list[socket.socket] = []

    async def serve() -> None:
        listener.setblocking(False)
        conn, _ = await loop.sock_accept(listener)
        accepted.append(conn)
        await loop.sock_sendall(conn, frame)

    server = asyncio.create_task(serve())
    transport = await TcpTransport.connect(host, port)
    decoder = IncrementalDecoder()
    try:
        if isinstance(loop, asyncio.SelectorEventLoop):
            assert isinstance(transport, PushStreamTransport)
            transport.attach_decoder(decoder)
            assert await transport.receive() is True
        else:
            assert not isinstance(transport, DecoderPushTransport)
            decoder.feed(await transport.read(65536))
        packet = decoder.next_packet()
        assert packet is not None
        assert len(packet.remaining) == len(frame) - 2
    finally:
        await server
        await transport.close()
        for conn in accepted:
            conn.close()
        listener.close()


def test_tls_and_websocket_keep_the_pull_capability() -> None:
    # They receive through SSLProtocol / their own framing, which never expose
    # the socket to a BufferedProtocol, so they must keep read() + feed().
    assert not hasattr(StreamTransport, "attach_decoder")
    assert not hasattr(StreamTransport, "receive")
    assert hasattr(StreamTransport, "read")


async def test_detach_stops_a_torn_down_connection_writing_into_the_decoder() -> None:
    # A reconnect hands the same decoder to the next connection. A callback
    # still in flight on the old one must not commit stale bytes into it.
    decoder = IncrementalDecoder()
    old, old_transport = _wire(decoder)
    _deliver(old, _publish(8))
    assert decoder.buffered > 0

    decoder.clear()
    old.detach()
    assert old_transport.paused is True

    # The old connection reports more data; it must be dropped, not committed.
    window = old.get_buffer(-1)
    window[:4] = b"\x30\x02ab"
    old.buffer_updated(4)
    assert decoder.buffered == 0

    # And the decoder still works for whoever owns it now.
    new, _ = _wire(decoder)
    _deliver(new, _publish(8))
    assert decoder.next_packet() is not None


async def test_closing_the_transport_detaches_it() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)

    class _ClosableWriter:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            return None

    writer = _ClosableWriter()
    transport = PushStreamTransport(asyncio.StreamReader(), writer, protocol)  # type: ignore[arg-type]
    await transport.close()

    assert writer.closed is True
    window = protocol.get_buffer(-1)
    window[:4] = b"\x30\x02ab"
    protocol.buffer_updated(4)
    assert decoder.buffered == 0


async def test_push_capability_is_declared_not_guessed() -> None:
    # The client selects the push path on this capability, so the classes that
    # must not offer it have to keep failing the check.
    protocol, _ = _wire(IncrementalDecoder())
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    assert isinstance(transport, DecoderPushTransport)

    plain = StreamTransport(asyncio.StreamReader(), None)  # type: ignore[arg-type]
    assert not isinstance(plain, DecoderPushTransport)


async def test_a_single_frame_larger_than_high_water_does_not_deadlock() -> None:
    # An MQTT frame may legally be as large as max_packet_size. Pausing merely
    # because an *incomplete* frame crossed the watermark stops the only source
    # that could complete it, and nothing can ever drain the slab.
    decoder = IncrementalDecoder()
    protocol, fake = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    frame = _publish(_HIGH_WATER + 64 * 1024)
    head = frame[: _HIGH_WATER + 1024]
    offset = 0
    while offset < len(head):
        window = protocol.get_buffer(-1)
        take = min(len(window), len(head) - offset)
        window[:take] = head[offset : offset + take]
        protocol.buffer_updated(take)
        offset += take

    assert decoder.buffered > _HIGH_WATER
    assert decoder.next_packet() is None  # the frame is not complete yet
    assert fake.paused is False  # must still be able to receive the remainder

    assert await transport.receive() is True

    rest = frame[len(head) :]
    offset = 0
    while offset < len(rest):
        window = protocol.get_buffer(-1)
        take = min(len(window), len(rest) - offset)
        window[:take] = rest[offset : offset + take]
        protocol.buffer_updated(take)
        offset += take

    assert await asyncio.wait_for(transport.receive(), timeout=1.0) is True
    packet = decoder.next_packet()
    assert packet is not None
    assert len(packet.remaining) == len(frame) - 4


async def test_a_paused_connection_resumes_when_the_head_frame_stops_being_ready() -> None:
    # Backlog of small frames pauses the socket; once the reader drains down to
    # a partial head frame, the remainder can only come from that same socket.
    decoder = IncrementalDecoder()
    protocol, fake = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    small = _publish(8 * 1024)
    while decoder.buffered <= _HIGH_WATER:
        _deliver(protocol, small)
    assert fake.paused is True

    partial = _publish(4096)[:100]
    _deliver(protocol, partial)

    while decoder.next_packet() is not None:
        pass
    assert decoder.buffered == len(partial)
    assert decoder.head_frame_ready() is False

    assert await transport.receive() is True
    assert fake.paused is False


def test_malformed_head_counts_as_ready_so_the_error_can_surface() -> None:
    # Otherwise a peer could hang the connection with a bad length prefix.
    decoder = IncrementalDecoder()
    decoder.feed(b"\x30\x80\x80\x80\x80\x80")
    assert decoder.head_frame_ready() is True


async def test_a_connection_error_is_raised_not_reported_as_eof() -> None:
    # The client's error taxonomy and reconnect policy distinguish a peer reset
    # from a clean close. StreamReader.read() raises here, so receive() must
    # too, or every transport failure would look like an orderly shutdown.
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    protocol.connection_lost(ConnectionResetError("peer reset"))

    with pytest.raises(ConnectionResetError, match="peer reset"):
        await transport.receive()


async def test_a_connection_error_wins_over_bytes_that_arrived_before_it() -> None:
    # Matches StreamReader ordering: set_exception makes subsequent reads raise
    # even when bytes were buffered first.
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    _deliver(protocol, _publish(8))
    protocol.connection_lost(ConnectionResetError("peer reset"))

    with pytest.raises(ConnectionResetError):
        await transport.receive()


async def test_an_error_arriving_while_the_reader_waits_is_raised() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    waiting = asyncio.ensure_future(transport.receive())
    await asyncio.sleep(0)
    protocol.connection_lost(OSError("link went down"))

    with pytest.raises(OSError, match="link went down"):
        await asyncio.wait_for(waiting, timeout=1.0)


async def test_a_clean_eof_still_delivers_the_last_generation() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    _deliver(protocol, _publish(8))
    protocol.connection_lost(None)

    assert await transport.receive() is True
    assert decoder.next_packet() is not None
    assert await transport.receive() is False


async def test_a_second_concurrent_reader_fails_loudly() -> None:
    # Overwriting the waiter used to orphan the first caller, which then hung
    # with no diagnostic at all. One reader owns this transport.
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    first = asyncio.ensure_future(transport.receive())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="another receive coroutine"):
        await transport.receive()

    _deliver(protocol, _publish(8))
    assert await asyncio.wait_for(first, timeout=1.0) is True


async def test_receive_stats_expose_the_wakeup_to_callback_ratio() -> None:
    # The level-triggered bug shows up as wakeups far exceeding callbacks, so
    # the ratio is worth being able to read at runtime.
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    for _ in range(20):
        _deliver(protocol, _publish(64))
        assert await transport.receive() is True
        while decoder.next_packet() is not None:
            pass

    stats = transport.receive_stats()
    assert stats["recv_callbacks"] == 20
    assert stats["reader_resumptions"] == 20
    assert stats["recv_bytes"] > 0
    assert stats["pause_count"] == 0


async def test_stats_report_bytes_waiting_in_the_decoder() -> None:
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)

    class _Writer:
        transport = None

        def is_closing(self) -> bool:
            return False

    transport = PushStreamTransport(asyncio.StreamReader(), _Writer(), protocol)  # type: ignore[arg-type]
    _deliver(protocol, _publish(4096)[:200])

    assert transport.stats().buffered_read_bytes == 200


async def test_receive_stats_distinguish_coalescing_from_spurious_resumption() -> None:
    # The counter must be independent of the callback count, or it cannot show
    # either failure. Many callbacks before the reader runs is coalescing:
    # resumptions fall below callbacks. The level-triggered bug is the mirror
    # image -- resumptions climb while callbacks stay flat.
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]

    for _ in range(10):
        _deliver(protocol, _publish(64))
    assert await transport.receive() is True

    stats = transport.receive_stats()
    assert stats["recv_callbacks"] == 10
    assert stats["reader_resumptions"] == 1
    assert stats["reader_waits"] == 0

    decoded = 0
    while decoder.next_packet() is not None:
        decoded += 1
    assert decoded == 10  # coalescing loses nothing
