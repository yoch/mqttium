"""Receive path that writes straight into the decoder's own storage.

RC13 and both buffered-receive prototypes copy every received byte at least
twice before it reaches the decoder: kernel to a receive buffer, then receive
buffer to the decoder's buffer. This path removes the second copy by handing
`socket.recv_into()` a window carved out of the decoder's slab, so bytes land
where the parser already expects them.

Only the read direction changes. Writes, drain, close and the connection
lifecycle stay with asyncio streams, exactly as in the StreamReader variant.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Protocol

from mqttium.transport._stream import StreamTransportBase
from mqttium.transport.stats import TransportStats

# The reader task drains in bounded batches, so the slab is allowed to hold
# more than one receive window before the transport is asked to stop.
_HIGH_WATER = 192 * 1024
_LOW_WATER = 64 * 1024
_MIN_WINDOW = 16 * 1024


class DecoderSink(Protocol):
    """The part of the decoder a receiving protocol needs.

    Structural, like `AsyncTransport`, so the transport layer does not depend on
    the codec package.
    """

    def writable_window(self, preferred: int = ...) -> memoryview: ...
    def commit(self, nbytes: int) -> None: ...
    def head_frame_ready(self) -> bool: ...
    @property
    def buffered(self) -> int: ...


class DecoderPushProtocol(asyncio.StreamReaderProtocol, asyncio.BufferedProtocol):
    """StreamReaderProtocol whose received bytes go to a decoder, not a StreamReader.

    `sizehint` is ignored: the window size is the decoder's business, and it
    naturally tracks how much of the slab is free, which gives a large quantum
    when the reader is keeping up and a smaller one when it is falling behind.
    """

    def __init__(self, reader: asyncio.StreamReader, *, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(reader, loop=loop)
        self._loop = loop
        self._sink: DecoderSink | None = None
        self._scratch: memoryview | None = None
        self._read_transport: asyncio.Transport | None = None
        self._waiter: asyncio.Future[None] | None = None
        self._received = 0
        self._received_bytes = 0
        self._pauses = 0
        self._resumes = 0
        self._eof = False
        self._exception: BaseException | None = None
        self._paused_reading = False

    # -- lifecycle ---------------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        assert isinstance(transport, asyncio.Transport)
        self._read_transport = transport
        # There is nowhere to receive into until a decoder is attached, and
        # get_buffer() may not return an empty buffer, so stay paused until then.
        transport.pause_reading()
        self._paused_reading = True

    def attach(self, sink: DecoderSink) -> None:
        self._sink = sink
        self.resume_if_drained()

    def detach(self) -> None:
        """Stop routing into the decoder.

        A reconnect hands the same decoder to a new connection, so a late
        callback from the old one must not be able to commit stale bytes into
        it. Reading is already stopped by close(); this makes it safe even if a
        callback is still in flight.
        """
        self._sink = None
        transport = self._read_transport
        if not self._paused_reading and transport is not None and not transport.is_closing():
            transport.pause_reading()
            self._paused_reading = True

    def eof_received(self) -> bool | None:
        self._eof = True
        self._wake()
        return super().eof_received()

    def connection_lost(self, exc: Exception | None) -> None:
        if exc is not None:
            self._exception = exc
        self._eof = True
        self._wake()
        super().connection_lost(exc)

    # -- receive -----------------------------------------------------------

    def get_buffer(self, sizehint: int) -> memoryview:
        del sizehint
        sink = self._sink
        if sink is None:
            # Detached, or not yet attached. Reading is paused in both cases, so
            # this should be unreachable -- but get_buffer() may not return an
            # empty buffer, so hand over scratch space rather than fail the
            # connection. Whatever lands in it is dropped by buffer_updated().
            scratch = self._scratch
            if scratch is None:
                scratch = self._scratch = memoryview(bytearray(_MIN_WINDOW))
            return scratch
        return sink.writable_window(_MIN_WINDOW)

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        sink = self._sink
        if sink is None:
            return
        sink.commit(nbytes)
        self._received += 1
        self._received_bytes += nbytes
        if (
            not self._paused_reading
            and self._read_transport is not None
            and sink.buffered > _HIGH_WATER
            # Never pause for a single incomplete frame that merely crossed the
            # watermark: the reader cannot consume it, so nothing would ever
            # drain the slab and the connection would hang. A frame may legally
            # be as large as max_packet_size.
            and sink.head_frame_ready()
        ):
            # In push mode the reader no longer applies backpressure by simply
            # not calling read(), so it has to be stated explicitly.
            self._read_transport.pause_reading()
            self._paused_reading = True
            self._pauses += 1
        self._wake()

    @property
    def received(self) -> int:
        return self._received

    @property
    def at_eof(self) -> bool:
        return self._eof

    @property
    def exception(self) -> BaseException | None:
        return self._exception

    @property
    def received_bytes(self) -> int:
        return self._received_bytes

    @property
    def pauses(self) -> int:
        return self._pauses

    @property
    def resumes(self) -> int:
        return self._resumes

    @property
    def sink(self) -> DecoderSink | None:
        return self._sink

    async def wait_for_data(self) -> None:
        if self._waiter is not None:
            # Overwriting the waiter would orphan the first caller, which then
            # hangs with no diagnostic. One reader owns this transport.
            raise RuntimeError("receive() called while another receive coroutine is waiting")
        waiter = self._loop.create_future()
        self._waiter = waiter
        try:
            await waiter
        finally:
            if self._waiter is waiter:
                self._waiter = None

    def resume_if_drained(self) -> None:
        sink = self._sink
        if (
            self._paused_reading
            and sink is not None
            and self._read_transport is not None
            and not self._read_transport.is_closing()
            # Resume once the reader has caught up, or as soon as the head frame
            # stops being consumable -- the rest of it can only arrive from the
            # socket we paused.
            and (sink.buffered <= _LOW_WATER or not sink.head_frame_ready())
        ):
            self._read_transport.resume_reading()
            self._paused_reading = False
            self._resumes += 1

    def _wake(self) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)


class PushStreamTransport(StreamTransportBase):
    """Stream transport whose reads are delivered into an attached decoder."""

    __slots__ = ("_protocol", "_seen", "_resumptions", "_waits")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: DecoderPushProtocol,
    ) -> None:
        super().__init__(reader, writer)
        self._protocol = protocol
        self._seen = 0
        self._resumptions = 0
        self._waits = 0

    def attach_decoder(self, decoder: DecoderSink) -> None:
        self._protocol.attach(decoder)

    async def receive(self) -> bool:
        """Wait until new bytes have landed in the decoder. False once EOF is reached.

        The wait is edge-triggered on the receive counter, never level-triggered
        on "the decoder holds bytes". Holding bytes is not the same as holding a
        complete frame: on a partial frame a level condition would return
        immediately, the reader would decode nothing, never await, and so never
        let the event loop deliver the rest of the frame.

        A connection error is raised rather than reported as end of stream, and
        it wins over bytes that arrived before it -- the same ordering
        ``StreamReader.read()`` gives, which the client's error taxonomy and
        reconnect policy depend on. A clean EOF is different: the last receive
        generation is still delivered before this returns False.
        """
        protocol = self._protocol
        # The reader has finished its batch by the time it asks for more.
        protocol.resume_if_drained()
        if protocol.exception is not None:
            raise protocol.exception
        while protocol.received == self._seen:
            if protocol.at_eof:
                return False
            self._waits += 1
            await protocol.wait_for_data()
            if protocol.exception is not None:
                raise protocol.exception
        self._seen = protocol.received
        self._resumptions += 1
        return True

    def receive_stats(self) -> dict[str, int]:
        """Counters for diagnosing the receive path.

        ``reader_resumptions`` counts times ``receive()`` handed the reader work;
        ``reader_waits`` counts times it actually had to suspend. Both are
        independent of ``recv_callbacks``, which is the point: under a
        level-triggered wait the reader resumes without new bytes, so
        resumptions climb without bound while callbacks stay flat. Coalescing is
        the opposite and equally visible -- several callbacks land before the
        reader is scheduled, so resumptions fall below callbacks.
        """
        protocol = self._protocol
        return {
            "recv_callbacks": protocol.received,
            "recv_bytes": protocol.received_bytes,
            "reader_resumptions": self._resumptions,
            "reader_waits": self._waits,
            "pause_count": protocol.pauses,
            "resume_count": protocol.resumes,
        }

    def stats(self) -> TransportStats:
        base = super().stats()
        sink = self._protocol.sink
        # The base class assumes bytes wait in a StreamReader; here they wait in
        # the decoder, and reporting 0 would hide real inbound backlog.
        buffered = 0 if sink is None else sink.buffered
        return replace(base, buffered_read_bytes=buffered)

    async def close(self) -> None:
        self._protocol.detach()
        await super().close()
