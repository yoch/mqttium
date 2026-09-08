"""Selector receive path that writes directly into decoder-owned storage."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Protocol

from mqttium.transport._stream import _StreamTransportBase
from mqttium.transport.stats import TransportStats

# Flow-control remains deliberately wider than #446: the pre-promotion ablation
# showed that 512/128 KiB materially helps the direct architecture.  Receive
# quantum is now a separate adaptive decision rather than an allocation floor.
_HIGH_WATER = 512 * 1024
_LOW_WATER = 128 * 1024
_INITIAL_WINDOW = 64 * 1024
_BULK_WINDOW = 256 * 1024
_PROMOTE_AFTER_FULL_WINDOWS = 2


class DecoderSink(Protocol):
    def writable_window(self, preferred: int = ...) -> memoryview: ...
    def commit(self, nbytes: int) -> None: ...
    def head_frame_ready(self) -> bool: ...
    @property
    def buffered(self) -> int: ...


class DecoderPushProtocol(asyncio.StreamReaderProtocol, asyncio.BufferedProtocol):
    """BufferedProtocol whose socket bytes land directly in a decoder slab.

    The connection starts with a 64-KiB receive window so a tiny/idle connection
    does not pay #447's 512-KiB floor.  Two substantially-filled callbacks
    promote that *connection* to the measured 256-KiB bulk quantum.  Promotion
    changes only future writable-window requests; decoder allocation policy is
    independent and can release oversized frame growth later.
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
        self._window_target = _INITIAL_WINDOW
        self._offered = 0
        self._full_window_streak = 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        if not isinstance(transport, asyncio.Transport):
            raise TypeError("decoder ingress requires asyncio.Transport")
        self._read_transport = transport
        # The decoder is attached by AsyncClient only after connection state and
        # packet-size limits are reset for this exact transport generation.
        transport.pause_reading()
        self._paused_reading = True

    def attach(self, sink: DecoderSink) -> None:
        if self._sink is not None and self._sink is not sink:
            raise RuntimeError("decoder ingress already attached")
        self._sink = sink
        self.resume_if_drained()

    def detach(self) -> None:
        """Prevent any late callback from committing into a later generation."""
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

    def get_buffer(self, sizehint: int) -> memoryview:
        del sizehint
        sink = self._sink
        if sink is None:
            # Reading is paused while detached.  A scratch window makes a stale
            # callback fail-safe rather than turning lifecycle timing into a
            # RuntimeError from asyncio's non-empty-buffer requirement.
            scratch = self._scratch
            if scratch is None:
                scratch = self._scratch = memoryview(bytearray(_INITIAL_WINDOW))
            self._offered = len(scratch)
            return scratch
        window = sink.writable_window(self._window_target)
        self._offered = len(window)
        return window

    def _observe_window_fill(self, nbytes: int) -> None:
        if self._window_target >= _BULK_WINDOW:
            return
        offered = self._offered
        if offered >= _INITIAL_WINDOW and nbytes * 4 >= offered * 3:
            self._full_window_streak += 1
            if self._full_window_streak >= _PROMOTE_AFTER_FULL_WINDOWS:
                self._window_target = _BULK_WINDOW
                self._full_window_streak = 0
        else:
            self._full_window_streak = 0

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        sink = self._sink
        if sink is None:
            # Detached generation: bytes may have landed in scratch, but are not
            # logically committed anywhere.
            return
        sink.commit(nbytes)
        self._observe_window_fill(nbytes)
        self._received += 1
        self._received_bytes += nbytes
        transport = self._read_transport
        # Never pause merely because a single legal incomplete frame crossed the
        # watermark.  Once its head is actionable, pausing is safe: the reader
        # can consume/reject it and explicitly resume below the low watermark.
        if (
            not self._paused_reading
            and transport is not None
            and sink.buffered >= _HIGH_WATER
            and sink.head_frame_ready()
        ):
            transport.pause_reading()
            self._paused_reading = True
            self._pauses += 1
        self._wake()

    @property
    def received(self) -> int:
        return self._received

    @property
    def received_bytes(self) -> int:
        return self._received_bytes

    @property
    def at_eof(self) -> bool:
        return self._eof

    @property
    def exception(self) -> BaseException | None:
        return self._exception

    @property
    def pauses(self) -> int:
        return self._pauses

    @property
    def resumes(self) -> int:
        return self._resumes

    @property
    def sink(self) -> DecoderSink | None:
        return self._sink

    @property
    def window_target(self) -> int:
        return self._window_target

    async def wait_for_data(self) -> None:
        if self._waiter is not None:
            raise RuntimeError("decoder ingress already has a waiting reader")
        waiter = self._loop.create_future()
        self._waiter = waiter
        try:
            await waiter
        finally:
            if self._waiter is waiter:
                self._waiter = None

    def resume_if_drained(self) -> None:
        sink = self._sink
        transport = self._read_transport
        if (
            self._paused_reading
            and sink is not None
            and transport is not None
            and not transport.is_closing()
            and (sink.buffered <= _LOW_WATER or not sink.head_frame_ready())
        ):
            transport.resume_reading()
            self._paused_reading = False
            self._resumes += 1

    def _wake(self) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)


class PushStreamTransport(_StreamTransportBase):
    """TCP stream whose receive side commits directly into an attached decoder.

    ``receive()`` is edge-triggered on selector callbacks; buffered partial MQTT
    data is not itself a readiness condition.  This class intentionally has no
    ``read()`` method: decoder ingress is a distinct receive capability rather
    than a byte-stream transport with altered semantics.
    """

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
        protocol = self._protocol
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
        protocol = self._protocol
        sink = protocol.sink
        stats = {
            "recv_callbacks": protocol.received,
            "recv_bytes": protocol.received_bytes,
            "reader_resumptions": self._resumptions,
            "reader_waits": self._waits,
            "pause_count": protocol.pauses,
            "resume_count": protocol.resumes,
            "receive_window_target": protocol.window_target,
        }
        if sink is not None:
            for name in (
                "capacity",
                "capacity_peak",
                "growth_count",
                "shrink_count",
                "compaction_count",
                "storage_generation",
            ):
                value = getattr(sink, name, None)
                if isinstance(value, int):
                    stats[f"decoder_{name}"] = value
            stats["decoder_buffered"] = sink.buffered
        return stats

    def stats(self) -> TransportStats:
        base = super().stats()
        sink = self._protocol.sink
        buffered = 0 if sink is None else sink.buffered
        return replace(base, buffered_read_bytes=buffered)

    async def close(self) -> None:
        self._protocol.detach()
        await super().close()
