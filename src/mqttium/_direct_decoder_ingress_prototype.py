"""Experimental direct selector ingress for receive-path benchmarks.

NOT production wired. Selector ``recv_into()`` writes directly into storage
owned by the MQTT incremental decoder, removing the intermediate
``StreamReader -> bytes -> decoder.feed()`` receive copy.

Read readiness is generation-gated: one synthetic ``read()`` notification is
issued for newly observed selector receive callbacks, not merely because bytes
remain buffered. This matters for TCP fragmentation because an incomplete MQTT
frame may legitimately stay buffered while the reader waits for another network
arrival.

Mutable storage never escapes the decoder. ``RawPacket`` bodies and
application-visible ``Message.payload`` values are still materialised as owned
``bytes`` before they can outlive synchronous decoding.
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import suppress
from typing import Any

from mqttium.api.async_client import AsyncClient as _AsyncClient
from mqttium.codec.buffer import IncrementalDecoder as _IncrementalDecoder
from mqttium.codec.buffer import RawPacket
from mqttium.enums import PacketType
from mqttium.errors import MalformedPacketError, PacketTooLargeError
from mqttium.transport._stream import StreamTransport
from mqttium.transport.tcp import TcpTransport as _StdTcpTransport

_RX_CHUNK = 256 * 1024
_INITIAL_CAPACITY = 512 * 1024
_READ_HIGH_WATER = 512 * 1024
_READ_LOW_WATER = 128 * 1024
_COPY_VIEW_THRESHOLD = 4096


class _IngressReady(bytes):
    pass


_INGRESS_READY = _IngressReady(b"\x00")


def _direct_ingress_enabled(ssl: Any, loop: asyncio.AbstractEventLoop) -> bool:
    """Return whether this connection can use selector ``recv_into`` ingress."""

    return ssl is None and isinstance(loop, asyncio.SelectorEventLoop)


class DirectIngressDecoder(_IncrementalDecoder):
    """IncrementalDecoder variant with writable spare capacity.

    ``_buf`` is capacity, not logical length. ``_end`` is the first unwritten
    byte and all inherited/public decode semantics are reproduced against the
    logical ``[_start:_end]`` interval.
    """

    __slots__ = ("_end", "_growth_count", "_compaction_count")

    def __init__(
        self,
        max_packet_size: int,
        *,
        initial_capacity: int = _INITIAL_CAPACITY,
    ) -> None:
        if max_packet_size < 1:
            raise ValueError("max_packet_size too small")
        if initial_capacity < _RX_CHUNK:
            initial_capacity = _RX_CHUNK
        self._buf = bytearray(initial_capacity)
        self._start = 0
        self._end = 0
        self._max_packet_size = max_packet_size
        self._high_water = 0
        self._growth_count = 0
        self._compaction_count = 0

    @property
    def buffered(self) -> int:
        return self._end - self._start

    @property
    def next_header_byte(self) -> int | None:
        return self._buf[self._start] if self._start < self._end else None

    @property
    def capacity(self) -> int:
        return len(self._buf)

    @property
    def growth_count(self) -> int:
        return self._growth_count

    @property
    def compaction_count(self) -> int:
        return self._compaction_count

    def clear(self) -> None:
        self._start = 0
        self._end = 0

    def _compact(self) -> None:
        start = self._start
        end = self._end
        if start == 0:
            return
        if start == end:
            self._start = 0
            self._end = 0
            return
        live = end - start
        # Equal-length memoryview assignment maps to an overlapping in-place
        # move without allocating a second bytearray of the live region.
        view = memoryview(self._buf)
        try:
            self._buf[:live] = view[start:end]
        finally:
            view.release()
        self._start = 0
        self._end = live
        self._compaction_count += 1

    def _ensure_tail(self, wanted: int) -> None:
        if len(self._buf) - self._end >= wanted:
            return
        if self._start:
            self._compact()
            if len(self._buf) - self._end >= wanted:
                return
        needed = self._end + wanted
        capacity = len(self._buf)
        new_capacity = max(needed, max(capacity * 2, _INITIAL_CAPACITY))
        self._buf.extend(b"\x00" * (new_capacity - capacity))
        self._growth_count += 1

    def writable_buffer(self) -> memoryview:
        self._ensure_tail(_RX_CHUNK)
        return memoryview(self._buf)[self._end : self._end + _RX_CHUNK]

    def commit_written(self, nbytes: int) -> None:
        if nbytes < 0 or self._end + nbytes > len(self._buf):
            raise RuntimeError("invalid direct-ingress commit")
        self._end += nbytes
        buffered = self._end - self._start
        if buffered > self._high_water:
            self._high_water = buffered

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        if data is _INGRESS_READY:
            return
        if not data:
            return
        size = len(data)
        self._ensure_tail(size)
        self._buf[self._end : self._end + size] = data
        self.commit_written(size)

    def peek_packet_bounds(self) -> tuple[int, int, int] | None:
        buf = self._buf
        start = self._start
        available = self._end - start
        if available < 2:
            return None

        header = buf[start]
        pos = start + 1
        value = 0
        multiplier = 1
        encoded_bytes = 0
        while True:
            if pos >= self._end:
                return None
            byte = buf[pos]
            pos += 1
            encoded_bytes += 1
            value += (byte & 0x7F) * multiplier
            if byte & 0x80 == 0:
                break
            if encoded_bytes == 4:
                raise MalformedPacketError("Malformed Variable Byte Integer (too long)")
            multiplier *= 128

        canonical = 1 if value < 128 else 2 if value < 16_384 else 3 if value < 2_097_152 else 4
        if encoded_bytes != canonical:
            raise MalformedPacketError("Non-canonical Variable Byte Integer")

        fixed_header_len = pos - start
        total = fixed_header_len + value
        if total > self._max_packet_size:
            raise PacketTooLargeError(
                f"Packet size {total} exceeds maximum {self._max_packet_size}"
            )
        if available < total:
            return None
        return header, pos, start + total

    def head_is_actionable(self) -> bool:
        """Whether the reader can make progress without receiving more bytes.

        Malformed and oversize headers are actionable too: the reader must run
        so it can surface the protocol error rather than leaving the socket
        paused forever.
        """

        try:
            return self.peek_packet_bounds() is not None
        except (MalformedPacketError, PacketTooLargeError):
            return True

    def consume_peeked_packet(self, body_end: int) -> None:
        if not (self._start < body_end <= self._end):
            raise AssertionError("invalid direct-ingress packet boundary")
        self._start = body_end
        if self._start == self._end:
            self._start = 0
            self._end = 0

    def next_packet(self) -> RawPacket | None:
        bounds = self.peek_packet_bounds()
        if bounds is None:
            return None
        header, body_start, body_end = bounds
        remaining_length = body_end - body_start
        if remaining_length >= _COPY_VIEW_THRESHOLD:
            view = memoryview(self._buf)
            try:
                body = bytes(view[body_start:body_end])
            finally:
                view.release()
        else:
            body = bytes(self._buf[body_start:body_end])
        self.consume_peeked_packet(body_end)
        return RawPacket(
            packet_type=PacketType.from_byte(header),
            flags=header & 0x0F,
            remaining=body,
        )


class _DirectDecoderProtocol(asyncio.StreamReaderProtocol, asyncio.BufferedProtocol):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        decoder: DirectIngressDecoder,
        *,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(reader, loop=loop)
        self.decoder = decoder
        self.ready = asyncio.Event()
        self.rx_transport: asyncio.Transport | None = None
        self.read_paused = False
        self.eof = False
        self.exc: BaseException | None = None
        self.recv_callbacks = 0
        self.recv_bytes = 0
        self.pause_count = 0
        self.resume_count = 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        if not isinstance(transport, asyncio.Transport):
            raise TypeError("direct ingress requires asyncio.Transport")
        self.rx_transport = transport

    def get_buffer(self, sizehint: int) -> memoryview:
        del sizehint
        return self.decoder.writable_buffer()

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        self.decoder.commit_written(nbytes)
        self.recv_callbacks += 1
        self.recv_bytes += nbytes
        transport = self.rx_transport
        # Never pause merely because a large *incomplete* MQTT frame crossed
        # the byte watermark: the decoder cannot consume that frame yet, so
        # doing so would deadlock receive progress. Pause only when the reader
        # can actually consume or reject the head frame.
        if (
            not self.read_paused
            and self.decoder.buffered >= _READ_HIGH_WATER
            and self.decoder.head_is_actionable()
            and transport is not None
        ):
            transport.pause_reading()
            self.read_paused = True
            self.pause_count += 1
        self.ready.set()

    def maybe_resume_reading(self) -> None:
        transport = self.rx_transport
        if (
            self.read_paused
            and (self.decoder.buffered <= _READ_LOW_WATER or not self.decoder.head_is_actionable())
            and transport is not None
            and not transport.is_closing()
        ):
            transport.resume_reading()
            self.read_paused = False
            self.resume_count += 1

    def eof_received(self) -> bool | None:
        self.eof = True
        self.ready.set()
        return super().eof_received()

    def connection_lost(self, exc: Exception | None) -> None:
        if exc is not None:
            self.exc = exc
        self.eof = True
        self.ready.set()
        super().connection_lost(exc)


class DirectIngressTcpTransport(StreamTransport):
    """StreamTransport facade whose read readiness tracks receive generations."""

    __slots__ = ("_direct_protocol", "_direct_decoder", "_seen_callbacks")

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        protocol: _DirectDecoderProtocol,
        decoder: DirectIngressDecoder,
    ) -> None:
        super().__init__(reader, writer)
        self._direct_protocol = protocol
        self._direct_decoder = decoder
        self._seen_callbacks = 0

    def _consume_generation(self) -> bytes | None:
        protocol = self._direct_protocol
        callbacks = protocol.recv_callbacks
        if callbacks == self._seen_callbacks:
            return None
        self._seen_callbacks = callbacks
        protocol.maybe_resume_reading()
        return _INGRESS_READY

    def receive_stats(self) -> dict[str, int]:
        protocol = self._direct_protocol
        decoder = self._direct_decoder
        return {
            "recv_callbacks": protocol.recv_callbacks,
            "recv_bytes": protocol.recv_bytes,
            "pause_count": protocol.pause_count,
            "resume_count": protocol.resume_count,
            "decoder_high_water": decoder.high_water,
            "decoder_capacity": decoder.capacity,
            "decoder_growth_count": decoder.growth_count,
            "decoder_compaction_count": decoder.compaction_count,
            "decoder_buffered": decoder.buffered,
        }

    async def read(self, n: int = 65536) -> bytes:
        del n
        protocol = self._direct_protocol
        protocol.maybe_resume_reading()

        # Match StreamReader.read(): a transport exception wins over bytes that
        # were buffered before connection_lost(exc). Normal EOF is different:
        # the last receive generation is still drained before returning b"".
        if protocol.exc is not None:
            raise protocol.exc
        ready = self._consume_generation()
        if ready is not None:
            return ready
        if protocol.eof:
            return b""

        # Event.clear() is paired with a generation re-check so a callback in
        # the clear/check race cannot be lost. An error in the same race keeps
        # StreamReader's exception-first semantics.
        protocol.ready.clear()
        if protocol.exc is not None:
            raise protocol.exc
        ready = self._consume_generation()
        if ready is not None:
            return ready
        if protocol.eof:
            return b""

        await protocol.ready.wait()
        if protocol.exc is not None:
            raise protocol.exc
        ready = self._consume_generation()
        if ready is not None:
            return ready
        return b""


async def _connect_direct(
    host: str,
    port: int,
    *,
    ssl: Any,
    decoder: DirectIngressDecoder,
) -> StreamTransport:
    loop = asyncio.get_running_loop()
    if not _direct_ingress_enabled(ssl, loop):
        return await _StdTcpTransport.connect(host, port, ssl=ssl)

    reader = asyncio.StreamReader(loop=loop)
    protocol = _DirectDecoderProtocol(reader, decoder, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    sock = writer.get_extra_info("socket")
    if sock is not None:
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return DirectIngressTcpTransport(reader, writer, protocol, decoder)


def install() -> type[_AsyncClient]:
    """Install the benchmark-only AsyncClient subclass into mqttium.api.

    The ordinary decoder is retained until a clear-text SelectorEventLoop
    connection actually selects the experiment. TLS, ProactorEventLoop and any
    other unsupported loop therefore keep #446's transport *and* decoder.
    """

    import mqttium.api as api_module
    import mqttium.api.async_client as async_client_module

    base_client = async_client_module.AsyncClient
    if getattr(base_client, "_direct_ingress_prototype", False):
        return base_client

    class DirectIngressAsyncClient(base_client):
        _direct_ingress_prototype = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            standard_decoder = self._decoder
            direct_decoder: DirectIngressDecoder | None = None

            async def factory(
                host: str,
                port: int,
                *,
                ssl: Any = None,
            ) -> StreamTransport:
                nonlocal direct_decoder

                loop = asyncio.get_running_loop()
                current_limit = self._decoder.max_packet_size
                if _direct_ingress_enabled(ssl, loop):
                    if direct_decoder is None:
                        direct_decoder = DirectIngressDecoder(current_limit)
                    else:
                        direct_decoder.max_packet_size = current_limit
                    self._decoder = direct_decoder
                    return await _connect_direct(host, port, ssl=None, decoder=direct_decoder)

                standard_decoder.max_packet_size = current_limit
                self._decoder = standard_decoder
                return await _StdTcpTransport.connect(host, port, ssl=ssl)

            self._transport_factory = factory

    DirectIngressAsyncClient.__name__ = "AsyncClient"
    DirectIngressAsyncClient.__qualname__ = "AsyncClient"
    async_client_module.AsyncClient = DirectIngressAsyncClient
    api_module.AsyncClient = DirectIngressAsyncClient
    return DirectIngressAsyncClient


__all__ = ["DirectIngressDecoder", "DirectIngressTcpTransport", "install"]
