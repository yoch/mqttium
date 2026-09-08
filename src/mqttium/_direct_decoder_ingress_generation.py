"""Generation-gated refinement of the direct decoder-ingress prototype.

A read notification corresponds to *new network ingress*, not merely to bytes
remaining buffered in the decoder. This matters for TCP fragmentation: an
incomplete MQTT frame may legitimately remain buffered while the reader waits
for another selector callback.

The module remains benchmark-only and preserves the same ownership rule as the
base prototype: mutable receive storage is internal; data that can outlive
synchronous decoding is materialised as owned ``bytes`` by the existing parser.
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import suppress
from typing import Any

from mqttium.api.async_client import AsyncClient as _AsyncClient
from mqttium.transport._stream import StreamTransport
from mqttium.transport.tcp import TcpTransport as _StdTcpTransport

from mqttium._direct_decoder_ingress_prototype import (
    DirectIngressDecoder,
    _DirectDecoderProtocol,
    _INGRESS_READY,
)


class GenerationDirectIngressTcpTransport(StreamTransport):
    """Wake AsyncClient once for each observed receive generation.

    Several selector callbacks may collapse into one generation observed by the
    consumer; that is fine because all bytes are already committed to the same
    decoder and AsyncClient drains complete packets until it reaches an
    incomplete frame. Crucially, an incomplete residual frame cannot itself
    trigger another synthetic read.
    """

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

    async def read(self, n: int = 65536) -> bytes:
        del n
        protocol = self._direct_protocol
        protocol.maybe_resume_reading()

        ready = self._consume_generation()
        if ready is not None:
            return ready
        if protocol.exc is not None:
            raise protocol.exc
        if protocol.eof:
            return b""

        # Event.clear() is paired with a generation re-check so a callback in
        # the clear/check race cannot be lost.
        protocol.ready.clear()
        ready = self._consume_generation()
        if ready is not None:
            return ready
        if protocol.exc is not None:
            raise protocol.exc
        if protocol.eof:
            return b""

        await protocol.ready.wait()
        ready = self._consume_generation()
        if ready is not None:
            return ready
        if protocol.exc is not None:
            raise protocol.exc
        return b""


async def _connect_generation_direct(
    host: str,
    port: int,
    *,
    ssl: Any,
    decoder: DirectIngressDecoder,
) -> StreamTransport:
    loop = asyncio.get_running_loop()
    if ssl is not None or not isinstance(loop, asyncio.SelectorEventLoop):
        return await _StdTcpTransport.connect(host, port, ssl=ssl)

    reader = asyncio.StreamReader(loop=loop)
    protocol = _DirectDecoderProtocol(reader, decoder, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    sock = writer.get_extra_info("socket")
    if sock is not None:
        with suppress(OSError):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return GenerationDirectIngressTcpTransport(reader, writer, protocol, decoder)


def install() -> type[_AsyncClient]:
    """Install the generation-gated benchmark-only AsyncClient subclass."""

    import mqttium.api as api_module
    import mqttium.api.async_client as async_client_module

    base_client = async_client_module.AsyncClient
    if getattr(base_client, "_generation_direct_ingress_prototype", False):
        return base_client

    class GenerationDirectIngressAsyncClient(base_client):
        _generation_direct_ingress_prototype = True

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            old_decoder = self._decoder
            decoder = DirectIngressDecoder(old_decoder.max_packet_size)
            self._decoder = decoder

            async def factory(
                host: str,
                port: int,
                *,
                ssl: Any = None,
            ) -> StreamTransport:
                return await _connect_generation_direct(
                    host,
                    port,
                    ssl=ssl,
                    decoder=decoder,
                )

            self._transport_factory = factory

    GenerationDirectIngressAsyncClient.__name__ = "AsyncClient"
    GenerationDirectIngressAsyncClient.__qualname__ = "AsyncClient"
    async_client_module.AsyncClient = GenerationDirectIngressAsyncClient
    api_module.AsyncClient = GenerationDirectIngressAsyncClient
    return GenerationDirectIngressAsyncClient


__all__ = ["GenerationDirectIngressTcpTransport", "install"]
