"""Correctness checks for the experimental direct decoder ingress."""

from __future__ import annotations

import asyncio

import pytest

from mqttium._direct_decoder_ingress_prototype import (
    DirectIngressDecoder,
    _INGRESS_READY,
    _connect_direct,
)
from mqttium.errors import MalformedPacketError


def _commit(decoder: DirectIngressDecoder, data: bytes) -> None:
    target = decoder.writable_buffer()
    try:
        target[: len(data)] = data
    finally:
        target.release()
    decoder.commit_written(len(data))


def test_direct_decoder_accepts_fragmented_packet_into_writable_storage() -> None:
    decoder = DirectIngressDecoder(1024 * 1024)

    _commit(decoder, b"\x30\x03a")
    assert decoder.next_packet() is None

    _commit(decoder, b"bc")
    packet = decoder.next_packet()

    assert packet is not None
    assert packet.remaining == b"abc"
    assert isinstance(packet.remaining, bytes)
    assert decoder.buffered == 0


def test_direct_decoder_rejects_four_continuation_vbi_bytes_immediately() -> None:
    decoder = DirectIngressDecoder(1024 * 1024)
    _commit(decoder, b"\x30\x80\x80\x80\x80")

    with pytest.raises(MalformedPacketError, match="too long"):
        decoder.peek_packet_bounds()


def test_direct_decoder_compacts_live_bytes_without_changing_them() -> None:
    decoder = DirectIngressDecoder(1024 * 1024)
    live = bytes((i % 251 for i in range(200_000)))
    decoder._buf[100_000:300_000] = live
    decoder._start = 100_000
    decoder._end = 300_000

    target = decoder.writable_buffer()
    target.release()

    assert decoder._start == 0
    assert decoder._end == len(live)
    assert bytes(decoder._buf[: len(live)]) == live
    assert decoder.compaction_count == 1
    assert decoder.growth_count == 0


async def test_direct_transport_waits_for_new_receive_generation_on_fragment() -> None:
    release_tail = asyncio.Event()

    async def send_fragmented_packet(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        del reader
        writer.write(b"\x30")
        await writer.drain()
        await release_tail.wait()
        writer.write(b"\x03abc")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(send_fragmented_packet, "127.0.0.1", 0)
    socket = server.sockets[0]
    host, port = socket.getsockname()[:2]
    decoder = DirectIngressDecoder(1024 * 1024)
    transport = await _connect_direct(host, port, ssl=None, decoder=decoder)

    try:
        first = await asyncio.wait_for(transport.read(), timeout=1.0)
        assert first is _INGRESS_READY
        assert decoder.next_packet() is None

        pending = asyncio.create_task(transport.read())
        await asyncio.sleep(0)
        assert not pending.done()

        release_tail.set()
        second = await asyncio.wait_for(pending, timeout=1.0)
        assert second is _INGRESS_READY

        packet = decoder.next_packet()
        assert packet is not None
        assert packet.remaining == b"abc"
    finally:
        release_tail.set()
        await transport.close()
        server.close()
        await server.wait_closed()
