"""Wire effects from a dead transport never cross a connection epoch."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, PacketType
from mqttium.packets import PublishPacket, encode_frame
from mqttium.protocol.engine import EffectKind


class ConnackTransport:
    def __init__(self, *, session_present: bool) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._decoder = IncrementalDecoder()
        self._closing = False
        self.session_present = session_present
        self.written: list[bytes] = []

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            if raw.packet_type is PacketType.CONNECT:
                flags = 1 if self.session_present else 0
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, bytes([flags, 0])))

    async def read(self, n: int = 65536) -> bytes:
        return await self._rx.get()

    async def close(self) -> None:
        self._closing = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def _cancel_send_under_backpressure(client: AsyncClient) -> int:
    client._engine.state = ConnectionState.CONNECTED
    await client._enqueue_outbound(b"x")
    publishing = asyncio.create_task(client.publish("audit/stale", b"payload", qos=1))
    for _ in range(100):
        if client._outbound_waiters:
            break
        await asyncio.sleep(0)
    assert client._outbound_waiters == 1
    publishing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publishing
    assert any(effect.kind is EffectKind.SEND for effect in client._pending_effects)
    mid = next(iter(client._receipts))
    await client._invalidate_connection_epoch()
    client._engine.notify_transport_closed()
    client._engine.take_effects()
    return mid


def _packets(written: list[bytes]):
    decoder = IncrementalDecoder()
    packets = []
    for data in written:
        decoder.feed(data)
        packets.extend(decoder.drain_packets())
    return packets


async def test_clean_reconnect_does_not_send_failed_old_publish() -> None:
    client = AsyncClient(
        client_id="audit-clean",
        max_outbound_messages=1,
        max_outbound_bytes=1,
    )
    mid = await _cancel_send_under_backpressure(client)
    transport = ConnackTransport(session_present=False)

    async def factory(host: str, port: int, *, ssl: object = None) -> ConnackTransport:
        return transport

    client._transport_factory = factory
    await client.connect("fake", timeout=1.0)
    await asyncio.sleep(0)

    assert [packet.packet_type for packet in _packets(transport.written)] == [PacketType.CONNECT]
    assert mid not in client._receipts
    await client._force_close()


async def test_resumed_session_rebuilds_exactly_one_publish() -> None:
    client = AsyncClient(
        client_id="audit-resume",
        clean_start=False,
        max_outbound_messages=1,
        max_outbound_bytes=1,
    )
    await _cancel_send_under_backpressure(client)
    transport = ConnackTransport(session_present=True)

    async def factory(host: str, port: int, *, ssl: object = None) -> ConnackTransport:
        return transport

    client._transport_factory = factory
    await client.connect("fake", timeout=1.0)
    packets = []
    for _ in range(100):
        packets = _packets(transport.written)
        if sum(p.packet_type is PacketType.PUBLISH for p in packets) == 1:
            break
        await asyncio.sleep(0)

    publishes = [
        PublishPacket.decode(packet.flags, packet.remaining)
        for packet in packets
        if packet.packet_type is PacketType.PUBLISH
    ]
    assert len(publishes) == 1
    assert publishes[0].dup is True
    await client._force_close()


async def test_collecting_after_an_epoch_bump_does_not_tag_fresh_effects_stale() -> None:
    """One tag covers the whole deque, so it must never span two epochs.

    Teardown bumps the epoch while effects may still be pending. If the fresh
    effects inherited the old tag, the flusher would discard the whole deque as
    stale — including the effects belonging to the current epoch.
    """
    client = AsyncClient(client_id="c")
    # More than one effect skips the single-effect inline fast path, so these
    # stay pending and tagged with the current epoch.
    client._engine._emit(EffectKind.PINGRESP, None)
    client._engine._emit(EffectKind.PINGRESP, None)
    client._collect_effects_locked()
    assert len(client._pending_effects) == 2
    stale = list(client._pending_effects)

    await client._invalidate_connection_epoch()

    client._engine._emit(EffectKind.SUBACK, None)
    client._engine._emit(EffectKind.UNSUBACK, None)
    client._collect_effects_locked()

    assert client._pending_effect_epoch == client._connection_epoch
    assert [effect.kind for effect in client._pending_effects] == [
        EffectKind.SUBACK,
        EffectKind.UNSUBACK,
    ], "stale effects must be dropped, not merged under the new tag"
    assert not any(effect is old for old in stale for effect in client._pending_effects)
    assert client._effect_enqueued == client._effect_applied + len(client._pending_effects)
