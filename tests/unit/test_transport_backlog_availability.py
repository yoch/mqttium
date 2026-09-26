"""Unknown stream backlog must remain distinguishable from measured zero."""

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.transport._push import PushStreamTransport
from mqttium.transport._stream import StreamTransport
from mqttium.transport.stats import TransportStats
from mqttium.transport.websocket import WebSocketTransport
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until
from tests.unit.test_decoder_push_transport import _deliver, _publish, _wire


class _Writer:
    def __init__(self):
        self.transport = self

    def get_write_buffer_size(self):
        return 0

    def is_closing(self):
        return False


@pytest.mark.parametrize("transport_type", [StreamTransport, WebSocketTransport])
async def test_reader_backlog_is_unknown_not_zero(transport_type):
    reader = asyncio.StreamReader()
    reader.feed_data(b"x" * 100)
    transport = transport_type(reader, _Writer())
    snapshot = transport.stats()
    assert snapshot.buffered_read_bytes is None
    assert await reader.read(100) == b"x" * 100
    # Empty buffers are not reported as measured when the API cannot measure.
    assert transport.stats().buffered_read_bytes is None


def test_no_transport_and_unknown_transport_are_distinct():
    assert TransportStats._unavailable(None).buffered_read_bytes == 0
    assert TransportStats._unavailable(object()).buffered_read_bytes is None


class _ClosingWriter(_Writer):
    def is_closing(self):
        return True


async def test_websocket_fragment_under_reassembly_is_not_reported_as_the_backlog():
    reader = asyncio.StreamReader()
    transport = WebSocketTransport(reader, _Writer())
    # An unmasked server binary frame without FIN starts a fragmented message.
    reader.feed_data(b"\x02\x0a" + b"f" * 10)
    pending = asyncio.create_task(transport.read())
    try:
        await wait_until(lambda: transport._fragment is not None)
        assert len(transport._fragment) == 10
        assert transport.stats().buffered_read_bytes is None
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("closing", [False, True])
async def test_push_decoder_occupancy_is_measured_busy_then_empty(closing):
    decoder = IncrementalDecoder()
    protocol, _ = _wire(decoder)
    writer = _ClosingWriter() if closing else _Writer()
    transport = PushStreamTransport(asyncio.StreamReader(), writer, protocol)
    frame = _publish(4096)
    _deliver(protocol, frame[:200])
    assert transport.stats().buffered_read_bytes == 200
    _deliver(protocol, frame[200:])
    assert decoder.next_packet() is not None
    # A measured empty decoder is zero, including while the transport closes.
    snapshot = transport.stats()
    assert snapshot.closing is closing
    assert snapshot.buffered_read_bytes == 0


async def test_client_snapshot_composes_transport_availability():
    broker = ScriptedBrokerTransport()
    client = AsyncClient("backlog", keepalive=0)
    client._transport_factory = transport_factory(broker)
    await client.connect("unused")
    try:
        # A live transport without a statistics method cannot measure.
        live = client.stats().transport
        assert live.buffered_read_bytes is None
    finally:
        await client.disconnect()
    assert client.stats().transport == TransportStats._unavailable(None)
    assert client.stats().transport.buffered_read_bytes == 0

    # A present but closing pull transport is still present, not absent.
    reader = asyncio.StreamReader()
    reader.feed_data(b"x")
    client._transport = StreamTransport(reader, _ClosingWriter())
    closing = client.stats().transport
    assert (closing.closing, closing.buffered_read_bytes) == (
        True,
        None,
    )
    client._transport = None
