"""Unknown stream backlog must remain distinguishable from measured zero."""

import asyncio

import pytest

from mqttium.transport._stream import StreamTransport
from mqttium.transport.stats import TransportStats
from mqttium.transport.websocket import WebSocketTransport


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
    assert TransportStats.unavailable(None).buffered_read_bytes == 0
    assert TransportStats.unavailable(object()).buffered_read_bytes is None
