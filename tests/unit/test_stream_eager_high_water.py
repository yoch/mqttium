from __future__ import annotations

import asyncio

from mqttium.transport._stream import StreamTransport, _WRITE_BUFFER_HIGH_WATER


class _BufferTransport:
    def __init__(self, size: int) -> None:
        self.size = size

    def get_write_buffer_size(self) -> int:
        return self.size


class _Writer:
    def __init__(self, transport: _BufferTransport) -> None:
        self.transport = transport
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)
        self.transport.size += len(data)


def _stream(buffered: int) -> tuple[StreamTransport, _Writer]:
    writer = _Writer(_BufferTransport(buffered))
    stream = StreamTransport(asyncio.StreamReader(), writer)  # type: ignore[arg-type]
    return stream, writer


def test_eager_write_refuses_frame_that_would_cross_high_water() -> None:
    stream, writer = _stream(_WRITE_BUFFER_HIGH_WATER - 4)

    assert stream.write_nowait(b"12345") is False
    assert writer.writes == []
    assert writer.transport.size == _WRITE_BUFFER_HIGH_WATER - 4


def test_eager_write_accepts_frame_that_lands_exactly_on_high_water() -> None:
    stream, writer = _stream(_WRITE_BUFFER_HIGH_WATER - 4)

    assert stream.write_nowait(b"1234") is True
    assert writer.writes == [b"1234"]
    assert writer.transport.size == _WRITE_BUFFER_HIGH_WATER


def test_eager_write_refuses_oversized_contiguous_frame_from_empty_buffer() -> None:
    stream, writer = _stream(0)
    frame = b"x" * (_WRITE_BUFFER_HIGH_WATER + 1)

    assert stream.write_nowait(frame) is False
    assert writer.writes == []
    assert writer.transport.size == 0
