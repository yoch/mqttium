"""Contract, transient-allocation and progress-bound regressions for ingress."""

from __future__ import annotations

import sys
import tracemalloc
from collections.abc import Callable

import pytest

from mqttium.codec.buffer import IncrementalDecoder, RECEIVE_QUANTUM
from mqttium.codec.vbi import encode_vbi

MIB = 1024 * 1024


def _frame(size: int) -> bytes:
    body = b"\x00\x01t" + bytes(range(256)) * (size // 256) + b"p" * (size % 256)
    return b"\x30" + encode_vbi(len(body)) + body


def _peak(action: Callable[[], object]) -> int:
    # Tracing is diagnostic only, never used for throughput. Allocate input and
    # backing storage before this helper so it captures the operation itself.
    tracemalloc.start()
    try:
        before = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        action()
        return tracemalloc.get_traced_memory()[1] - before
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize("layout", ["byte", "word", "matrix", "scalar", "stride", "reverse"])
def test_feed_treats_memoryview_as_wire_bytes(layout: str) -> None:
    wire = b"\xd0\x00"
    views = {
        "byte": memoryview(wire),
        "word": memoryview(wire).cast("H"),
        "matrix": memoryview(wire).cast("B", (1, 2)),
        "scalar": memoryview(wire).cast("H", ()),
        "stride": memoryview(b"\xd0X\x00X")[::2],
        "reverse": memoryview(wire[::-1])[::-1],
    }
    decoder = IncrementalDecoder()
    source = views[layout]
    try:
        decoder.feed(source)
        packet = decoder.next_packet()
        assert packet is not None and packet.remaining == b""
        assert decoder.buffered == 0
        assert source.tobytes() == wire  # the caller's view was not released
    finally:
        for view in views.values():
            view.release()


def test_released_view_fails_before_mutating_decoder() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(b"\xd0")
    view = memoryview(b"\x00")
    view.release()
    with pytest.raises(ValueError):
        decoder.feed(view)
    assert decoder.buffered == 1
    decoder.feed(b"\x00")
    assert decoder.next_packet() is not None


@pytest.mark.skipif(sys.implementation.name != "cpython", reason="CPython allocation regression")
@pytest.mark.parametrize("kind", [bytes, bytearray, memoryview])
def test_feed_does_not_copy_through_a_payload_sized_temporary(kind: type) -> None:
    decoder = IncrementalDecoder()
    window = decoder.writable_window(MIB)
    window.release()
    source = kind(b"x" * MIB)
    before = decoder._buf
    assert _peak(lambda: decoder.feed(source)) < 64 * 1024
    assert decoder._buf is before
    assert bytes(decoder._buf[: decoder.buffered]) == b"x" * MIB


@pytest.mark.skipif(sys.implementation.name != "cpython", reason="CPython allocation regression")
def test_real_overlapping_compaction_has_no_live_sized_temporary() -> None:
    first = _frame(MIB)
    second = _frame(8 * MIB)
    decoder = IncrementalDecoder()
    decoder.feed(first + second[: 4 * MIB])
    assert decoder.next_packet() is not None
    assert 0 < decoder._start < decoder.buffered  # source/destination overlap
    identity = decoder._buf

    def compact() -> None:
        window = decoder.writable_window()
        window.release()

    assert _peak(compact) < 64 * 1024
    assert decoder._buf is identity
    assert decoder._start == 0
    assert bytes(decoder._buf[: decoder.buffered]) == second[: 4 * MIB]
    decoder.feed(second[4 * MIB :])
    packet = decoder.next_packet()
    assert packet is not None and packet.remaining.endswith(second[-MIB:])


@pytest.mark.skipif(sys.implementation.name != "cpython", reason="CPython allocation regression")
def test_growth_copies_directly_into_the_new_slab() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_frame(8 * MIB)[: 4 * MIB])
    target = 8 * MIB
    # Count the new slab, but reject an additional temporary the size of live data.
    assert _peak(lambda: decoder._reallocate(target)) < target + 64 * 1024
    assert decoder.buffered == 4 * MIB


@pytest.mark.parametrize("mode", ["feed", "push"])
@pytest.mark.parametrize("size", [MIB - 32, MIB + 1, 2 * MIB + 4096, 8 * MIB, 16 * MIB - 16])
def test_fragmented_large_frames_finish_at_exact_capacity(mode: str, size: int) -> None:
    wire = _frame(size)
    decoder = IncrementalDecoder()
    offset = 0
    while offset < len(wire):
        count = min(65536, len(wire) - offset)
        if mode == "push":
            window = decoder.writable_window()
            count = min(count, len(window))
            window[:count] = wire[offset : offset + count]
            window.release()
            decoder.commit(count)
        else:
            decoder.feed(wire[offset : offset + count])
        offset += count
    assert decoder.capacity == len(wire)
    packet = decoder.next_packet()
    assert packet is not None and packet.remaining.endswith(wire[-size:])


@pytest.mark.parametrize("mode", ["feed", "push"])
def test_a_maximum_length_announcement_cannot_reserve_the_full_body(mode: str) -> None:
    decoder = IncrementalDecoder()
    header = b"\x30" + encode_vbi(16 * MIB - 5)
    # One byte at a time: neither growth nor parsing may mistake stale slab
    # contents for body progress. Only actual ingress can justify more storage.
    for byte in header + b"\x00":
        if mode == "push":
            window = decoder.writable_window()
            window[0] = byte
            window.release()
            decoder.commit(1)
        else:
            decoder.feed(bytes((byte,)))
        assert decoder.next_packet() is None
        assert decoder.capacity <= 2 * RECEIVE_QUANTUM
    assert decoder.buffered == len(header) + 1


def test_feed_can_finish_a_large_frame_and_include_following_frames() -> None:
    wire = _frame(MIB)
    following = b"\xd0\x00" * 30
    decoder = IncrementalDecoder()
    decoder.feed(wire[:65536])
    decoder.feed(wire[65536:] + following)
    packets = decoder.drain_packets()
    assert len(packets) == 31
    assert decoder.buffered == 0


def test_explicit_window_preference_and_short_tail_contract() -> None:
    decoder = IncrementalDecoder()
    window = decoder.writable_window(1)
    assert len(window) == 65536  # preference, not an upper bound
    window.release()
    wire = _frame(65536)
    decoder.feed(wire[:-1])
    window = decoder.writable_window()
    assert len(window) == 1  # known incomplete head, not a minimum guarantee
    window[0] = wire[-1]
    window.release()
    decoder.commit(1)
    assert decoder.next_packet() is not None
