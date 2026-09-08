"""Fixed-capacity storage invariants for IncrementalDecoder.

These cover the slab itself: the window handed to a receiver, in-place
compaction, growth for an oversized frame, and the shrink back afterwards.
"""

from __future__ import annotations

import random

import pytest

from mqttium.codec.buffer import (
    _MIN_WINDOW,
    RECEIVE_QUANTUM,
    _OVERSIZE_RETENTION,
    DEFAULT_CAPACITY,
    IncrementalDecoder,
)
from mqttium.enums import PacketType


def _publish(payload_size: int) -> bytes:
    body = b"\x00\x01t" + b"p" * payload_size
    remaining = len(body)
    out = bytearray([0x30])
    while True:
        digit = remaining % 128
        remaining //= 128
        out.append(digit | 0x80 if remaining else digit)
        if not remaining:
            break
    return bytes(out) + body


def test_window_is_never_empty_and_survives_being_filled_to_capacity() -> None:
    # asyncio.BufferedProtocol.get_buffer() raises RuntimeError on an empty
    # buffer, so the window must stay usable even when the slab is full.
    decoder = IncrementalDecoder()
    for _ in range(64):
        window = decoder.writable_window()
        assert len(window) > 0
        window[:] = b"\x00" * len(window)
        decoder.commit(len(window))
        decoder._start = decoder._end  # pretend the reader consumed everything
        decoder._reset()


def test_receiving_through_the_window_decodes_the_same_frames_as_feed() -> None:
    frames = b"".join(_publish(size) for size in (10, 5000, 70000, 3))

    through_feed = IncrementalDecoder()
    through_feed.feed(frames)
    expected = [(p.packet_type, p.remaining) for p in through_feed.drain_packets(limit=10)]

    through_window = IncrementalDecoder()
    offset = 0
    while offset < len(frames):
        window = through_window.writable_window()
        chunk = frames[offset : offset + len(window)]
        window[: len(chunk)] = chunk
        through_window.commit(len(chunk))
        offset += len(chunk)
    actual = [(p.packet_type, p.remaining) for p in through_window.drain_packets(limit=10)]

    assert actual == expected
    assert expected[0][0] is PacketType.PUBLISH


def test_frame_split_across_windows_and_a_compaction_still_decodes() -> None:
    decoder = IncrementalDecoder()
    # Consume a frame first so `_start` is non-zero and a compaction is needed.
    decoder.feed(_publish(200))
    assert decoder.next_packet() is not None

    frame = _publish(100_000)
    for offset in range(0, len(frame), 9973):  # deliberately unaligned chunks
        piece = frame[offset : offset + 9973]
        window = decoder.writable_window()
        assert len(window) >= len(piece) or len(window) > 0
        taken = min(len(window), len(piece))
        window[:taken] = piece[:taken]
        decoder.commit(taken)
        if taken < len(piece):
            rest = piece[taken:]
            window = decoder.writable_window(len(rest))
            window[: len(rest)] = rest
            decoder.commit(len(rest))

    packet = decoder.next_packet()
    assert packet is not None
    assert len(packet.remaining) == len(frame) - 4


def test_slab_grows_for_an_oversized_frame_then_shrinks_back() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_publish(64))
    assert decoder.next_packet() is not None
    assert decoder.capacity == DEFAULT_CAPACITY

    decoder.feed(_publish(DEFAULT_CAPACITY * 3))
    assert decoder.capacity > DEFAULT_CAPACITY
    assert decoder.next_packet() is not None

    # Retained for a while, then given back: a single huge frame must not pin
    # the connection's memory for its lifetime.
    for _ in range(_OVERSIZE_RETENTION + 1):
        decoder.feed(_publish(64))
        assert decoder.next_packet() is not None
    assert decoder.capacity == DEFAULT_CAPACITY
    assert decoder.buffered == 0


def test_a_stream_of_oversized_frames_does_not_thrash_the_slab() -> None:
    # Shrinking on every drain would reallocate twice per frame here -- grow,
    # consume, shrink, grow -- which is the churn this design exists to remove.
    decoder = IncrementalDecoder()
    reallocations = 0
    original = IncrementalDecoder._reallocate

    def counting(self: IncrementalDecoder, capacity: int) -> None:
        nonlocal reallocations
        reallocations += 1
        original(self, capacity)

    frame = _publish(DEFAULT_CAPACITY)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(IncrementalDecoder, "_reallocate", counting)
        for _ in range(50):
            decoder.feed(frame)
            assert decoder.next_packet() is not None

    assert reallocations == 1


def test_clear_drops_oversized_growth_for_the_next_connection() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_publish(DEFAULT_CAPACITY * 2))
    assert decoder.capacity > DEFAULT_CAPACITY

    decoder.clear()
    assert decoder.capacity == DEFAULT_CAPACITY
    assert decoder.buffered == 0


def test_compaction_reuses_the_slab_rather_than_reallocating() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_publish(1000))
    assert decoder.next_packet() is not None
    identity = id(decoder._buf)

    # Enough traffic to force many compactions without ever exceeding capacity.
    for _ in range(200):
        decoder.feed(_publish(20_000))
        assert decoder.next_packet() is not None

    assert id(decoder._buf) == identity
    assert decoder.capacity == DEFAULT_CAPACITY


def test_commit_rejects_more_than_the_window_handed_out() -> None:
    decoder = IncrementalDecoder()
    window = decoder.writable_window()
    with pytest.raises(ValueError):
        decoder.commit(len(window) + 1)


def test_clear_keeps_capacity_so_the_next_receive_does_not_reallocate() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_publish(1000))
    identity = id(decoder._buf)
    decoder.clear()
    assert decoder.buffered == 0
    assert decoder.capacity == DEFAULT_CAPACITY
    assert id(decoder._buf) == identity


@pytest.mark.parametrize("seed", range(25))
def test_random_window_chunking_matches_feed(seed: int) -> None:
    # The window path must frame identically to feed() no matter how the
    # receiver's chunk boundaries fall relative to frames and compactions.
    rng = random.Random(seed)
    sizes = [rng.choice([0, 1, 7, 300, 4095, 4096, 40_000, 300_000]) for _ in range(12)]
    frames = b"".join(_publish(size) for size in sizes)

    reference = IncrementalDecoder()
    reference.feed(frames)
    expected = [p.remaining for p in reference.drain_packets(limit=64)]

    decoder = IncrementalDecoder()
    produced: list[bytes] = []
    offset = 0
    while offset < len(frames):
        window = decoder.writable_window(rng.choice([1, 1024, 16 * 1024]))
        take = min(len(window), rng.randint(1, 70_000), len(frames) - offset)
        window[:take] = frames[offset : offset + take]
        decoder.commit(take)
        offset += take
        produced.extend(p.remaining for p in decoder.drain_packets(limit=64))
    produced.extend(p.remaining for p in decoder.drain_packets(limit=64))

    assert produced == expected
    assert len(produced) == len(sizes)


def test_sustained_oversized_frames_never_churn_past_the_retention_window() -> None:
    # Retention must key off how far each drain actually reached, not off
    # whether that drain reallocated. Once the first oversized frame has grown
    # the slab, later ones fit without reallocating -- and would look idle,
    # retiring the slab on a fixed period and re-growing on the very next frame.
    decoder = IncrementalDecoder()
    frame = _publish(DEFAULT_CAPACITY + 8192)
    reallocations = 0
    original = IncrementalDecoder._reallocate

    def counting(self: IncrementalDecoder, capacity: int) -> None:
        nonlocal reallocations
        reallocations += 1
        original(self, capacity)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(IncrementalDecoder, "_reallocate", counting)
        for _ in range(_OVERSIZE_RETENTION * 4):
            decoder.feed(frame)
            assert decoder.next_packet() is not None

    assert reallocations == 1  # the initial growth only
    assert decoder.capacity > DEFAULT_CAPACITY

    # And the room is still given back once the big frames stop.
    for _ in range(_OVERSIZE_RETENTION + 1):
        decoder.feed(_publish(64))
        assert decoder.next_packet() is not None
    assert decoder.capacity == DEFAULT_CAPACITY


def test_capacity_never_exceeds_one_maximum_frame_plus_a_window() -> None:
    # Doubling alone reached 2x max_packet_size: a frame just over a doubling
    # step leaves a tail too small for the next window and doubles again.
    limit = 4 * 1024 * 1024
    decoder = IncrementalDecoder(max_packet_size=limit)
    frame = _publish(limit - 200)
    assert len(frame) <= limit

    offset = 0
    while offset < len(frame):
        window = decoder.writable_window()
        take = min(len(window), 4096, len(frame) - offset)
        window[:take] = frame[offset : offset + take]
        window.release()
        decoder.commit(take)
        offset += take

    assert decoder.capacity <= limit + _MIN_WINDOW
    assert decoder.next_packet() is not None


def test_a_feed_larger_than_the_ceiling_is_still_honoured() -> None:
    # The ceiling is what framing can ever need; it must not cap an explicit
    # feed(), which WebSocket and TLS may hand over in one call.
    decoder = IncrementalDecoder(max_packet_size=1024 * 1024)
    decoder.feed(b"\x00" * (8 * 1024 * 1024))
    assert decoder.buffered == 8 * 1024 * 1024


def test_the_window_is_capped_regardless_of_retained_capacity() -> None:
    # A slab left large must not hand its whole free tail to one recv_into().
    decoder = IncrementalDecoder()
    decoder.feed(_publish(8 * DEFAULT_CAPACITY))
    assert decoder.next_packet() is not None
    assert decoder.capacity > 8 * DEFAULT_CAPACITY // 2  # still retained

    window = decoder.writable_window()
    assert len(window) == RECEIVE_QUANTUM
    window.release()

    # An explicit larger need is still honoured.
    window = decoder.writable_window(RECEIVE_QUANTUM * 2)
    assert len(window) >= RECEIVE_QUANTUM * 2
    window.release()


def test_a_capped_window_still_receives_a_frame_larger_than_the_quantum() -> None:
    decoder = IncrementalDecoder()
    frame = _publish(3 * RECEIVE_QUANTUM)
    offset = 0
    windows = 0
    while offset < len(frame):
        window = decoder.writable_window()
        assert len(window) <= RECEIVE_QUANTUM
        take = min(len(window), len(frame) - offset)
        window[:take] = frame[offset : offset + take]
        window.release()
        decoder.commit(take)
        offset += take
        windows += 1

    assert windows >= 3  # the cap really did split it
    packet = decoder.next_packet()
    assert packet is not None
    assert len(packet.remaining) == 3 + 3 * RECEIVE_QUANTUM
