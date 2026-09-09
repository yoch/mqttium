"""Fixed-capacity storage invariants for IncrementalDecoder.

These cover the slab itself: the window handed to a receiver, in-place
compaction, growth for an oversized frame, and the shrink back afterwards.
"""

from __future__ import annotations

import random

import pytest

from mqttium.errors import MalformedPacketError, PacketTooLargeError

from mqttium.codec.buffer import (
    _INITIAL_WINDOW,
    _MIN_CAPACITY,
    _OVERSIZE_RETENTION,
    DEFAULT_CAPACITY,
    RECEIVE_QUANTUM,
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


def test_first_touch_allocates_only_what_is_needed() -> None:
    # A short-lived client, an idle connection, or the TLS/WebSocket feed() path
    # must not pay the steady-state receive size just to decode a 4-byte ACK.
    decoder = IncrementalDecoder()
    decoder.feed(b"\x40\x02\x00\x01")
    assert decoder.capacity == _MIN_CAPACITY
    assert decoder.capacity < DEFAULT_CAPACITY


def test_the_receive_window_starts_small_and_is_promoted_by_full_fills() -> None:
    decoder = IncrementalDecoder()
    window = decoder.writable_window()
    assert len(window) == _INITIAL_WINDOW
    window.release()

    offered = set()
    for _ in range(40):
        window = decoder.writable_window()
        size = len(window)
        window[:size] = b"\x00" * size
        window.release()
        decoder.commit(size)
        decoder._start = decoder._end = 0
        offered.add(size)
    assert max(offered) == RECEIVE_QUANTUM
    assert _INITIAL_WINDOW in offered


def test_a_partly_filled_window_is_not_promoted() -> None:
    decoder = IncrementalDecoder()
    for _ in range(40):
        window = decoder.writable_window()
        window[:100] = b"\x00" * 100
        window.release()
        decoder.commit(100)
        decoder._start = decoder._end = 0
    assert len(decoder.writable_window()) == _INITIAL_WINDOW


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
    baseline = decoder.capacity

    decoder.feed(_publish(DEFAULT_CAPACITY * 3))
    assert decoder.capacity > baseline
    assert decoder.next_packet() is not None

    # Retention fits the peak seen over a window. The drain that consumed the
    # huge frame falls in the first window, so the slab survives that one and is
    # retired during the second, once the peak reflects only small traffic.
    for _ in range(2 * _OVERSIZE_RETENTION + 1):
        decoder.feed(_publish(64))
        assert decoder.next_packet() is not None
    assert decoder.capacity == _INITIAL_WINDOW  # the retirement floor
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
    assert decoder.capacity == _INITIAL_WINDOW
    assert decoder.buffered == 0


def test_compaction_reuses_the_slab_rather_than_reallocating() -> None:
    decoder = IncrementalDecoder()
    # Warm up so the slab has settled at a size these frames fit into.
    for _ in range(5):
        decoder.feed(_publish(20_000))
        assert decoder.next_packet() is not None
    identity = id(decoder._buf)

    # Enough traffic to force many compactions without ever exceeding capacity.
    for _ in range(200):
        decoder.feed(_publish(20_000))
        assert decoder.next_packet() is not None

    assert id(decoder._buf) == identity


def test_commit_rejects_more_than_the_window_handed_out() -> None:
    decoder = IncrementalDecoder()
    window = decoder.writable_window()
    with pytest.raises(ValueError):
        decoder.commit(len(window) + 1)


def test_clear_rewinds_without_reallocating_a_right_sized_slab() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_publish(1000))
    identity = id(decoder._buf)
    decoder.clear()
    assert decoder.buffered == 0
    assert decoder.capacity <= _INITIAL_WINDOW
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
    frame = _publish(4 * DEFAULT_CAPACITY)
    reallocations = 0
    original = IncrementalDecoder._reallocate

    shrinks = 0

    def counting(self: IncrementalDecoder, capacity: int) -> None:
        nonlocal reallocations, shrinks
        reallocations += 1
        if capacity < self._capacity:
            shrinks += 1
        original(self, capacity)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(IncrementalDecoder, "_reallocate", counting)
        for _ in range(_OVERSIZE_RETENTION * 4):
            decoder.feed(frame)
            assert decoder.next_packet() is not None

    # Growth may take a few doublings; what must never happen is a shrink
    # followed by an immediate regrow.
    assert shrinks == 0
    assert reallocations <= 8
    assert decoder.capacity > DEFAULT_CAPACITY

    # And the room is still given back once the big frames stop.
    for _ in range(2 * _OVERSIZE_RETENTION + 1):
        decoder.feed(_publish(64))
        assert decoder.next_packet() is not None
    assert decoder.capacity == _INITIAL_WINDOW


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

    assert decoder.capacity <= limit + RECEIVE_QUANTUM
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
    assert decoder.capacity > 4 * DEFAULT_CAPACITY  # still retained

    window = decoder.writable_window()
    assert len(window) <= RECEIVE_QUANTUM
    assert len(window) < decoder.capacity
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
        assert len(window) <= max(RECEIVE_QUANTUM, len(frame))
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


@pytest.mark.parametrize(
    "stale_body",
    [
        bytes([0xFF, 0xFF, 0x10]),  # frames a ~34 MiB length from continuation bits
        bytes([0xFF, 0xFF, 0xFF]),
        bytes([0x80, 0x80, 0x80]),
        bytes([0xFF, 0x7F, 0x00]),
    ],
)
@pytest.mark.parametrize("committed_header", [b"\x30\x80", b"\x30\xff", b"\x30\x80\x80"])
def test_stale_slab_bytes_never_take_part_in_framing(
    stale_body: bytes, committed_header: bytes
) -> None:
    # The slab is reused, not zeroed: bytes past _end are previous traffic. A
    # partially received Remaining Length must read as "need more data", never
    # continue into them.
    decoder = IncrementalDecoder(max_packet_size=16 * 1024 * 1024)
    decoder.feed(bytes([0x40, len(stale_body)]) + stale_body)
    assert decoder.next_packet() is not None
    assert decoder.buffered == 0

    decoder.feed(committed_header)
    assert bytes(decoder._buf[: len(committed_header) + len(stale_body)]) != committed_header

    assert decoder.peek_packet_bounds() is None
    assert decoder.head_frame_ready() is False
    assert decoder.next_packet() is None


def test_a_partial_header_still_resumes_once_the_real_bytes_arrive() -> None:
    decoder = IncrementalDecoder(max_packet_size=16 * 1024 * 1024)
    decoder.feed(bytes([0x40, 0x03, 0xFF, 0xFF, 0x10]))
    assert decoder.next_packet() is not None

    decoder.feed(b"\x30\x80")
    assert decoder.next_packet() is None
    decoder.feed(bytes([0x02, 0x00, 0x01, 0x74]) + b"p" * 253)

    packet = decoder.next_packet()
    assert packet is not None
    assert len(packet.remaining) == 256
    assert decoder.buffered == 0


def test_an_oversize_length_still_raises_when_it_is_genuinely_committed() -> None:
    # Bounding the VBI must not mask a real oversize frame.
    decoder = IncrementalDecoder(max_packet_size=1024)
    decoder.feed(bytes([0x30, 0x80, 0x80, 0x80, 0x10]))
    with pytest.raises(PacketTooLargeError):
        decoder.next_packet()


def test_a_genuinely_malformed_vbi_still_raises() -> None:
    decoder = IncrementalDecoder()
    decoder.feed(bytes([0x30, 0x80, 0x80, 0x80, 0x80, 0x01]))
    with pytest.raises(MalformedPacketError):
        decoder.next_packet()


@pytest.mark.parametrize("seed", range(20))
def test_random_split_streams_never_frame_stale_bytes(seed: int) -> None:
    # Same oracle, but with arbitrary receive boundaries over mixed traffic.
    rng = random.Random(seed)
    frames = [_publish(rng.choice([0, 5, 130, 500, 20_000])) for _ in range(30)]
    stream = b"".join(frames)

    decoder = IncrementalDecoder(max_packet_size=16 * 1024 * 1024)
    decoded = 0
    offset = 0
    while offset < len(stream):
        take = min(rng.randint(1, 900), len(stream) - offset)
        window = decoder.writable_window(take)
        window[:take] = stream[offset : offset + take]
        window.release()
        decoder.commit(take)
        offset += take
        while decoder.next_packet() is not None:
            decoded += 1

    assert decoded == len(frames)


def test_coalesced_small_frames_do_not_pin_a_multi_mib_slab() -> None:
    # Batches of small frames routinely exceed DEFAULT_CAPACITY in aggregate. A
    # single threshold would read that as "the big slab is still in use" and pin
    # megabytes for a workload that needs a few hundred KiB.
    decoder = IncrementalDecoder(max_packet_size=16 * 1024 * 1024)
    decoder.feed(_publish(4 * 1024 * 1024))
    assert decoder.next_packet() is not None
    grown = decoder.capacity
    assert grown >= 8 * 1024 * 1024

    batch = _publish(64 * 1024) * 5  # ~320 KiB live per drain, all small frames
    for _ in range(4 * _OVERSIZE_RETENTION):
        decoder.feed(batch)
        while decoder.next_packet() is not None:
            pass

    assert decoder.capacity < grown // 4
    assert decoder.capacity >= len(batch)  # still fits the batches it actually sees


def test_repeated_large_frames_keep_their_slab() -> None:
    decoder = IncrementalDecoder(max_packet_size=16 * 1024 * 1024)
    frame = _publish(2 * 1024 * 1024)
    reallocations = 0
    original = IncrementalDecoder._reallocate

    def counting(self: IncrementalDecoder, capacity: int) -> None:
        nonlocal reallocations
        reallocations += 1
        original(self, capacity)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(IncrementalDecoder, "_reallocate", counting)
        for _ in range(4 * _OVERSIZE_RETENTION):
            decoder.feed(frame)
            assert decoder.next_packet() is not None

    assert reallocations == 1


def test_commit_is_bounded_by_the_window_that_was_offered() -> None:
    decoder = IncrementalDecoder(max_packet_size=16 * 1024 * 1024)
    decoder.feed(_publish(4 * 1024 * 1024))
    assert decoder.next_packet() is not None

    window = decoder.writable_window()
    offered = len(window)
    window.release()
    assert offered < decoder.capacity  # a retained slab has far more room
    with pytest.raises(ValueError):
        decoder.commit(offered + 1)


def test_a_large_frame_is_sized_exactly_rather_than_doubled_past() -> None:
    # Doubling costs a whole extra frame when the frame sits just above a step.
    limit = 4 * 1024 * 1024
    decoder = IncrementalDecoder(max_packet_size=limit)
    frame = _publish(limit - 200)

    offset = 0
    while offset < len(frame):
        window = decoder.writable_window()
        take = min(len(window), 4096, len(frame) - offset)
        window[:take] = frame[offset : offset + take]
        window.release()
        decoder.commit(take)
        offset += take

    assert decoder.next_packet() is not None
    assert decoder.capacity < 2 * len(frame)
    assert decoder.capacity >= len(frame)
