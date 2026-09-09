"""Storage cost depends on live data, not completed heads or write offsets."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder, _INITIAL_WINDOW, _OVERSIZE_RETENTION
from mqttium.codec.vbi import encode_vbi

MIB = 1024 * 1024


def _frame(size: int) -> bytes:
    body = b"\x00\x01t" + b"p" * size
    return b"\x30" + encode_vbi(len(body)) + body


def _ingress(decoder: IncrementalDecoder, data: bytes, mode: str) -> None:
    if mode == "feed":
        decoder.feed(data)
        return
    offset = 0
    while offset < len(data):
        window = decoder.writable_window()
        size = min(len(window), len(data) - offset)
        window[:size] = data[offset : offset + size]
        window.release()
        decoder.commit(size)
        offset += size


@pytest.mark.parametrize("mode", ["feed", "push"])
@pytest.mark.parametrize("size", [MIB, 2 * MIB, 4 * MIB])
def test_complete_head_does_not_cap_growth_of_following_frames(monkeypatch, mode, size) -> None:
    # Leaving a complete frame in front is allowed. Its two-byte extent cannot
    # describe the following stream, or every fragment would reallocate.
    wire = b"\xd0\x00" + _frame(size)
    decoder = IncrementalDecoder()
    copied = 0
    reallocations = 0
    original = IncrementalDecoder._reallocate

    def tracked(self: IncrementalDecoder, capacity: int) -> None:
        nonlocal copied, reallocations
        copied += self.buffered
        reallocations += 1
        original(self, capacity)

    monkeypatch.setattr(IncrementalDecoder, "_reallocate", tracked)
    for offset in range(0, len(wire), 8192):
        _ingress(decoder, wire[offset : offset + 8192], mode)
    packets = decoder.drain_packets()
    assert len(packets) == 2
    assert packets[0].remaining == b""
    assert packets[1].remaining == b"\x00\x01t" + b"p" * size
    assert copied < 4 * len(wire)
    assert reallocations <= 12


def test_bounded_drains_keep_amortised_growth_beyond_per_packet_limit(monkeypatch) -> None:
    # An explicit feed may carry many individually valid frames. The packet
    # limit is not an aggregate backlog limit, even if decoding is incremental.
    decoder = IncrementalDecoder(max_packet_size=1024)
    small = _frame(32)
    batch = small * 1000
    copied = 0
    reallocations = 0
    original = IncrementalDecoder._reallocate

    def tracked(self: IncrementalDecoder, capacity: int) -> None:
        nonlocal copied, reallocations
        copied += self.buffered
        reallocations += 1
        original(self, capacity)

    monkeypatch.setattr(IncrementalDecoder, "_reallocate", tracked)
    count = 0
    for _ in range(32):
        decoder.feed(batch)
        count += len(decoder.drain_packets())
    count += len(decoder.drain_packets(limit=32_000))
    assert count == 32_000
    assert decoder.buffered == 0
    assert copied < 4 * 32 * len(batch)
    assert reallocations <= 12


@pytest.mark.parametrize("mode", ["feed", "push"])
@pytest.mark.parametrize("consume", ["next", "peek"])
def test_retirement_tracks_live_peak_not_traversed_bytes(mode: str, consume: str) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_frame(2 * MIB))
    assert decoder.next_packet() is not None
    large_capacity = decoder.capacity
    small = _frame(4096)
    half = len(small) // 2
    middle = small[half:] + small[:half]
    peak = 0

    def ingress(piece: bytes) -> None:
        nonlocal peak
        _ingress(decoder, piece, mode)
        peak = max(peak, decoder.buffered)

    def consume_one() -> None:
        if consume == "peek":
            bounds = decoder.peek_packet_bounds()
            assert bounds is not None
            decoder.consume_peeked_packet(bounds[2])
        else:
            packet = decoder.next_packet()
            assert packet is not None
            assert packet.remaining == b"\x00\x01t" + b"p" * 4096

    for index in range(_OVERSIZE_RETENTION):
        ingress(small + small[:half])
        consume_one()
        for _ in range(80):
            assert decoder.next_packet() is None
            ingress(middle)
            consume_one()
        # The physical offset travelled >256 KiB, but at most 6 KiB was live.
        ingress(small[half:])
        consume_one()
        assert decoder.buffered == 0
        if index < _OVERSIZE_RETENTION - 1:
            assert decoder.capacity == large_capacity
    assert peak == len(small) + half
    assert decoder.capacity == _INITIAL_WINDOW
    assert decoder._window_peak == 0

    # Continued small traffic must not churn after retirement.
    identity = decoder._buf
    for _ in range(2 * _OVERSIZE_RETENTION):
        ingress(small)
        consume_one()
    assert decoder._buf is identity


@pytest.mark.parametrize("mode", ["feed", "push"])
def test_retirement_preserves_genuine_coalesced_live_pressure(mode: str) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(_frame(4 * MIB))
    assert decoder.next_packet() is not None
    batch = _frame(4096) * 90
    for _ in range(_OVERSIZE_RETENTION):
        _ingress(decoder, batch, mode)
        assert decoder.buffered == len(batch)
        assert len(decoder.drain_packets()) == 90
    assert len(batch) <= decoder.capacity < 2 * len(batch)
    identity = decoder._buf
    for _ in range(2 * _OVERSIZE_RETENTION):
        _ingress(decoder, batch, mode)
        assert len(decoder.drain_packets()) == 90
    assert decoder._buf is identity
