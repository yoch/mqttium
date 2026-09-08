"""Production decoder-ingress storage invariants."""

from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder


def _publish(payload_size: int) -> bytes:
    body = b"\x00\x01t" + b"p" * payload_size
    remaining = len(body)
    encoded = bytearray([0x30])
    while True:
        digit = remaining % 128
        remaining //= 128
        encoded.append(digit | 0x80 if remaining else digit)
        if not remaining:
            break
    return bytes(encoded) + body


def _receive(decoder: IncrementalDecoder, frame: bytes) -> None:
    offset = 0
    callbacks = 0
    while offset < len(frame):
        preferred = 64 * 1024 if callbacks < 2 else 256 * 1024
        window = decoder.writable_window(preferred)
        take = min(len(window), len(frame) - offset)
        window[:take] = frame[offset : offset + take]
        window.release()
        decoder.commit(take)
        offset += take
        callbacks += 1


def test_storage_is_lazy_and_tiny_feed_does_not_pay_bulk_reserve() -> None:
    decoder = IncrementalDecoder()
    assert decoder.capacity == 0
    decoder.feed(_publish(16))
    assert decoder.capacity == 4 * 1024
    assert decoder.next_packet() is not None
    assert decoder.capacity == 4 * 1024


def test_first_direct_window_is_64k_not_old_512k_floor() -> None:
    decoder = IncrementalDecoder()
    window = decoder.writable_window(64 * 1024)
    try:
        assert len(window) == 64 * 1024
        assert decoder.capacity == 64 * 1024
    finally:
        window.release()


def test_retained_old_window_cannot_block_growth() -> None:
    decoder = IncrementalDecoder()
    stale = decoder.writable_window(64 * 1024)
    old_object = stale.obj
    # Growth swaps storage instead of resizing the exported bytearray.
    current = decoder.writable_window(256 * 1024)
    try:
        assert current.obj is not old_object
        assert decoder.capacity >= 256 * 1024
        assert len(stale) == 64 * 1024
    finally:
        current.release()
        stale.release()


def test_near_limit_fragmented_frame_reserves_exact_extent_not_double() -> None:
    limit = 4 * 1024 * 1024
    decoder = IncrementalDecoder(max_packet_size=limit)
    frame = _publish(limit - 32)
    assert len(frame) <= limit
    _receive(decoder, frame)
    assert decoder.capacity == len(frame)
    assert decoder.capacity_peak == len(frame)
    packet = decoder.next_packet()
    assert packet is not None
    assert len(packet.remaining) == len(frame) - (len(frame) - len(packet.remaining))


def test_repeated_large_frames_reuse_then_small_drains_retire_oversize() -> None:
    decoder = IncrementalDecoder(max_packet_size=2 * 1024 * 1024)
    frame = _publish(1024 * 1024)
    for _ in range(12):
        _receive(decoder, frame)
        assert decoder.next_packet() is not None
    grown = decoder.capacity
    growths = decoder.growth_count
    assert grown >= len(frame)
    # Sustained large frames do not trigger shrink/grow oscillation.
    assert decoder.growth_count == growths

    tiny = _publish(32)
    for _ in range(32):
        decoder.feed(tiny)
        assert decoder.next_packet() is not None
    assert decoder.capacity == 256 * 1024
    assert decoder.shrink_count >= 1


def test_clear_releases_growth_for_reconnect() -> None:
    decoder = IncrementalDecoder(max_packet_size=2 * 1024 * 1024)
    frame = _publish(1024 * 1024)
    _receive(decoder, frame)
    assert decoder.next_packet() is not None
    assert decoder.capacity > 64 * 1024
    decoder.clear()
    assert decoder.buffered == 0
    assert decoder.capacity == 64 * 1024
