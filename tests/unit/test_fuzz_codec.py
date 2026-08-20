"""Fuzz / malformed-packet resilience tests."""

from __future__ import annotations

import random

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.errors import MQTTError, PacketTooLargeError


@pytest.mark.parametrize(
    "blob",
    [
        b"\x30\xff\xff\xff\xff\x01",  # overlong VBI
        b"\x30\x80\x80\x80\x80\x00",  # 5-byte VBI
        b"\xff\x00",  # unknown type
        b"\x30\x02\x00",  # truncated publish
    ],
)
def test_malformed_does_not_crash(blob: bytes) -> None:
    dec = IncrementalDecoder(max_packet_size=1024)
    try:
        dec.feed(blob)
        while True:
            pkt = dec.next_packet()
            if pkt is None:
                break
    except MQTTError:
        pass


def test_oversized_packet_rejected() -> None:
    # Remaining length claims 2000 bytes, max is 100.
    dec = IncrementalDecoder(max_packet_size=100)
    dec.feed(b"\x30\x88\x0f" + b"x" * 10)  # VBI for 1928?
    # Simpler: remaining length 200 via two-byte VBI 0xC8 0x01 = 200
    dec2 = IncrementalDecoder(max_packet_size=50)
    dec2.feed(bytes([0x30, 0xC8, 0x01]) + b"x" * 20)
    with pytest.raises(PacketTooLargeError):
        dec2.next_packet()


def test_seeded_noise_stable() -> None:
    rng = random.Random(20260820)
    dec = IncrementalDecoder(max_packet_size=4096)
    for _ in range(100):
        dec.feed(rng.randbytes(64))
        try:
            while dec.next_packet() is not None:
                pass
        except MQTTError:
            dec.clear()
