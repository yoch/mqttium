"""Focused accounting tests for the compact PacketIdPool representation."""

from __future__ import annotations

from mqttium.protocol.packet_ids import PacketIdPool


def test_free_count_tracks_release_allocate_and_reserve() -> None:
    pool = PacketIdPool()
    assert [pool.allocate() for _ in range(5)] == [1, 2, 3, 4, 5]

    pool.release(2)
    pool.release(4)

    assert pool._free_count == 2
    assert len(pool) == 3

    pool.reserve(2)
    assert pool._free_count == 1
    assert len(pool) == 4

    assert pool.allocate() == 4
    assert pool._free_count == 0
    assert len(pool) == 5


def test_duplicate_release_does_not_change_free_count() -> None:
    pool = PacketIdPool()
    first = pool.allocate()
    second = pool.allocate()

    pool.release(first)
    pool.release(first)

    assert pool._free_count == 1
    assert len(pool) == 1
    assert pool.in_use(second)

    pool.release(second)
    assert pool._free_count == 0
    assert len(pool) == 0
    assert pool.allocate() == 1


def test_high_reservation_release_does_not_change_low_hole_count() -> None:
    pool = PacketIdPool()
    pool.reserve(100)
    pool.release(100)

    assert pool._free_count == 0
    assert len(pool) == 0
    assert pool.allocate() == 1
