"""Correctness and compact-state tests for PacketIdPool."""

from __future__ import annotations

import random

import pytest

from mqttium.errors import FlowControlError
from mqttium.protocol.packet_ids import PacketIdPool


def test_allocates_full_mqtt_range_then_exhausts() -> None:
    pool = PacketIdPool()

    assert [pool.allocate() for _ in range(65_535)] == list(range(1, 65_536))
    assert len(pool) == 65_535
    assert pool.available == 0
    with pytest.raises(FlowControlError):
        pool.allocate()


def test_release_reuses_hole_and_empty_pool_restarts_at_one() -> None:
    pool = PacketIdPool()
    mids = [pool.allocate() for _ in range(3)]

    pool.release(2)
    assert pool.allocate() == 2
    for mid in mids:
        pool.release(mid)

    assert len(pool) == 0
    assert pool.available == 65_535
    assert pool.allocate() == 1


def test_reserve_out_of_order_ids_are_skipped_and_folded_into_frontier() -> None:
    pool = PacketIdPool()

    for mid in (5, 3, 4):
        pool.reserve(mid)

    assert [pool.allocate(), pool.allocate()] == [1, 2]
    assert pool.allocate() == 6
    assert all(pool.in_use(mid) for mid in range(1, 7))
    assert len(pool) == 6


def test_reserve_released_mid_removes_the_hole() -> None:
    pool = PacketIdPool()
    assert [pool.allocate() for _ in range(4)] == [1, 2, 3, 4]

    pool.release(2)
    pool.release(3)
    pool.reserve(2)
    pool.reserve(3)

    assert pool.allocate() == 5
    assert len(pool) == 5


def test_multiple_holes_are_reused_without_duplicates() -> None:
    pool = PacketIdPool()
    allocated = [pool.allocate() for _ in range(20)]
    for mid in (2, 5, 8, 13):
        pool.release(mid)

    refilled = {pool.allocate() for _ in range(4)}
    assert refilled == {2, 5, 8, 13}
    assert len(pool) == len(allocated)


def test_duplicate_reserve_and_release_are_idempotent() -> None:
    pool = PacketIdPool()

    pool.reserve(10)
    pool.reserve(10)
    assert len(pool) == 1
    pool.release(10)
    pool.release(10)
    assert len(pool) == 0


def test_invalid_ids_are_rejected_or_ignored() -> None:
    pool = PacketIdPool()

    for mid in (0, 65_536):
        with pytest.raises(ValueError):
            pool.reserve(mid)
        pool.release(mid)
        assert not pool.in_use(mid)


def test_sequential_allocation_retains_no_per_mid_container() -> None:
    pool = PacketIdPool()

    for _ in range(30_000):
        pool.allocate()

    assert len(pool) == 30_000
    assert pool._free_many is None
    assert pool._reserved is None


def test_exhaustion_after_last_out_of_order_reservation_keeps_state() -> None:
    pool = PacketIdPool()
    for mid in range(1, 65_535):
        assert pool.allocate() == mid
    pool.reserve(65_535)

    with pytest.raises(FlowControlError):
        pool.allocate()

    assert len(pool) == 65_535
    assert pool.in_use(65_535)


def test_randomized_operations_match_a_reference_set() -> None:
    randomizer = random.Random(0xC0FFEE)
    pool = PacketIdPool()
    used: set[int] = set()

    for _ in range(30_000):
        roll = randomizer.random()
        if roll < 0.50 and len(used) < 65_535:
            mid = pool.allocate()
            assert mid not in used
            used.add(mid)
        elif roll < 0.78 and used:
            mid = randomizer.choice(tuple(used))
            pool.release(mid)
            used.remove(mid)
        else:
            mid = randomizer.randint(1, 65_535)
            pool.reserve(mid)
            used.add(mid)

        assert len(pool) == len(used)
        assert pool.available == 65_535 - len(used)
        for probe in (1, 2, 3, 65_533, 65_534, 65_535, randomizer.randint(1, 65_535)):
            assert pool.in_use(probe) == (probe in used)
