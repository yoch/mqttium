"""Differential framing checks for pull-feed versus decoder-owned ingress."""

from __future__ import annotations

import os

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings, strategies as st

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.errors import MQTTError

settings.register_profile("direct_production", max_examples=2500, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "direct_production"))

_ALLOWED = (MQTTError, ValueError)


def _logical_bytes(decoder: IncrementalDecoder) -> bytes:
    return bytes(decoder._buf[decoder._start : decoder._end])


def _push(decoder: IncrementalDecoder, data: bytes, preferred: int) -> None:
    offset = 0
    while offset < len(data):
        window = decoder.writable_window(preferred)
        take = min(len(window), len(data) - offset)
        window[:take] = data[offset : offset + take]
        window.release()
        decoder.commit(take)
        offset += take


def _packet_result(decoder: IncrementalDecoder):
    try:
        return ("ok", decoder.next_packet())
    except _ALLOWED as exc:
        return ("exc", type(exc))


def _assert_same_state(left: IncrementalDecoder, right: IncrementalDecoder) -> None:
    assert right.buffered == left.buffered
    assert _logical_bytes(right) == _logical_bytes(left)
    assert right.next_header_byte == left.next_header_byte
    assert right.max_packet_size == left.max_packet_size


def _drain_equally(left: IncrementalDecoder, right: IncrementalDecoder) -> bool:
    for _ in range(4096):
        a = _packet_result(left)
        b = _packet_result(right)
        assert a[0] == b[0]
        if a[0] == "exc":
            assert a[1] is b[1]
            return False
        lp = a[1]
        rp = b[1]
        assert (lp is None) == (rp is None)
        if lp is None:
            _assert_same_state(left, right)
            return True
        assert rp is not None
        assert rp.packet_type is lp.packet_type
        assert rp.flags == lp.flags
        assert rp.remaining == lp.remaining
        assert isinstance(lp.remaining, bytes)
        assert isinstance(rp.remaining, bytes)
        _assert_same_state(left, right)
    raise AssertionError("decoder failed to quiesce")


_feed = st.tuples(
    st.just("feed"),
    st.binary(max_size=192),
    st.integers(min_value=1, max_value=256),
)
_clear = st.tuples(st.just("clear"), st.just(b""), st.just(1))


@given(
    max_packet_size=st.sampled_from(
        [1, 2, 3, 4, 5, 16, 127, 128, 129, 255, 256, 1024, 4096, 16_384]
    ),
    operations=st.lists(st.one_of(_feed, _clear), min_size=1, max_size=48),
)
@settings(deadline=None)
def test_writable_ingress_matches_pull_feed_for_fragmented_streams(
    max_packet_size: int,
    operations: list[tuple[str, bytes, int]],
) -> None:
    """Packets/errors/ownership are independent of how bytes enter the slab."""
    pull = IncrementalDecoder(max_packet_size=max_packet_size)
    push = IncrementalDecoder(max_packet_size=max_packet_size)

    for operation, payload, preferred in operations:
        if operation == "clear":
            pull.clear()
            push.clear()
            _assert_same_state(pull, push)
            continue
        pull.feed(payload)
        _push(push, payload, preferred)
        _assert_same_state(pull, push)
        if not _drain_equally(pull, push):
            return


@pytest.mark.parametrize(
    "encoded_remaining_length",
    [
        b"\x80\x00",
        b"\x81\x00",
        b"\x80\x81\x00",
        b"\x80\x80\x80\x80",
        b"\xff\xff\xff\xff\x00",
    ],
)
def test_writable_ingress_matches_malformed_vbi_contract(encoded_remaining_length: bytes) -> None:
    wire = b"\x30" + encoded_remaining_length
    pull = IncrementalDecoder(max_packet_size=1024)
    push = IncrementalDecoder(max_packet_size=1024)
    pull.feed(wire)
    _push(push, wire, 1)
    left = _packet_result(pull)
    right = _packet_result(push)
    assert left[0] == right[0] == "exc"
    assert left[1] is right[1]


@pytest.mark.parametrize(
    ("remaining_length", "limit"),
    [
        (0, 1), (0, 2), (127, 64), (127, 129), (128, 129), (128, 131),
        (16_383, 1024), (16_384, 16_387), (2_097_151, 4096),
        (2_097_152, 4096), (268_435_455, 16 * 1024 * 1024),
    ],
)
def test_writable_ingress_matches_size_limit_decision_without_body(
    remaining_length: int, limit: int
) -> None:
    from mqttium.codec.vbi import encode_vbi

    encoded = encode_vbi(remaining_length)
    wire = b"\x30" + encoded
    pull = IncrementalDecoder(max_packet_size=limit)
    push = IncrementalDecoder(max_packet_size=limit)
    pull.feed(wire)
    _push(push, wire, 1)
    survived = _drain_equally(pull, push)
    total_size = 1 + len(encoded) + remaining_length
    assert survived is (total_size <= limit)
