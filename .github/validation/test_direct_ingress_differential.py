"""Differential contract checks for the direct-ingress decoder prototype."""

from __future__ import annotations

import os

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings, strategies as st

from mqttium._direct_decoder_ingress_prototype import DirectIngressDecoder
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.errors import MQTTError

settings.register_profile("direct_prepromotion", max_examples=2500, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "direct_prepromotion"))

_ALLOWED = (MQTTError, ValueError)


def _logical_bytes(decoder: IncrementalDecoder) -> bytes:
    if isinstance(decoder, DirectIngressDecoder):
        return bytes(decoder._buf[decoder._start : decoder._end])
    return bytes(decoder._buf[decoder._start :])


def _packet_result(decoder: IncrementalDecoder):
    try:
        return ("ok", decoder.next_packet())
    except _ALLOWED as exc:
        return ("exc", type(exc))


def _assert_same_state(
    standard: IncrementalDecoder,
    direct: DirectIngressDecoder,
) -> None:
    assert direct.buffered == standard.buffered
    assert _logical_bytes(direct) == _logical_bytes(standard)
    assert direct.next_header_byte == standard.next_header_byte
    assert direct.max_packet_size == standard.max_packet_size


def _drain_equally(
    standard: IncrementalDecoder,
    direct: DirectIngressDecoder,
) -> bool:
    """Drain both decoders until incomplete or an equivalent fatal error.

    Returns False when a fatal decode error was observed; state after a fatal
    protocol error is deliberately outside the comparison contract.
    """
    for _ in range(4096):
        left = _packet_result(standard)
        right = _packet_result(direct)
        assert left[0] == right[0]
        if left[0] == "exc":
            assert left[1] is right[1]
            return False

        left_packet = left[1]
        right_packet = right[1]
        assert (left_packet is None) == (right_packet is None)
        if left_packet is None:
            _assert_same_state(standard, direct)
            return True

        assert right_packet is not None
        assert right_packet.packet_type is left_packet.packet_type
        assert right_packet.flags == left_packet.flags
        assert right_packet.remaining == left_packet.remaining
        assert isinstance(left_packet.remaining, bytes)
        assert isinstance(right_packet.remaining, bytes)
        _assert_same_state(standard, direct)
    raise AssertionError("decoder failed to quiesce")


_feed = st.tuples(st.just("feed"), st.binary(max_size=192))
_clear = st.tuples(st.just("clear"), st.just(b""))


@given(
    max_packet_size=st.sampled_from(
        [1, 2, 3, 4, 5, 16, 127, 128, 129, 255, 256, 1024, 4096, 16_384]
    ),
    operations=st.lists(st.one_of(_feed, _clear), min_size=1, max_size=48),
)
@settings(deadline=None)
def test_direct_decoder_matches_incremental_decoder_for_fragmented_streams(
    max_packet_size,
    operations,
):
    """Compare packets, errors, ownership and unconsumed logical bytes.

    Arbitrary byte chunks deliberately cover valid frames, malformed headers,
    non-canonical VBIs, oversize declarations, concatenated frames and arbitrary
    fragmentation. ``clear`` operations additionally exercise reuse.
    """
    standard = IncrementalDecoder(max_packet_size=max_packet_size)
    direct = DirectIngressDecoder(max_packet_size=max_packet_size)

    for operation, payload in operations:
        if operation == "clear":
            standard.clear()
            direct.clear()
            _assert_same_state(standard, direct)
            continue

        standard.feed(payload)
        direct.feed(payload)
        _assert_same_state(standard, direct)
        if not _drain_equally(standard, direct):
            return


@pytest.mark.parametrize(
    ("encoded_remaining_length", "expected_exception"),
    [
        (b"\x80\x00", True),  # non-canonical zero
        (b"\x81\x00", True),  # non-canonical one
        (b"\x80\x81\x00", True),  # non-canonical 128
        (b"\x80\x80\x80\x80", True),  # four continuation bytes: impossible VBI
        (b"\xff\xff\xff\xff\x00", True),  # fifth VBI byte
    ],
)
def test_direct_decoder_matches_malformed_vbi_contract(
    encoded_remaining_length,
    expected_exception,
):
    wire = b"\x30" + encoded_remaining_length
    standard = IncrementalDecoder(max_packet_size=1024)
    direct = DirectIngressDecoder(max_packet_size=1024)
    standard.feed(wire)
    direct.feed(wire)

    left = _packet_result(standard)
    right = _packet_result(direct)
    assert left[0] == right[0] == ("exc" if expected_exception else "ok")
    if expected_exception:
        assert left[1] is right[1]


@pytest.mark.parametrize(
    ("remaining_length", "limit"),
    [
        (0, 1),
        (0, 2),
        (127, 64),
        (127, 129),
        (128, 129),
        (128, 131),
        (16_383, 1024),
        (16_384, 16_387),
        (2_097_151, 4096),
        (2_097_152, 4096),
        (268_435_455, 16 * 1024 * 1024),
    ],
)
def test_direct_decoder_matches_size_limit_decision_without_body(
    remaining_length,
    limit,
):
    # Build only the fixed header. Oversize decisions are required as soon as
    # Remaining Length is known. RL=0 is already a complete frame; non-zero
    # in-range declarations remain incomplete until body bytes arrive.
    from mqttium.codec.vbi import encode_vbi

    encoded = encode_vbi(remaining_length)
    wire = b"\x30" + encoded
    standard = IncrementalDecoder(max_packet_size=limit)
    direct = DirectIngressDecoder(max_packet_size=limit)
    standard.feed(wire)
    direct.feed(wire)

    survived = _drain_equally(standard, direct)
    total_size = 1 + len(encoded) + remaining_length
    assert survived is (total_size <= limit)
