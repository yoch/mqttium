"""Unit tests for VBI and incremental decoder."""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import _VIEW_COPY_THRESHOLD, IncrementalDecoder
from mqttium.codec.vbi import decode_vbi, encode_vbi, vbi_len
from mqttium.enums import PacketType, QoS
from mqttium.errors import MalformedPacketError, PacketTooLargeError, ProtocolError
from mqttium.packets import PublishPacket, encode_frame


@pytest.mark.parametrize(
    ("value", "expected_len"),
    [
        (0, 1),
        (127, 1),
        (128, 2),
        (16_383, 2),
        (16_384, 3),
        (2_097_151, 3),
        (2_097_152, 4),
        (268_435_455, 4),
    ],
)
def test_vbi_roundtrip(value: int, expected_len: int) -> None:
    encoded = encode_vbi(value)
    assert len(encoded) == expected_len
    assert vbi_len(value) == expected_len
    decoded, offset = decode_vbi(encoded)
    assert decoded == value
    assert offset == len(encoded)


def test_vbi_rejects_overlong() -> None:
    with pytest.raises(MalformedPacketError):
        decode_vbi(bytes([0x80, 0x80, 0x80, 0x80, 0x01]))


@pytest.mark.parametrize(
    "encoded",
    [
        b"\x80\x00",
        b"\x81\x00",
        b"\xff\x00",
        b"\x80\x81\x00",
    ],
)
def test_vbi_rejects_non_canonical_encoding(encoded: bytes) -> None:
    with pytest.raises(MalformedPacketError, match="Non-canonical"):
        decode_vbi(encoded)


@pytest.mark.parametrize("value", [-1, 268_435_456])
def test_vbi_len_rejects_out_of_range(value: int) -> None:
    with pytest.raises(ValueError):
        vbi_len(value)


def test_decoder_byte_by_byte_publish() -> None:
    packet = PublishPacket(topic="a/b", payload=b"hi", qos=0, retain=False, dup=False)
    wire = packet.encode()
    dec = IncrementalDecoder()
    got = None
    for i in range(len(wire)):
        dec.feed(wire[i : i + 1])
        got = dec.next_packet()
        if got is not None:
            break
    assert got is not None
    assert got.packet_type is PacketType.PUBLISH
    parsed = PublishPacket.decode(got.flags, got.remaining)
    assert parsed.topic == "a/b"
    assert parsed.payload == b"hi"


def test_decoder_multi_packet_chunk() -> None:
    p1 = PublishPacket(topic="t1", payload=b"1", qos=0, retain=False, dup=False).encode()
    p2 = PublishPacket(topic="t2", payload=b"2", qos=0, retain=False, dup=False).encode()
    dec = IncrementalDecoder()
    dec.feed(p1 + p2)
    packets = dec.drain_packets()
    assert len(packets) == 2
    assert PublishPacket.decode(packets[0].flags, packets[0].remaining).topic == "t1"
    assert PublishPacket.decode(packets[1].flags, packets[1].remaining).topic == "t2"


def test_decoder_enforces_max_size() -> None:
    huge = encode_frame(PacketType.PUBLISH, 0, b"x" * 100)
    dec = IncrementalDecoder(max_packet_size=50)
    dec.feed(huge)
    with pytest.raises(PacketTooLargeError):
        dec.next_packet()


def test_decoder_incomplete_waits() -> None:
    packet = PublishPacket(
        topic="wait", payload=b"payload", qos=0, retain=False, dup=False
    ).encode()
    dec = IncrementalDecoder()
    dec.feed(packet[:3])
    assert dec.next_packet() is None
    dec.feed(packet[3:])
    assert dec.next_packet() is not None


def test_decoder_process_packets_bounded_by_bytes() -> None:
    wires = [
        PublishPacket(
            topic=f"t/{index}",
            payload=b"x" * 32,
            qos=0,
            retain=False,
            dup=False,
        ).encode()
        for index in range(3)
    ]
    decoder = IncrementalDecoder()
    decoder.feed(b"".join(wires))
    seen = []

    count, decoded_bytes = decoder.process_packets_bounded(
        seen.append,
        limit=256,
        max_bytes=1,
    )

    assert count == 1
    assert decoded_bytes >= 1
    assert len(seen) == 1
    assert len(decoder.drain_packets()) == 2


@pytest.mark.parametrize("limit", [0, -1])
def test_decoder_bounded_batch_rejects_invalid_byte_limit(limit: int) -> None:
    decoder = IncrementalDecoder()
    with pytest.raises(ValueError, match="max_bytes"):
        decoder.process_packets_bounded(lambda packet: None, limit=1, max_bytes=limit)


@pytest.mark.parametrize(
    "payload_size",
    [
        0,
        1,
        _VIEW_COPY_THRESHOLD - 1,
        _VIEW_COPY_THRESHOLD,
        _VIEW_COPY_THRESHOLD + 1,
        4 * _VIEW_COPY_THRESHOLD,
    ],
)
def test_both_body_copy_branches_produce_identical_owned_bytes(payload_size: int) -> None:
    payload = bytes(range(256)) * (payload_size // 256) + bytes(range(payload_size % 256))
    assert len(payload) == payload_size
    wire = PublishPacket(
        topic="t/threshold",
        payload=payload,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=9,
    ).encode()

    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()

    assert raw is not None
    assert type(raw.remaining) is bytes
    packet = PublishPacket.decode(raw.flags, raw.remaining)
    assert packet.payload == payload
    assert packet.topic == "t/threshold"

    decoder.feed(wire)
    again = decoder.next_packet()
    assert again is not None
    assert again.remaining == raw.remaining


def test_a_large_body_is_not_aliased_to_the_reusable_buffer() -> None:
    payload = b"\xa5" * (4 * _VIEW_COPY_THRESHOLD)
    wire = PublishPacket(
        topic="t/alias",
        payload=payload,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    ).encode()

    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None

    snapshot = bytes(raw.remaining)
    decoder.feed(b"\xff" * 512)
    decoder.clear()
    decoder.feed(wire)
    decoder.next_packet()
    assert raw.remaining == snapshot


def test_validate_utf8_rejects_exactly_what_encode_utf8_rejects() -> None:
    """The validator exists to skip the encode, not to change the rules."""
    from mqttium.codec.primitives import encode_utf8, validate_utf8

    cases = [
        "sensors/t",
        "a",
        "",
        "é",
        "温度/センサー",
        "🎉/emoji",
        "mixed/é/x",
        "x" * 65535,
        "x" * 65536,
        "é" * 32767,
        "é" * 32768,
        "a\x00b",
        "\x00",
        "﻿",
        "a﻿b",
        "\ud800",
        "a\ud800b",
        b"bytes",
        None,
        42,
    ]

    def outcome(fn, value):
        try:
            fn(value)
        except ProtocolError:
            return "reject"
        return "accept"

    for case in cases:
        assert outcome(encode_utf8, case) == outcome(validate_utf8, case), repr(case)[:40]


def test_validate_utf8_bounds_length_in_bytes_not_characters() -> None:
    """The MQTT limit is 65535 bytes; skipping the encode must not lose that."""
    from mqttium.codec.primitives import validate_utf8

    validate_utf8("é" * 32767)  # 65534 bytes
    with pytest.raises(ProtocolError, match="too long"):
        validate_utf8("é" * 32768)  # 65536 bytes, only 32768 characters

    validate_utf8("x" * 65535)
    with pytest.raises(ProtocolError, match="too long"):
        validate_utf8("x" * 65536)


def test_validate_utf8_does_not_encode_an_ascii_string(monkeypatch) -> None:
    """The whole point: an ASCII topic is validated without producing bytes."""
    from mqttium.codec import primitives

    encoded: list[str] = []
    original = str.encode

    class Tracking(str):
        def encode(self, *args, **kwargs):  # type: ignore[override]
            encoded.append(str(self))
            return original(self, *args, **kwargs)

    primitives.validate_utf8(Tracking("sensors/temperature/room-12"))
    assert encoded == []

    primitives.validate_utf8(Tracking("capteurs/température"))
    assert len(encoded) == 1, "a non-ASCII string still needs its byte length"
