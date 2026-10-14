"""Deterministic wire corpus shared by Python and native codec processes."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from enum import Enum
import hashlib
import json
import random

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.primitives import pack_utf8, unpack_u16, unpack_u32, unpack_utf8
from mqttium.codec.properties import (
    ALL_PACKETS,
    PUBLISH,
    _SPECS,
    decode_properties,
    encode_properties,
)
from mqttium.codec.vbi import decode_vbi, encode_vbi
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.packets._bindings import bind_codec
from mqttium.packets._publish import encode_publish_item_v311, encode_publish_item_v5
from mqttium.types import Properties


def normalize(value):
    if isinstance(value, bytes):
        return {"length": len(value), "sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Properties):
        return normalize(dict(value.values))
    if is_dataclass(value):
        return normalize({f.name: getattr(value, f.name) for f in fields(value)})
    if isinstance(value, (tuple, list)):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    return value


def outcome(fn, *args, **kwargs):
    try:
        return normalize(fn(*args, **kwargs))
    except Exception as exc:
        return [type(exc).__name__, str(exc)]


def main():
    rng = random.Random(1701)
    rows = []
    for _ in range(1000):
        data = rng.randbytes(rng.randrange(40))
        offset = rng.choice([0, 1, -1, len(data), len(data) + 1, 10**30, -(10**30)])
        for make in (bytes, bytearray, memoryview):
            for fn in (decode_vbi, unpack_u16, unpack_u32, unpack_utf8):
                rows.append(outcome(fn, make(data), offset))
        rows.append(outcome(decode_properties, data, 0, PUBLISH))
        for version in MQTTProtocolVersion:
            bindings = bind_codec(version)
            for name in (
                "decode_puback",
                "decode_pubrec",
                "decode_pubrel",
                "decode_pubcomp",
                "decode_connack",
                "decode_suback",
                "decode_unsuback",
                "decode_disconnect",
                "decode_auth",
            ):
                rows.append(outcome(getattr(bindings, name), data))
    for value in (0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455):
        wire = encode_vbi(value)
        rows.append(outcome(decode_vbi, wire))
        for end in range(len(wire) + 1):
            rows.append(outcome(decode_vbi, wire + b"stale", 0, end))
    for encode in (encode_publish_item_v311, encode_publish_item_v5):
        for mid in (None, 0, 1, 65535, 65536, 1.5, True, 10**30):
            for qos in (0, 1, 2, 3):
                for payload in (b"", b"x", b"x" * 131072, bytearray(b"mutable")):
                    rows.append(
                        outcome(
                            encode,
                            "t/é",
                            payload,
                            qos=qos,
                            mid=mid,
                            retain=False,
                            dup=False,
                            properties=None,
                        )
                    )
    rows.extend(fragmented_packets())
    rows.extend(property_contexts())
    rows.extend(stream_operations())
    print(json.dumps(rows, ensure_ascii=True, sort_keys=True))


def fragmented_packets():
    rows = []
    for version in MQTTProtocolVersion:
        for qos in QoS:
            for size in (0, 64, 4096, 131071, 131072, 262145):
                packet = PublishPacket(
                    "sensor/é", b"x" * size, qos, False, False, mid=17 if qos else None
                )
                wire = packet.encode(version)
                rows.append(normalize(wire))
                decoder = IncrementalDecoder()
                received = []
                for offset in range(0, len(wire), 73):
                    decoder.feed(wire[offset : offset + 73])
                    received.extend(decoder.drain_packets())
                assert len(received) == 1
                raw = received[0]
                owned = raw.remaining
                decoder.clear()
                decoder.feed(b"\x30\x03\x00\x01x")
                assert raw.remaining is owned
                rows.append(normalize(PublishPacket.decode(raw.flags, raw.remaining, version)))
    return rows


def property_contexts():
    rows = []
    examples = {
        "B": 1,
        "2I": 65535,
        "4I": 4294967295,
        "VBI": 268435455,
        "S": "sensor/é",
        "SP": (("site", "été"), ("site", "north")),
        "BIN": b"\x00\xff",
    }
    for spec in _SPECS:
        value = examples[spec.type.value]
        if spec.name == "subscription_identifier":
            value = (value,)
        props = Properties({spec.name: value})
        for packet in sorted(ALL_PACKETS):
            rows.append(outcome(encode_properties, props, packet))
            if packet not in spec.packets:
                continue
            wire = encode_properties(props, packet)
            decoded, end = decode_properties(wire, 0, packet)
            assert end == len(wire)
            rows.append(normalize(decoded))
            for stop in range(len(wire)):
                rows.append(outcome(decode_properties, wire[:stop], 0, packet))
    for value in ("", "ASCII", "été/東京/🌡", "\x00", "\ud800", "x" * 65535, "é" * 32768):
        rows.append(outcome(pack_utf8, value))
    return rows


def stream_operations():
    rng = random.Random(613)
    rows = []
    decoder = IncrementalDecoder()
    for trial in range(120):
        wire = b"".join(
            PublishPacket("t/é", rng.randbytes(rng.randrange(4097)), 0, False, False).encode()
            for _ in range(4)
        )
        received = []
        offset = 0
        while offset < len(wire):
            size = min(rng.randrange(1, 1025), len(wire) - offset)
            if trial % 2:
                view = decoder.writable_window()
                size = min(size, len(view))
                view[:size] = wire[offset : offset + size]
                view.release()
                decoder.commit(size)
            else:
                decoder.feed(memoryview(wire)[offset : offset + size])
            offset += size
            while True:
                ready = decoder.peek_packet_bounds()
                packet = decoder.next_packet()
                assert (ready is None) == (packet is None)
                if packet is None:
                    break
                received.append(normalize(packet))
        assert len(received) == 4 and decoder.buffered == 0
        rows.append(received)
        decoder.clear()
    for _ in range(2000):
        decoder = IncrementalDecoder(max_packet_size=4096)
        decoder.feed(rng.randbytes(rng.randrange(64)))
        rows.append(outcome(decoder.peek_packet_bounds))
        rows.append(outcome(decoder.next_packet))
    return rows


if __name__ == "__main__":
    main()
