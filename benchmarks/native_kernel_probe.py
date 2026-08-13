"""Diagnostic timings for compiled-kernel evaluation.

Reports nanoseconds per operation for codec, object construction, engine
ingress and WebSocket masking. Results are build artefacts: write them under
``/tmp`` (or ``--output``) and do not commit them.

This is not a paired A/B and not release evidence. Use it to decide whether a
compiled prototype moved a real kernel, then confirm with
``paired_regression.py`` on an eligible host.

    PYTHONPATH=src python benchmarks/native_kernel_probe.py
    PYTHONPATH=src python benchmarks/native_kernel_probe.py --output /tmp/native-kernel.json
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import unpack_utf8
from mqttium.codec.properties import PUBLISH, decode_properties, encode_properties
from mqttium.codec.vbi import append_vbi, decode_vbi, encode_vbi
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubAckPacket, PublishPacket, encode_frame
from mqttium.packets.publish import encode_publish_item
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import _decode_v311_qos0_message
from mqttium.topics import validate_received_publish_topic
from mqttium.transport.websocket import _mask_payload
from mqttium.types import Message, Properties

TOPIC = "bench/sensors/temp"
PAYLOAD = b'{"t":21.5,"h":40}'
TELEMETRY = b"x" * 256
PAYLOAD_4K = b"z" * 4096


def _ns(fn: Callable[[], object], operations: int, warmup: int = 2_000) -> float:
    for _ in range(warmup):
        fn()
    started = time.perf_counter()
    for _ in range(operations):
        fn()
    return (time.perf_counter() - started) * 1e9 / operations


def _measure() -> list[dict[str, Any]]:
    qos0 = PublishPacket(
        topic=TOPIC, payload=PAYLOAD, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    )
    qos0_256 = PublishPacket(
        topic=TOPIC, payload=TELEMETRY, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    )
    qos1 = PublishPacket(
        topic=TOPIC,
        payload=PAYLOAD,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=42,
    )
    wire0 = qos0.encode()
    wire256 = qos0_256.encode()
    ack = PubAckPacket(mid=42).encode()

    decoder = IncrementalDecoder()
    decoder.feed(wire0)
    raw0 = decoder.drain_packets()[0]
    decoder.feed(wire256)
    raw256 = decoder.drain_packets()[0]
    decoder.feed(qos1.encode())
    raw1 = decoder.drain_packets()[0]

    prefed = IncrementalDecoder()
    prefed.feed(wire0 * 64)

    def next_prefed() -> None:
        packet = prefed.next_packet()
        if packet is None:
            prefed.feed(wire0 * 64)
            packet = prefed.next_packet()
        assert packet is not None

    def feed_next(frame: bytes) -> Callable[[], None]:
        local = IncrementalDecoder()

        def run() -> None:
            local.feed(frame)
            packet = local.next_packet()
            assert packet is not None

        return run

    vbi_buf = bytearray()

    def append_small_vbi() -> None:
        vbi_buf.clear()
        append_vbi(vbi_buf, 40)

    def parse_fields() -> None:
        _topic, pos = unpack_utf8(raw0.remaining)
        _payload = raw0.remaining[pos:]

    def parse_validate() -> None:
        topic, pos = unpack_utf8(raw0.remaining)
        validate_received_publish_topic(topic, utf8_validated=True)
        _payload = raw0.remaining[pos:]

    def parse_message() -> None:
        topic, pos = unpack_utf8(raw0.remaining)
        validate_received_publish_topic(topic, utf8_validated=True)
        Message(
            topic=topic,
            payload=raw0.remaining[pos:],
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            mid=None,
            properties=None,
        )

    empty_props = Properties()
    props = Properties()
    props.set("message_expiry_interval", 60)
    props.add_user_property("trace", "x")
    encoded_props = encode_properties(props, PUBLISH)

    engine = ProtocolEngine(
        EngineConfig(client_id="probe-qos0", protocol=MQTTProtocolVersion.MQTTv311)
    )
    engine.state = ConnectionState.CONNECTED

    def engine_qos0() -> None:
        engine.handle_raw(raw0)
        engine.take_effects()

    engine256 = ProtocolEngine(
        EngineConfig(client_id="probe-qos0-256", protocol=MQTTProtocolVersion.MQTTv311)
    )
    engine256.state = ConnectionState.CONNECTED

    def engine_qos0_256() -> None:
        engine256.handle_raw(raw256)
        engine256.take_effects()

    qos1_engine = ProtocolEngine(
        EngineConfig(client_id="probe-qos1", protocol=MQTTProtocolVersion.MQTTv311),
        store=MemoryInflightStore(),
    )
    qos1_engine.begin_connect()
    connack = IncrementalDecoder()
    connack.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    qos1_engine.handle_raw(connack.drain_packets()[0])
    qos1_engine.take_effects()

    def qos1_cycle() -> None:
        handle = qos1_engine.queue_publish(TOPIC, PAYLOAD, qos=1)
        qos1_engine.take_effects()
        local = IncrementalDecoder()
        local.feed(PubAckPacket(mid=handle.mid or 0).encode())
        qos1_engine.handle_raw(local.drain_packets()[0])
        qos1_engine.take_effects()

    inbound_qos1 = ProtocolEngine(
        EngineConfig(client_id="probe-in-qos1", protocol=MQTTProtocolVersion.MQTTv311),
        store=MemoryInflightStore(),
    )
    inbound_qos1.begin_connect()
    inbound_connack = IncrementalDecoder()
    inbound_connack.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    inbound_qos1.handle_raw(inbound_connack.drain_packets()[0])
    inbound_qos1.take_effects()

    def inbound_qos1_once() -> None:
        inbound_qos1.handle_raw(raw1)
        inbound_qos1.take_effects()

    mask = b"\x01\x02\x03\x04"
    mask_64 = b"z" * 64
    mask_4k = b"z" * 4096
    mask_1m = b"z" * (1024 * 1024)

    specs: tuple[tuple[str, Callable[[], object], int, int], ...] = (
        ("empty_python_call", lambda: None, 500_000, 5_000),
        ("bytes_copy_18b", lambda: bytes(PAYLOAD), 300_000, 2_000),
        ("encode_vbi_one_byte", lambda: encode_vbi(40), 300_000, 2_000),
        ("append_vbi_one_byte", append_small_vbi, 300_000, 2_000),
        ("decode_vbi_one_byte", lambda: decode_vbi(b"\x28", 0), 300_000, 2_000),
        (
            "raw_packet_ctor",
            lambda: RawPacket(PacketType.PUBLISH, 0, raw0.remaining),
            200_000,
            2_000,
        ),
        ("unpack_utf8_payload_slice", parse_fields, 200_000, 2_000),
        ("parse_plus_topic_validation", parse_validate, 200_000, 2_000),
        ("decoder_feed_next_qos0_small", feed_next(wire0), 200_000, 2_000),
        ("decoder_feed_next_qos0_256b", feed_next(wire256), 200_000, 2_000),
        ("next_packet_prefed_qos0_small", next_prefed, 200_000, 2_000),
        (
            "message_fields_already_decoded",
            lambda: Message(
                topic=TOPIC,
                payload=PAYLOAD,
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                mid=None,
                properties=None,
            ),
            200_000,
            2_000,
        ),
        ("parse_plus_message", parse_message, 200_000, 2_000),
        (
            "decode_v311_qos0_message",
            lambda: _decode_v311_qos0_message(raw0),
            200_000,
            2_000,
        ),
        (
            "publish_packet_decode_qos0",
            lambda: PublishPacket.decode(0, raw0.remaining),
            120_000,
            2_000,
        ),
        (
            "encode_publish_item_qos0_small",
            lambda: encode_publish_item(
                TOPIC,
                PAYLOAD,
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                mid=None,
                properties=None,
            ),
            120_000,
            2_000,
        ),
        (
            "encode_publish_item_qos1_small",
            lambda: encode_publish_item(
                TOPIC,
                PAYLOAD,
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=42,
                properties=None,
            ),
            120_000,
            2_000,
        ),
        (
            "encode_publish_item_qos0_4kib",
            lambda: encode_publish_item(
                TOPIC,
                PAYLOAD_4K,
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                mid=None,
                properties=None,
            ),
            80_000,
            2_000,
        ),
        (
            "encode_properties_empty",
            lambda: encode_properties(empty_props, PUBLISH),
            200_000,
            2_000,
        ),
        (
            "encode_properties_two_fields",
            lambda: encode_properties(props, PUBLISH),
            100_000,
            2_000,
        ),
        (
            "decode_properties_two_fields",
            lambda: decode_properties(encoded_props, 0, PUBLISH),
            100_000,
            2_000,
        ),
        ("puback_encode_success", lambda: PubAckPacket(mid=42).encode(), 200_000, 2_000),
        ("puback_decode_v311", lambda: PubAckPacket.decode(ack[2:]), 200_000, 2_000),
        ("engine_handle_raw_qos0_small", engine_qos0, 80_000, 2_000),
        ("engine_handle_raw_qos0_256b", engine_qos0_256, 80_000, 2_000),
        ("engine_qos1_publish_puback_cycle", qos1_cycle, 30_000, 500),
        ("engine_inbound_qos1_handle_raw", inbound_qos1_once, 40_000, 500),
        ("websocket_mask_64b", lambda: _mask_payload(mask_64, mask), 80_000, 500),
        ("websocket_mask_4kib", lambda: _mask_payload(mask_4k, mask), 20_000, 200),
        ("websocket_mask_1mib", lambda: _mask_payload(mask_1m, mask), 40, 4),
    )

    rows: list[dict[str, Any]] = []
    for name, fn, operations, warmup in specs:
        ns = _ns(fn, operations, warmup=warmup)
        rows.append(
            {
                "name": name,
                "ns_per_op": ns,
                "ops_per_s": 1e9 / ns,
                "operations": operations,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rows = _measure()
    payload = {
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "rows": rows,
    }
    print(f"{'stage':<42} {'ns/op':>10} {'ops/s':>14}")
    print("-" * 68)
    for row in rows:
        print(f"{row['name']:<42} {row['ns_per_op']:10.0f} {row['ops_per_s']:14,.0f}")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
