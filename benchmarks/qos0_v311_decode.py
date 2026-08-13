from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_utf8
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import PublishPacket
from mqttium.packets._publish import decode_qos0_message_v311 as _decode_v311_qos0_message
from mqttium.types import Message


def _rate(operation, *, count: int, repeat: int) -> tuple[float, list[float]]:
    samples: list[float] = []
    for _ in range(repeat):
        started = time.perf_counter()
        for _ in range(count):
            operation()
        samples.append(count / (time.perf_counter() - started))
    return statistics.median(samples), samples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=300_000)
    parser.add_argument("--repeat", type=int, default=9)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    raw = RawPacket(
        PacketType.PUBLISH,
        0x00,
        pack_utf8("bench/exact/telemetry") + b"t" * 256,
    )

    def generic_packet_then_message() -> Message:
        packet = PublishPacket.decode(
            raw.flags,
            raw.remaining,
            MQTTProtocolVersion.MQTTv311,
        )
        return Message(
            topic=packet.topic,
            payload=packet.payload,
            qos=packet.qos,
            retain=packet.retain,
            dup=packet.dup,
            mid=packet.mid,
            properties=packet.properties,
        )

    def direct_message() -> Message:
        return _decode_v311_qos0_message(raw)

    generic = generic_packet_then_message()
    direct = direct_message()
    assert direct == generic
    generic_median, generic_samples = _rate(
        generic_packet_then_message,
        count=args.count,
        repeat=args.repeat,
    )
    direct_median, direct_samples = _rate(
        direct_message,
        count=args.count,
        repeat=args.repeat,
    )
    result = {
        "generic_packet_then_message_ops_s_median": generic_median,
        "direct_message_ops_s_median": direct_median,
        "direct_speedup": direct_median / generic_median,
        "generic_samples": generic_samples,
        "direct_samples": direct_samples,
    }
    rendered = json.dumps(result, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
