from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.transport.writes import item_size
from mqttium.types import Properties


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
    parser.add_argument("--count", type=int, default=200_000)
    parser.add_argument("--repeat", type=int, default=9)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    protocol = MQTTProtocolVersion.MQTTv5
    qos = QoS.AT_LEAST_ONCE
    topic = "bench/nowait/request"
    payload = b"r" * 64
    properties = Properties()
    properties.add_user_property("profile", "standard")
    engine = ProtocolEngine(EngineConfig(protocol=protocol))

    def encode_preview() -> int:
        return item_size(
            PublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=False,
                dup=False,
                mid=1,
                properties=properties,
            ).encode_write_item(protocol)
        )

    def exact_size() -> int:
        return engine.outbound.publish_wire_size(
            topic,
            len(payload),
            qos,
            properties,
        )

    expected = encode_preview()
    assert exact_size() == expected
    encode_median, encode_samples = _rate(encode_preview, count=args.count, repeat=args.repeat)
    size_median, size_samples = _rate(exact_size, count=args.count, repeat=args.repeat)
    result = {
        "encoded_size": expected,
        "preview_encode_ops_s_median": encode_median,
        "exact_size_ops_s_median": size_median,
        "exact_size_speedup": size_median / encode_median,
        "preview_encode_samples": encode_samples,
        "exact_size_samples": size_samples,
    }
    rendered = json.dumps(result, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
