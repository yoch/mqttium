"""Micro A/B for specialized acknowledgement decode primitives."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

# Allow running as a script without install: repo-root PYTHONPATH=src.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mqttium.packets import PubAckPacket
from mqttium.packets._ack_v311 import decode_puback_v311
from mqttium.packets._ack_v5 import decode_puback_v5
from mqttium.enums import MQTTProtocolVersion


def _rate(operation, *, count: int, repeat: int) -> tuple[float, list[float]]:
    samples: list[float] = []
    for _ in range(repeat):
        started = time.perf_counter()
        for _ in range(count):
            operation()
        samples.append(count / (time.perf_counter() - started))
    return statistics.median(samples), samples


def _alloc_peak(operation, *, count: int) -> int:
    tracemalloc.start()
    for _ in range(count):
        operation()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=300_000)
    parser.add_argument("--repeat", type=int, default=9)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    bodies = {
        "v311_2byte": b"\x00\x09",
        "v5_2byte": b"\x00\x09",
        "v5_3byte_0x10": b"\x00\x09\x10",
        "v5_3byte_0x00": b"\x00\x09\x00",
        "v5_4byte_empty_props": b"\x00\x09\x00\x00",
    }

    results: dict[str, object] = {"count": args.count, "repeat": args.repeat, "cases": {}}
    for name, body in bodies.items():
        if name.startswith("v311"):

            def primitive(b=body):
                return decode_puback_v311(b)

            def packet(b=body):
                return PubAckPacket.decode(b, MQTTProtocolVersion.MQTTv311)
        else:

            def primitive(b=body):
                return decode_puback_v5(b)

            def packet(b=body):
                return PubAckPacket.decode(b, MQTTProtocolVersion.MQTTv5)

        # Packet.decode is now a factory over the primitive, so measure the
        # factory overhead separately from a hand-rolled generic baseline by
        # comparing primitive vs constructing the dataclass on top.
        prim_rate, _ = _rate(primitive, count=args.count, repeat=args.repeat)
        pkt_rate, _ = _rate(packet, count=args.count, repeat=args.repeat)
        results["cases"][name] = {
            "primitive_ops_per_s": prim_rate,
            "packet_factory_ops_per_s": pkt_rate,
            "factory_over_primitive": pkt_rate / prim_rate,
            "primitive_peak_bytes": _alloc_peak(primitive, count=min(args.count, 50_000)),
            "packet_peak_bytes": _alloc_peak(packet, count=min(args.count, 50_000)),
            "sample": primitive(),
        }

    text = json.dumps(results, indent=2, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
