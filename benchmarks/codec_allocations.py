"""How many times an inbound PUBLISH payload is copied, and what it costs.

The specialised in-buffer PUBLISH path was prototyped after the ordinary body
copy had been reduced. That prototype did not reduce the measured peak below
the two owned payload-sized buffers already present in the simple path, so the
specialisation is not shipped. This benchmark keeps the remaining allocation
claim reproducible.

It measures the whole ingress path a broker byte actually travels:
`IncrementalDecoder.feed()`, `next_packet()`, then `PublishPacket.decode()`.
Results are build artifacts and must not be committed.
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import statistics
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Properties

PAYLOAD_SIZES = (64, 4 * 1024, 64 * 1024, 1024 * 1024, 8 * 1024 * 1024)
FRAGMENTATIONS = ("contiguous", "random", "byte")


@dataclass
class Sample:
    protocol: str
    payload_bytes: int
    fragmentation: str
    allocated_bytes_per_message: float
    copies_of_payload: float
    peak_bytes: int
    mib_per_s: float


def build_frame(payload_bytes: int, protocol: MQTTProtocolVersion) -> bytes:
    properties: Properties | None = None
    if protocol == MQTTProtocolVersion.MQTTv5:
        properties = Properties()
        properties.set("message_expiry_interval", 60)
        properties.add_user_property("trace", "codec-allocations")
    packet = PublishPacket(
        topic="bench/codec/ingress",
        payload=b"z" * payload_bytes,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=1234,
        properties=properties,
    )
    return packet.encode(protocol)


def chunks(frame: bytes, fragmentation: str, rng: random.Random) -> list[bytes]:
    if fragmentation == "contiguous":
        return [frame]
    if fragmentation == "byte":
        return [frame[index : index + 1] for index in range(len(frame))]
    pieces: list[bytes] = []
    offset = 0
    while offset < len(frame):
        size = rng.randint(1, max(1, len(frame) // 8))
        pieces.append(frame[offset : offset + size])
        offset += size
    return pieces


def decode_once(
    decoder: IncrementalDecoder,
    pieces: list[bytes],
    protocol: MQTTProtocolVersion,
) -> int:
    for piece in pieces:
        decoder.feed(piece)
    raw = decoder.next_packet()
    if raw is None:
        raise RuntimeError("frame did not decode")
    packet = PublishPacket.decode(raw.flags, raw.remaining, protocol)
    return len(packet.payload)


def measure(
    payload_bytes: int,
    protocol: MQTTProtocolVersion,
    fragmentation: str,
    *,
    iterations: int,
) -> Sample:
    rng = random.Random(20260805)
    frame = build_frame(payload_bytes, protocol)
    pieces = chunks(frame, fragmentation, rng)
    decoder = IncrementalDecoder(max_packet_size=64 * 1024 * 1024)

    for _ in range(3):
        decode_once(decoder, pieces, protocol)

    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    tracemalloc.reset_peak()
    decode_once(decoder, pieces, protocol)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    allocated = max(0, peak - before)

    started = time.perf_counter()
    for _ in range(iterations):
        decode_once(decoder, pieces, protocol)
    elapsed = time.perf_counter() - started
    mib = (payload_bytes * iterations) / (1024 * 1024)

    return Sample(
        protocol="v5" if protocol == MQTTProtocolVersion.MQTTv5 else "v311",
        payload_bytes=payload_bytes,
        fragmentation=fragmentation,
        allocated_bytes_per_message=float(allocated),
        copies_of_payload=allocated / payload_bytes if payload_bytes else 0.0,
        peak_bytes=peak,
        mib_per_s=mib / elapsed if elapsed else 0.0,
    )


def iterations_for(payload_bytes: int, fragmentation: str) -> int:
    if fragmentation == "byte":
        return max(1, min(200, 4_000_000 // max(1, payload_bytes)))
    if payload_bytes >= 1024 * 1024:
        return 40
    if payload_bytes >= 64 * 1024:
        return 400
    return 4000


def run(args: argparse.Namespace) -> dict[str, object]:
    samples: list[Sample] = []
    protocols = (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5)
    for protocol in protocols:
        for payload_bytes in PAYLOAD_SIZES:
            for fragmentation in FRAGMENTATIONS:
                if fragmentation == "byte" and payload_bytes > args.max_byte_fragment:
                    continue
                repeats = [
                    measure(
                        payload_bytes,
                        protocol,
                        fragmentation,
                        iterations=iterations_for(payload_bytes, fragmentation),
                    )
                    for _ in range(args.repeat)
                ]
                best = min(repeats, key=lambda sample: sample.allocated_bytes_per_message)
                best.mib_per_s = statistics.median(sample.mib_per_s for sample in repeats)
                samples.append(best)
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "repeat": args.repeat,
        "samples": [asdict(sample) for sample in samples],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--max-byte-fragment", type=int, default=64 * 1024)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = run(args)
    print(
        f"{'proto':6s} {'payload':>9s} {'fragment':11s} "
        f"{'alloc/msg':>11s} {'xpayload':>9s} {'MiB/s':>9s}"
    )
    for sample in payload["samples"]:  # type: ignore[index]
        print(
            f"{sample['protocol']:6s} {sample['payload_bytes']:9d} "
            f"{sample['fragmentation']:11s} "
            f"{sample['allocated_bytes_per_message']:11.0f} "
            f"{sample['copies_of_payload']:9.2f} {sample['mib_per_s']:9.1f}"
        )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n")


if __name__ == "__main__":
    main()
