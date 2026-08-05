"""A/B the retained-frame policy for QoS 1 and pre-PUBREC replay.

The current in-memory record keeps ``encoded_publish`` after first launch even
though retransmission re-encodes it with DUP=1. Below the transport segmentation
threshold that retained frame duplicates the payload. At and above the threshold
the write item is ``(header, payload)`` and the payload object is shared.

This benchmark compares:

* current: retain every first-send write item, then re-encode on retransmission;
* reencode: retain no write item and re-encode whenever replay is required;
* segmented: retain only segmented write items, patch DUP in their small header
  on first replay, and reuse the already-DUP item on later replays.

It reports retained traced allocation separately from first and later replay CPU.
Generated JSON is a workflow artifact and must not be committed.
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import statistics
import sys
import time
import tracemalloc
from dataclasses import asdict, dataclass
from pathlib import Path
from collections.abc import Callable

from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.transport.writes import SEGMENT_THRESHOLD, WriteItem
from mqttium.types import Properties

PAYLOAD_SIZES = (64, 4 * 1024, 64 * 1024, 256 * 1024, SEGMENT_THRESHOLD, 8 * 1024 * 1024)


@dataclass
class Sample:
    protocol: str
    profile: str
    payload_bytes: int
    segmented: bool
    retention_messages: int
    current_retained_bytes: int
    current_peak_bytes: int
    reencode_retained_bytes: int
    reencode_peak_bytes: int
    segmented_retained_bytes: int
    segmented_peak_bytes: int
    reencode_first_ns: float
    retained_patch_first_ns: float
    segmented_policy_first_ns: float
    segmented_policy_later_ns: float


def properties_for(protocol: MQTTProtocolVersion, profile: str) -> Properties | None:
    if protocol != MQTTProtocolVersion.MQTTv5:
        return None
    props = Properties()
    if profile == "property-heavy":
        props.set("message_expiry_interval", 3600)
        props.set("content_type", "application/octet-stream")
        props.set("response_topic", "bench/replies/frame-policy")
        props.set("correlation_data", b"correlation" * 8)
        for index in range(24):
            props.add_user_property(f"key-{index:02d}", "value" * 8)
    return props


def encode_item(
    payload: bytes,
    protocol: MQTTProtocolVersion,
    properties: Properties | None,
    *,
    dup: bool,
) -> WriteItem:
    return PublishPacket(
        topic="bench/qos1/frame-policy",
        payload=payload,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=dup,
        mid=17,
        properties=properties,
    ).encode_write_item(protocol)


def mark_dup(item: WriteItem) -> WriteItem:
    """Return the same write item when DUP is set, otherwise patch byte zero."""
    if isinstance(item, bytes):
        if item[0] & 0x08:
            return item
        return bytes((item[0] | 0x08,)) + item[1:]
    header, payload = item
    if header[0] & 0x08:
        return item
    return (bytes((header[0] | 0x08,)) + header[1:], payload)


def operations_for(payload_bytes: int) -> int:
    if payload_bytes <= 64:
        return 120_000
    if payload_bytes <= 4 * 1024:
        return 40_000
    if payload_bytes <= 64 * 1024:
        return 8_000
    if payload_bytes < SEGMENT_THRESHOLD:
        return 2_000
    return 20_000


def median_ns(fn: Callable[[], object], operations: int, repeat: int) -> float:
    warmup = min(2_000, max(50, operations // 20))
    for _ in range(warmup):
        fn()
    samples: list[float] = []
    for _ in range(repeat):
        started = time.perf_counter_ns()
        for _ in range(operations):
            fn()
        elapsed = time.perf_counter_ns() - started
        samples.append(elapsed / operations)
    return statistics.median(samples)


def retention_count(payload_bytes: int) -> int:
    return min(4_096, max(8, (32 * 1024 * 1024) // max(1, payload_bytes)))


def traced_retention(builder: Callable[[], list[WriteItem | None]]) -> tuple[int, int]:
    gc.collect()
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    retained = builder()
    current, peak = tracemalloc.get_traced_memory()
    # Keep the list alive until after both readings.
    if len(retained) < 0:  # pragma: no cover - makes ownership explicit
        raise AssertionError
    tracemalloc.stop()
    return max(0, current - before), max(0, peak - before)


def measure(
    payload_bytes: int,
    protocol: MQTTProtocolVersion,
    profile: str,
    *,
    repeat: int,
) -> Sample:
    count = retention_count(payload_bytes)
    payloads = [bytes(((index % 251) + 1,)) * payload_bytes for index in range(count)]
    properties = properties_for(protocol, profile)

    probe = encode_item(payloads[0], protocol, properties, dup=False)
    segmented = not isinstance(probe, bytes)
    if segmented:
        assert probe[1] is payloads[0]

    current_retained, current_peak = traced_retention(
        lambda: [encode_item(payload, protocol, properties, dup=False) for payload in payloads]
    )
    reencode_retained, reencode_peak = traced_retention(
        lambda: [
            None
            for payload in payloads
            if encode_item(payload, protocol, properties, dup=False) is not None
        ]
    )
    segmented_retained, segmented_peak = traced_retention(
        lambda: [
            item
            if not isinstance(item := encode_item(payload, protocol, properties, dup=False), bytes)
            else None
            for payload in payloads
        ]
    )

    payload = payloads[0]
    first_item = encode_item(payload, protocol, properties, dup=False)
    dup_item = mark_dup(first_item)
    operations = operations_for(payload_bytes)

    reencode_first = median_ns(
        lambda: encode_item(payload, protocol, properties, dup=True), operations, repeat
    )
    retained_patch_first = median_ns(lambda: mark_dup(first_item), operations, repeat)

    if segmented:
        segmented_first = retained_patch_first
        segmented_later = median_ns(lambda: mark_dup(dup_item), operations, repeat)
    else:
        # The selected policy intentionally keeps no contiguous frame.
        segmented_first = reencode_first
        segmented_later = reencode_first

    return Sample(
        protocol="v5" if protocol == MQTTProtocolVersion.MQTTv5 else "v311",
        profile=profile,
        payload_bytes=payload_bytes,
        segmented=segmented,
        retention_messages=count,
        current_retained_bytes=current_retained,
        current_peak_bytes=current_peak,
        reencode_retained_bytes=reencode_retained,
        reencode_peak_bytes=reencode_peak,
        segmented_retained_bytes=segmented_retained,
        segmented_peak_bytes=segmented_peak,
        reencode_first_ns=reencode_first,
        retained_patch_first_ns=retained_patch_first,
        segmented_policy_first_ns=segmented_first,
        segmented_policy_later_ns=segmented_later,
    )


def run(args: argparse.Namespace) -> dict[str, object]:
    samples: list[Sample] = []
    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        profiles = (
            ("plain",) if protocol == MQTTProtocolVersion.MQTTv311 else ("plain", "property-heavy")
        )
        for profile in profiles:
            for payload_bytes in PAYLOAD_SIZES:
                samples.append(measure(payload_bytes, protocol, profile, repeat=args.repeat))
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "segment_threshold": SEGMENT_THRESHOLD,
        "repeat": args.repeat,
        "samples": [asdict(sample) for sample in samples],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = run(args)
    print(
        f"{'proto':5s} {'profile':14s} {'payload':>9s} {'seg':>3s} "
        f"{'retain-now':>12s} {'retain-new':>12s} "
        f"{'reenc ns':>10s} {'first ns':>10s} {'later ns':>10s}"
    )
    for raw in payload["samples"]:  # type: ignore[index]
        print(
            f"{raw['protocol']:5s} {raw['profile']:14s} {raw['payload_bytes']:9d} "
            f"{str(raw['segmented']):>3s} {raw['current_retained_bytes']:12d} "
            f"{raw['segmented_retained_bytes']:12d} {raw['reencode_first_ns']:10.1f} "
            f"{raw['segmented_policy_first_ns']:10.1f} "
            f"{raw['segmented_policy_later_ns']:10.1f}"
        )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n")


if __name__ == "__main__":
    main()
