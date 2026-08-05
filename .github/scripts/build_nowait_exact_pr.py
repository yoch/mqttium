from __future__ import annotations

import sys
from pathlib import Path

from apply_nowait_size_candidate import apply


TESTS = '''from __future__ import annotations

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.transport.writes import item_size
from mqttium.types import Properties


def _properties(protocol: MQTTProtocolVersion, rich: bool) -> Properties | None:
    if protocol is MQTTProtocolVersion.MQTTv311 or not rich:
        return None
    properties = Properties()
    properties.add_user_property("source", "nowait-wire-size")
    properties.set("content_type", "application/octet-stream")
    return properties


@pytest.mark.parametrize("protocol", list(MQTTProtocolVersion))
@pytest.mark.parametrize("qos", list(QoS))
@pytest.mark.parametrize(
    ("topic", "payload_size", "rich_properties"),
    [
        ("bench/nowait", 0, False),
        ("capteur/température", 64, False),
        ("bench/vbi-boundary", 127, True),
        ("bench/segmented", 1 << 20, True),
    ],
)
def test_publish_wire_size_matches_encoded_frame(
    protocol: MQTTProtocolVersion,
    qos: QoS,
    topic: str,
    payload_size: int,
    rich_properties: bool,
) -> None:
    properties = _properties(protocol, rich_properties)
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    actual = PublishPacket(
        topic=topic,
        payload=b"x" * payload_size,
        qos=qos,
        retain=True,
        dup=False,
        mid=None if qos is QoS.AT_MOST_ONCE else 17,
        properties=properties,
    ).encode_write_item(protocol)
    assert engine.outbound.publish_wire_size(
        topic,
        payload_size,
        qos,
        properties,
    ) == item_size(actual)


async def test_publish_nowait_encodes_only_the_real_frame(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.encode_write_item

    def counted(
        self: PublishPacket,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    ):
        nonlocal calls
        calls += 1
        return original(self, protocol)

    monkeypatch.setattr(PublishPacket, "encode_write_item", counted)
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = client.publish_nowait("bench/nowait", b"payload", qos=1)

    assert receipt.mid is not None
    assert calls == 1
    assert client._effect_flush_task is None
'''


BENCHMARK = '''from __future__ import annotations

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
    encode_median, encode_samples = _rate(
        encode_preview, count=args.count, repeat=args.repeat
    )
    size_median, size_samples = _rate(
        exact_size, count=args.count, repeat=args.repeat
    )
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
        args.output.write_text(rendered + "\\n")
    print(rendered)


if __name__ == "__main__":
    main()
'''


DOC = '''# Exact wire-size admission for `publish_nowait()`

## Problem

`AsyncClient.publish_nowait()` must reject a publication immediately when the
bounded writer can never accept its encoded MQTT frame. The previous admission
check built and encoded a complete preview `PublishPacket` with a placeholder
packet identifier, used only its byte length, then let `OutboundSession` allocate
the real identifier and encode the real frame again.

For QoS 1/2 this meant two complete encodes per publication. The preview could
not be reused safely because the real packet identifier had not yet been allocated.

## Change

`OutboundSession.publish_wire_size()` computes the exact wire size from the same
validated topic/property sizes used by normal outbound admission, including the
fixed header, Remaining Length VBI, packet identifier and MQTT 5 properties.
`publish_nowait()` and `publish_many_nowait()` use that integer for the writer
capacity check and encode only the real publication.

No queue bound, backpressure mode, receipt lifecycle, packet identifier rule or
wire representation changes.

## Evidence

A seven-cycle rotated native QoS 1 RTT comparison, 12,000 request/response pairs
per sample and 32 outstanding requests, measured:

- isolated callback + `publish_nowait()`: median **+6.12%** throughput; all seven
  cycles positive; p50 latency **-5.67%**, p95 **-4.42%**;
- experimental inline callback + `publish_nowait()`: median **+5.42%**; all seven
  cycles positive; p50 **-5.98%**, p95 **-5.58%**;
- `await publish()` control, whose path is unchanged: noisy and treated as a
  control rather than a claimed gain.

Profiling confirms one `PublishPacket.encode_write_item()` call per real
publication after the change, versus two before it. The retained microbenchmark
compares exact-size arithmetic with preview encoding and also asserts equality.

Experimental run: <https://github.com/yoch/mqttium/actions/runs/31056402920>

## Risks

The principal risk is size-calculation drift from the encoder. Parametrized tests
compare the calculated size with actual encoded frames across MQTT 3.1.1/MQTT 5,
QoS 0/1/2, Unicode topics, MQTT 5 properties, VBI boundaries and segmented payloads.
'''


def build(root: Path) -> None:
    apply(root)
    (root / "tests/unit/test_nowait_wire_size.py").write_text(TESTS)
    (root / "benchmarks/nowait_wire_size.py").write_text(BENCHMARK)
    (root / "docs/NOWAIT-WIRE-SIZE.md").write_text(DOC)

    changelog = root / "CHANGELOG.md"
    text = changelog.read_text()
    heading = "### Changed\n\n"
    index = text.index(heading) + len(heading)
    entry = (
        "- `publish_nowait()` and `publish_many_nowait()` now compute the exact MQTT "
        "wire size for bounded-writer admission instead of encoding a disposable "
        "preview frame. QoS 1/2 now encode only the real publication after packet-ID "
        "allocation.\n"
    )
    changelog.write_text(text[:index] + entry + text[index:])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_nowait_exact_pr.py ROOT")
    build(Path(sys.argv[1]))
