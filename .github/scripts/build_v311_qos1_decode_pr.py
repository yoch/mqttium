from __future__ import annotations

import sys
from pathlib import Path

from apply_v311_qos1_decode_candidate_v3 import apply


TESTS = '''from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import _decode_v311_qos1_fields
from mqttium.protocol.state import InboundQoSState


def _connected(*, manual_ack: bool = False) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            protocol=MQTTProtocolVersion.MQTTv311,
            manual_ack=manual_ack,
        )
    )
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize(
    ("topic", "payload", "retain", "dup", "mid"),
    [
        ("bench/request", b"payload", False, False, 1),
        ("capteur/température", b"", True, True, 65535),
        ("a/b/c", b"x" * 4096, False, True, 42),
    ],
)
def test_v311_qos1_fields_match_generic_packet(
    topic: str,
    payload: bytes,
    retain: bool,
    dup: bool,
    mid: int,
) -> None:
    flags = 0x02 | int(retain) | (0x08 if dup else 0)
    raw = RawPacket(
        PacketType.PUBLISH,
        flags,
        pack_utf8(topic) + pack_u16(mid) + payload,
    )
    fields = _decode_v311_qos1_fields(raw)
    packet = PublishPacket.decode(flags, raw.remaining, MQTTProtocolVersion.MQTTv311)
    assert fields == (
        packet.topic,
        packet.payload,
        packet.mid,
        packet.retain,
        packet.dup,
    )


def test_v311_qos1_zero_mid_preserves_error() -> None:
    raw = RawPacket(
        PacketType.PUBLISH,
        0x02,
        pack_utf8("bench/zero") + b"\\x00\\x00payload",
    )
    with pytest.raises(MalformedPacketError, match="PUBLISH packet identifier"):
        _decode_v311_qos1_fields(raw)


def test_v311_qos1_auto_ack_avoids_generic_packet(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected()
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/qos1") + pack_u16(7) + b"payload",
        )
    )
    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE, EffectKind.SEND]
    assert effects[0].data.mid == 7
    assert effects[0].data.qos is QoS.AT_LEAST_ONCE
    assert calls == 0


def test_v311_qos1_manual_ack_preserves_duplicate_state() -> None:
    engine = _connected(manual_ack=True)
    first = RawPacket(
        PacketType.PUBLISH,
        0x02,
        pack_utf8("bench/manual") + pack_u16(9) + b"payload",
    )
    engine.handle_raw(first)
    first_effects = engine.take_effects()
    assert [effect.kind for effect in first_effects] == [EffectKind.MESSAGE]
    stored = engine.store.get_in(9)
    assert stored is not None
    assert stored.state is InboundQoSState.WAIT_PUBACK

    duplicate = RawPacket(
        PacketType.PUBLISH,
        0x0A,
        pack_utf8("bench/manual") + pack_u16(9) + b"payload",
    )
    engine.handle_raw(duplicate)
    duplicate_effects = engine.take_effects()
    assert [effect.kind for effect in duplicate_effects] == [EffectKind.MESSAGE]
    assert duplicate_effects[0].data.mid == 9
    assert duplicate_effects[0].data.dup is True


@pytest.mark.parametrize(
    ("protocol", "flags", "remaining"),
    [
        (
            MQTTProtocolVersion.MQTTv5,
            0x02,
            pack_utf8("bench/v5") + pack_u16(3) + b"\\x00payload",
        ),
        (
            MQTTProtocolVersion.MQTTv311,
            0x04,
            pack_utf8("bench/qos2") + pack_u16(3) + b"payload",
        ),
    ],
)
def test_mqtt5_and_qos2_remain_generic(
    monkeypatch,
    protocol: MQTTProtocolVersion,
    flags: int,
    remaining: bytes,
) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(RawPacket(PacketType.PUBLISH, flags, remaining))
    assert calls == 1
'''


BENCHMARK = '''from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.inbound import _decode_v311_qos1_fields
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
        0x02,
        pack_utf8("bench/rtt/request") + pack_u16(17) + b"r" * 64,
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

    def direct_fields_then_message() -> Message:
        topic, payload, mid, retain, dup = _decode_v311_qos1_fields(raw)
        return Message(
            topic=topic,
            payload=payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=retain,
            dup=dup,
            mid=mid,
            properties=None,
        )

    assert direct_fields_then_message() == generic_packet_then_message()
    generic_median, generic_samples = _rate(
        generic_packet_then_message,
        count=args.count,
        repeat=args.repeat,
    )
    direct_median, direct_samples = _rate(
        direct_fields_then_message,
        count=args.count,
        repeat=args.repeat,
    )
    result = {
        "generic_packet_then_message_ops_s_median": generic_median,
        "direct_fields_then_message_ops_s_median": direct_median,
        "direct_speedup": direct_median / generic_median,
        "generic_samples": generic_samples,
        "direct_samples": direct_samples,
    }
    rendered = json.dumps(result, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\\n")
    print(rendered)


if __name__ == "__main__":
    main()
'''


DOC = '''# MQTT 3.1.1 QoS 1 direct field decode

## Problem

Every inbound MQTT 3.1.1 QoS 1 PUBLISH was decoded into a generic
`PublishPacket`, then copied into the delivered `Message` before the shared
PUBACK/manual-ack state machine ran. Native RTT profiling identified this
packet-model round trip as a repeated CPU cost on both legs of each application
request/response pair.

## Change

For MQTT 3.1.1 QoS 1, `InboundSession` decodes the topic, packet identifier and
payload directly, then invokes a shared `_on_qos1()` state machine. The generic
MQTT 5/QoS 1 path invokes that same state machine after property-aware decoding.

This keeps Receive Maximum accounting, PUBACK generation, manual acknowledgement,
duplicate replay and persistence in one implementation. MQTT 5 and QoS 2 retain
the generic packet decoder.

## Evidence

A seven-cycle rotated capacity A/B, with the exact-size `publish_nowait()` fix
applied to both base and candidate, measured:

- `await publish()`: **+4.78%** median throughput, p50 **-4.82%**;
- isolated callback + `publish_nowait()`: **+4.18%**, p50 **-3.16%**;
- experimental inline callback + `publish_nowait()`: **+4.00%**, p50 **-3.84%**,
  p95 **-4.79%**.

Capacity run: <https://github.com/yoch/mqttium/actions/runs/31058609259>

An additional open-loop test completed every request at both offered loads. Near
90% of base capacity, all seven cycles favored the candidate: p50 improved
**8.37%** and p95 **18.11%**. Near 50%, completion remained 100% and latency was
scheduling-noise dominated, with no consistent benefit claimed.

Open-loop run: <https://github.com/yoch/mqttium/actions/runs/31058890448>

## Risks

The main risk is divergence between specialized parsing and the property-aware
generic path. Parsing is isolated in a small helper and QoS 1 state transitions
are factored into one shared method. Tests compare fields against
`PublishPacket.decode()`, cover packet identifier validation, automatic PUBACK,
manual-ack duplicate state, and prove MQTT 5/QoS 2 still use the generic decoder.
The full unit and fuzz suites run before publication.
'''


def build(root: Path) -> None:
    apply(root)
    (root / "tests/unit/test_qos1_v311_decode_fastpath.py").write_text(TESTS)
    (root / "benchmarks/qos1_v311_decode.py").write_text(BENCHMARK)
    (root / "docs/QOS1-V311-DECODE.md").write_text(DOC)

    changelog = root / "CHANGELOG.md"
    text = changelog.read_text()
    heading = "### Changed\n\n"
    index = text.index(heading) + len(heading)
    entry = (
        "- Inbound MQTT 3.1.1 QoS 1 PUBLISH packets now decode their delivery "
        "fields directly before entering the shared acknowledgement state machine, "
        "avoiding a short-lived intermediate `PublishPacket`; MQTT 5 and QoS 2 "
        "retain the generic decoder.\n"
    )
    changelog.write_text(text[:index] + entry + text[index:])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_v311_qos1_decode_pr.py ROOT")
    build(Path(sys.argv[1]))
