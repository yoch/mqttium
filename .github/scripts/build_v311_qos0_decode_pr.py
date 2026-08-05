from __future__ import annotations

import sys
from pathlib import Path


TESTS = '''from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import _decode_v311_qos0_message


def _connected(
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
) -> ProtocolEngine:
    engine = ProtocolEngine(EngineConfig(protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize(
    ("topic", "payload", "retain"),
    [
        ("bench/telemetry", b"payload", False),
        ("capteur/température", b"", True),
        ("a/b/c", b"x" * 4096, False),
    ],
)
def test_v311_qos0_direct_decode_matches_generic_packet(
    topic: str,
    payload: bytes,
    retain: bool,
) -> None:
    raw = RawPacket(
        packet_type=PacketType.PUBLISH,
        flags=0x01 if retain else 0x00,
        remaining=pack_utf8(topic) + payload,
    )
    message = _decode_v311_qos0_message(raw)
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv311)

    assert message.topic == packet.topic == topic
    assert message.payload == packet.payload == payload
    assert message.qos is packet.qos is QoS.AT_MOST_ONCE
    assert message.retain is packet.retain is retain
    assert message.dup is packet.dup is False
    assert message.mid is packet.mid is None
    assert message.properties is packet.properties is None


def test_v311_qos0_engine_emits_direct_message(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected()
    engine.handle_raw(
        RawPacket(PacketType.PUBLISH, 0x00, pack_utf8("bench/direct") + b"payload")
    )

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "bench/direct"
    assert effects[0].data.payload == b"payload"
    assert calls == 0


@pytest.mark.parametrize(
    ("flags", "remaining", "error_type", "match"),
    [
        (0x08, pack_utf8("bench/dup") + b"x", MalformedPacketError, "must not set DUP"),
        (0x00, pack_utf8("bench/+") + b"x", ProtocolError, "wildcards"),
        (0x00, pack_utf8("") + b"x", ProtocolError, "empty"),
    ],
)
def test_v311_qos0_direct_decode_preserves_validation(
    flags: int,
    remaining: bytes,
    error_type: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error_type, match=match):
        _decode_v311_qos0_message(RawPacket(PacketType.PUBLISH, flags, remaining))


def test_qos1_keeps_generic_acknowledged_path(monkeypatch) -> None:
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
    assert calls == 1


def test_mqtt5_qos0_keeps_property_aware_path(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected(MQTTProtocolVersion.MQTTv5)
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x00,
            pack_utf8("bench/v5") + b"\\x00" + b"payload",
        )
    )

    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [EffectKind.MESSAGE]
    assert effects[0].data.topic == "bench/v5"
    assert effects[0].data.payload == b"payload"
    assert calls == 1
'''


BENCHMARK = '''from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_utf8
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.inbound import _decode_v311_qos0_message
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
        args.output.write_text(rendered + "\\n")
    print(rendered)


if __name__ == "__main__":
    main()
'''


DOC = '''# MQTT 3.1.1 QoS 0 direct message decode

## Problem

For every inbound MQTT 3.1.1 QoS 0 PUBLISH, the generic path decoded and
validated a full `PublishPacket`, then immediately copied its fields into a
`Message` carried by an `EngineEffect`. Profiling showed that packet-model
construction and the subsequent packet-to-message conversion dominate the
subscriber hot path, ahead of callback dispatch.

MQTT 3.1.1 QoS 0 has no properties or packet identifier, so the intermediate
packet object carries no protocol state needed after validation.

## Change

`InboundSession` directly decodes the topic and payload into a `Message` for the
narrow MQTT 3.1.1/QoS 0 case. It preserves the established UTF-8 and publish-topic
validation, rejects QoS 0 with DUP, retains a zero-copy payload view and emits the
same ordered `MESSAGE` effect.

The generic `PublishPacket` path remains authoritative for:

- MQTT 5, including properties and topic aliases;
- QoS 1 and QoS 2 acknowledgement state;
- invalid/reserved QoS flag combinations;
- every non-PUBLISH packet.

## Evidence

Seven-cycle rotated same-runner A/B, 180,000 telemetry256 messages per sample:

| Delivery mode | Direct transport | Mosquitto with raw publishers |
| --- | ---: | ---: |
| Existing isolated callback worker | **+22.14%** | **+21.20%** |
| Experimental synchronous inline callback | **+25.26%** | **+23.41%** |

All seven cycles were positive in every cell. The gain composes with the inline
callback experiment, demonstrating that decode/model construction and callback
isolation are distinct costs.

Experimental run: <https://github.com/yoch/mqttium/actions/runs/31057216680>

## Risks

The main risk is validation drift between the specialized and generic decoders.
The specialized function is deliberately small and restricted to a protocol case
with no properties or packet identifier. Tests compare its result with
`PublishPacket.decode()`, cover Unicode, retain, empty and large payloads, DUP,
empty/wildcard topics, and verify that MQTT 5 and QoS 1 still invoke the generic
path. The full unit and Hypothesis fuzz suites run before publication.
'''


def build(root: Path) -> None:
    inbound_path = root / "src/mqttium/protocol/inbound.py"
    text = inbound_path.read_text()

    raw_import = "from mqttium.codec.buffer import RawPacket\n"
    if raw_import not in text:
        raise RuntimeError("RawPacket import did not match")
    text = text.replace(
        raw_import,
        raw_import + "from mqttium.codec.primitives import unpack_utf8\n",
        1,
    )

    error_import = "from mqttium.errors import ProtocolError\n"
    if error_import not in text:
        raise RuntimeError("ProtocolError import did not match")
    text = text.replace(
        error_import,
        "from mqttium.errors import MalformedPacketError, ProtocolError\n",
        1,
    )

    class_marker = "\n\nclass InboundSession:\n"
    if class_marker not in text:
        raise RuntimeError("InboundSession marker did not match")
    helper = '''

def _decode_v311_qos0_message(raw: RawPacket) -> Message:
    if raw.flags & 0x08:
        raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
    topic, payload_pos = unpack_utf8(raw.remaining)
    validate_received_publish_topic(topic, utf8_validated=True)
    return Message(
        topic=topic,
        payload=raw.remaining[payload_pos:],
        qos=QoS.AT_MOST_ONCE,
        retain=bool(raw.flags & 0x01),
        dup=False,
        mid=None,
        properties=None,
    )
'''
    text = text.replace(class_marker, helper + class_marker, 1)

    marker = '''        engine = self._engine
        config = self.config
        store = self.store
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
'''
    if marker not in text:
        raise RuntimeError("on_publish marker did not match")
    replacement = '''        engine = self._engine
        config = self.config
        store = self.store
        if config.protocol is MQTTProtocolVersion.MQTTv311 and not (raw.flags & 0x06):
            engine._emit(EffectKind.MESSAGE, _decode_v311_qos0_message(raw))
            return
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
'''
    inbound_path.write_text(text.replace(marker, replacement, 1))

    (root / "tests/unit/test_qos0_v311_decode_fastpath.py").write_text(TESTS)
    (root / "benchmarks/qos0_v311_decode.py").write_text(BENCHMARK)
    (root / "docs/QOS0-V311-DECODE.md").write_text(DOC)

    changelog = root / "CHANGELOG.md"
    text = changelog.read_text()
    heading = "### Changed\n\n"
    index = text.index(heading) + len(heading)
    entry = (
        "- Inbound MQTT 3.1.1 QoS 0 PUBLISH packets now decode directly into the "
        "delivered `Message`, avoiding a short-lived intermediate `PublishPacket`; "
        "MQTT 5 and acknowledged QoS paths keep the generic decoder.\n"
    )
    changelog.write_text(text[:index] + entry + text[index:])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_v311_qos0_decode_pr.py ROOT")
    build(Path(sys.argv[1]))
