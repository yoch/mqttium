from __future__ import annotations

import sys
from pathlib import Path


TESTS = '''from __future__ import annotations

import pytest

from mqttium.codec.buffer import RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    PacketType,
    QoS,
)
from mqttium.errors import MalformedPacketError
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
from mqttium.protocol.inbound import _decode_v311_qos1_fields


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


def test_v311_qos0_and_qos1_both_avoid_generic_packet(monkeypatch) -> None:
    calls = 0
    original = PublishPacket.decode

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(PublishPacket, "decode", counted)
    engine = _connected()
    engine.handle_raw(
        RawPacket(PacketType.PUBLISH, 0x00, pack_utf8("bench/qos0") + b"zero")
    )
    engine.handle_raw(
        RawPacket(
            PacketType.PUBLISH,
            0x02,
            pack_utf8("bench/qos1") + pack_u16(7) + b"one",
        )
    )
    effects = engine.take_effects()
    assert [effect.kind for effect in effects] == [
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
        EffectKind.SEND,
    ]
    assert effects[0].data.qos is QoS.AT_MOST_ONCE
    assert effects[1].data.qos is QoS.AT_LEAST_ONCE
    assert effects[1].data.mid == 7
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

The MQTT 3.1.1 inbound dispatch now has two narrow direct paths: QoS 0 decodes
directly to `Message`, and QoS 1 decodes topic, packet identifier and payload
before invoking a shared `_on_qos1()` state machine. The generic MQTT 5/QoS 1
path invokes that same state machine after property-aware decoding.

Receive Maximum accounting, PUBACK generation, manual acknowledgement, duplicate
replay and persistence remain in one implementation. MQTT 5 and QoS 2 retain the
generic packet decoder.

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

## Composition validation

The rebased tree is validated on top of the already merged QoS 0 direct decoder.
A dedicated test sends adjacent MQTT 3.1.1 QoS 0 and QoS 1 publications and
asserts that neither enters `PublishPacket.decode()`. MQTT 5 QoS 1 and MQTT 3.1.1
QoS 2 explicitly remain on the generic path.

## Risks

The main risk is divergence between specialized parsing and the property-aware
generic path. Parsing is isolated in small helpers and QoS 1 state transitions
are factored into one shared method. Tests compare fields against
`PublishPacket.decode()`, cover packet identifier validation, automatic PUBACK,
manual-ack duplicate state, and prove MQTT 5/QoS 2 still use the generic decoder.
The full unit and fuzz suites run before publication.
'''


def patch_runtime(root: Path) -> None:
    path = root / "src/mqttium/protocol/inbound.py"
    text = path.read_text()

    text = text.replace(
        "from mqttium.codec.buffer import RawPacket\n"
        "from mqttium.codec.primitives import unpack_utf8\n",
        "from mqttium.codec.buffer import RawPacket\n"
        "from mqttium.codec.packet_validation import require_nonzero_mid\n"
        "from mqttium.codec.primitives import unpack_u16, unpack_utf8\n",
        1,
    )
    text = text.replace(
        "from mqttium.types import InboundMessage, Message\n",
        "from mqttium.types import InboundMessage, Message, Properties\n",
        1,
    )

    marker = "\n\nclass InboundSession:\n"
    helper = '''

def _decode_v311_qos1_fields(
    raw: RawPacket,
) -> tuple[str, bytes, int, bool, bool]:
    topic, pos = unpack_utf8(raw.remaining)
    validate_received_publish_topic(topic, utf8_validated=True)
    mid, pos = unpack_u16(raw.remaining, pos)
    require_nonzero_mid(mid, "PUBLISH")
    return topic, raw.remaining[pos:], mid, bool(raw.flags & 0x01), bool(raw.flags & 0x08)
'''
    if marker not in text:
        raise RuntimeError("InboundSession marker did not match")
    text = text.replace(marker, helper + marker, 1)

    old_dispatch = '''        if config.protocol is MQTTProtocolVersion.MQTTv311 and not (raw.flags & 0x06):
            engine._emit(EffectKind.MESSAGE, _decode_v311_qos0_message(raw))
            return
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
'''
    new_dispatch = '''        if config.protocol is MQTTProtocolVersion.MQTTv311:
            qos = (raw.flags >> 1) & 0x03
            if qos == int(QoS.AT_MOST_ONCE):
                engine._emit(EffectKind.MESSAGE, _decode_v311_qos0_message(raw))
                return
            if qos == int(QoS.AT_LEAST_ONCE):
                topic, payload, mid, retain, dup = _decode_v311_qos1_fields(raw)
                self._on_qos1(
                    topic=topic,
                    payload=payload,
                    mid=mid,
                    retain=retain,
                    dup=dup,
                    properties=None,
                )
                return
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
'''
    if old_dispatch not in text:
        raise RuntimeError("QoS 0 dispatch did not match")
    text = text.replace(old_dispatch, new_dispatch, 1)

    old_qos1 = '''        if packet.qos == QoS.AT_LEAST_ONCE and config.manual_ack:
            # A duplicate QoS1 publish reuses the existing Receive Maximum slot,
            # but is surfaced again so an application can complete manual ACK
            # after a reconnect or callback cancellation.
            existing = store.get_in(packet.mid)
            if existing is not None and existing.state is InboundQoSState.WAIT_PUBACK:
                self._emit_message(existing, dup=True)
                return

        self._acquire_slot()
        if packet.qos == QoS.AT_LEAST_ONCE:
            engine._emit(
                EffectKind.MESSAGE,
                Message(
                    topic=topic,
                    payload=packet.payload,
                    qos=packet.qos,
                    retain=packet.retain,
                    dup=packet.dup,
                    mid=packet.mid,
                    properties=packet.properties,
                ),
            )
            if config.manual_ack:
                store.put_in(
                    InboundMessage(
                        mid=packet.mid,
                        topic=topic,
                        payload=packet.payload,
                        qos=packet.qos,
                        retain=packet.retain,
                        state=InboundQoSState.WAIT_PUBACK,
                        delivered=False,
                        properties=packet.properties,
                    )
                )
            else:
                engine._send(PubAckPacket(mid=packet.mid).encode(config.protocol))
                self._release_slot()
            return

        inbound = InboundMessage(
'''
    new_qos1 = '''        if packet.qos == QoS.AT_LEAST_ONCE:
            self._on_qos1(
                topic=topic,
                payload=packet.payload,
                mid=packet.mid,
                retain=packet.retain,
                dup=packet.dup,
                properties=packet.properties,
            )
            return

        self._acquire_slot()
        inbound = InboundMessage(
'''
    if old_qos1 not in text:
        raise RuntimeError("generic QoS 1 block did not match")
    text = text.replace(old_qos1, new_qos1, 1)

    method_marker = "    def on_pubrel(self, raw: RawPacket) -> None:\n"
    method = '''    def _on_qos1(
        self,
        *,
        topic: str,
        payload: bytes,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
    ) -> None:
        config = self.config
        store = self.store
        if config.manual_ack:
            # A duplicate QoS1 publish reuses the existing Receive Maximum slot,
            # but is surfaced again so an application can complete manual ACK
            # after a reconnect or callback cancellation.
            existing = store.get_in(mid)
            if existing is not None and existing.state is InboundQoSState.WAIT_PUBACK:
                self._emit_message(existing, dup=True)
                return

        self._acquire_slot()
        self._engine._emit(
            EffectKind.MESSAGE,
            Message(
                topic=topic,
                payload=payload,
                qos=QoS.AT_LEAST_ONCE,
                retain=retain,
                dup=dup,
                mid=mid,
                properties=properties,
            ),
        )
        if config.manual_ack:
            store.put_in(
                InboundMessage(
                    mid=mid,
                    topic=topic,
                    payload=payload,
                    qos=QoS.AT_LEAST_ONCE,
                    retain=retain,
                    state=InboundQoSState.WAIT_PUBACK,
                    delivered=False,
                    properties=properties,
                )
            )
        else:
            self._engine._send(PubAckPacket(mid=mid).encode(config.protocol))
            self._release_slot()

'''
    if method_marker not in text:
        raise RuntimeError("on_pubrel marker did not match")
    path.write_text(text.replace(method_marker, method + method_marker, 1))


def build(root: Path) -> None:
    patch_runtime(root)
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
        raise SystemExit("usage: rebuild_qos1_after_qos0.py ROOT")
    build(Path(sys.argv[1]))
