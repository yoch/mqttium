from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}")
    p.write_text(text.replace(old, new, 1))


def replace_between(path: str, start: str, end: str, body: str) -> None:
    p = Path(path)
    text = p.read_text()
    a = text.index(start)
    b = text.index(end, a)
    p.write_text(text[:a] + body + text[b:])


inbound = "src/mqttium/protocol/inbound.py"
outbound = "src/mqttium/protocol/outbound.py"

replace_once(
    inbound,
    ")\nfrom mqttium.protocol.effects import EffectKind\n",
    ")\nfrom mqttium.packets.acks import encode_success_ack\nfrom mqttium.protocol.effects import EffectKind\n",
)
replace_once(
    inbound,
    '    return bytes((0x40, 0x02, mid >> 8, mid & 0xFF))\n',
    '    return encode_success_ack(PacketType.PUBACK, mid)\n',
)
p = Path(inbound)
p.write_text(
    p.read_text().replace(
        "engine._send(PubRecPacket(mid=mid).encode(config.protocol))",
        "engine._send(encode_success_ack(PacketType.PUBREC, mid))",
    )
)

on_pubrel = '''    def on_pubrel(self, raw: RawPacket) -> None:  # noqa: C901
        engine = self._engine
        config = self.config
        store = self.store
        remaining = raw.remaining
        if len(remaining) == 2:
            mid = (remaining[0] << 8) | remaining[1]
            require_nonzero_mid(mid, "PUBREL")
        else:
            rel = PubRelPacket.decode(remaining, config.protocol)
            engine._validate_inbound_problem_information(PacketType.PUBREL, rel.properties)
            mid = rel.mid
        transitions = self._transitions
        if transitions is not None:
            meta = transitions.in_meta(mid)
            if meta is None:
                engine._send(encode_success_ack(PacketType.PUBCOMP, mid))
                return
            if meta.state is InboundQoSState.WAIT_USER_ACK:
                if config.manual_ack:
                    return
                completed = transitions.complete_in(mid, InboundQoSState.WAIT_USER_ACK)
                if completed is None:
                    raise ProtocolError(f"Inbound mid={mid} changed while completing PUBREL")
                engine._send(encode_success_ack(PacketType.PUBCOMP, mid))
                self._release_slot(completed.logical_size)
                return
            if meta.state is not InboundQoSState.WAIT_PUBREL:
                raise ProtocolError(
                    f"PUBREL for inbound mid={mid} in invalid state {meta.state!r}"
                )
            if config.manual_ack and not meta.user_acked:
                changed = transitions.transition_in(
                    mid,
                    InboundQoSState.WAIT_PUBREL,
                    InboundQoSState.WAIT_USER_ACK,
                )
                if changed is None:
                    raise ProtocolError(f"Inbound mid={mid} changed while processing PUBREL")
                return
            completed = transitions.complete_in(mid, InboundQoSState.WAIT_PUBREL)
            if completed is None:
                raise ProtocolError(f"Inbound mid={mid} changed while completing PUBREL")
            engine._send(encode_success_ack(PacketType.PUBCOMP, mid))
            self._release_slot(completed.logical_size)
            return

        inbound = store.get_in(mid)
        if inbound is None:
            engine._send(encode_success_ack(PacketType.PUBCOMP, mid))
            return
        if inbound.state is InboundQoSState.WAIT_USER_ACK:
            if config.manual_ack:
                return
            popped = store.pop_in(mid)
            if popped is None:
                raise ProtocolError(f"Inbound mid={mid} disappeared while completing PUBREL")
            engine._send(encode_success_ack(PacketType.PUBCOMP, mid))
            self._release_slot(self.stored_logical_size(popped))
            return
        if inbound.state is not InboundQoSState.WAIT_PUBREL:
            raise ProtocolError(
                f"PUBREL for inbound mid={mid} in invalid state {inbound.state!r}"
            )
        if config.manual_ack and not inbound.user_acked:
            inbound.state = InboundQoSState.WAIT_USER_ACK
            store.update_in(inbound)
            return
        popped = store.pop_in(mid)
        if popped is None:
            raise ProtocolError(f"Inbound mid={mid} disappeared while completing PUBREL")
        engine._send(encode_success_ack(PacketType.PUBCOMP, mid))
        self._release_slot(self.stored_logical_size(popped))

'''
replace_between(
    inbound,
    "    def on_pubrel(self, raw: RawPacket) -> None:  # noqa: C901\n",
    "    # --- application acknowledgement and replay ---------------------------\n",
    on_pubrel,
)
replace_once(
    inbound,
    '''        packet = (
            PubAckPacket(mid=mid)
            if state is InboundQoSState.WAIT_PUBACK
            else PubCompPacket(mid=mid)
        )
        wire = packet.encode(config.protocol)
''',
    '''        wire = encode_success_ack(
            PacketType.PUBACK if state is InboundQoSState.WAIT_PUBACK else PacketType.PUBCOMP,
            mid,
        )
''',
)

replace_once(
    outbound,
    ")\nfrom mqttium.packets.publish import encode_publish_item\n",
    ")\nfrom mqttium.packets.acks import encode_success_ack\nfrom mqttium.packets.publish import encode_publish_item\n",
)

on_pubrec = '''    def on_pubrec(self, raw: RawPacket) -> None:
        remaining = raw.remaining
        if len(remaining) == 2:
            mid = (remaining[0] << 8) | remaining[1]
            require_nonzero_mid(mid, "PUBREC")
            reason_code = 0
        else:
            rec = PubRecPacket.decode(remaining, self.config.protocol)
            self._engine._validate_inbound_problem_information(
                PacketType.PUBREC, rec.properties
            )
            mid = rec.mid
            reason_code = rec.reason_code
        transitions = self._transitions
        if transitions is not None:
            if reason_code >= 128:
                self._fail_after_pubrec(mid, reason_code)
                return
            changed = transitions.transition_out(
                mid,
                OutboundQoSState.WAIT_PUBREC,
                OutboundQoSState.WAIT_PUBCOMP,
                compact=True,
            )
            if changed is not None:
                self._send(encode_success_ack(PacketType.PUBREL, mid, flags=0x02))
                return
            if transitions.out_meta(mid) is None:
                self._send_orphan_pubrel(mid)
            return

        msg = self.store.get_out(mid)
        if msg is None:
            self._send_orphan_pubrel(mid)
            return
        if msg.state is not OutboundQoSState.WAIT_PUBREC:
            return
        if reason_code >= 128:
            self._fail_after_pubrec(mid, reason_code)
            return
        msg.state = OutboundQoSState.WAIT_PUBCOMP
        msg.topic = ""
        msg.payload = b""
        msg.properties = None
        msg.encoded_publish = None
        if msg.encoded_pubrel is None:
            msg.encoded_pubrel = encode_success_ack(PacketType.PUBREL, mid, flags=0x02)
        self.store.update_out(msg)
        self._send(msg.encoded_pubrel)

'''
replace_between(
    outbound,
    "    def on_pubrec(self, raw: RawPacket) -> None:\n",
    "    def _send_orphan_pubrel(self, mid: int) -> None:\n",
    on_pubrec,
)

on_pubcomp = '''    def on_pubcomp(self, raw: RawPacket) -> None:
        remaining = raw.remaining
        if len(remaining) == 2:
            mid = (remaining[0] << 8) | remaining[1]
            require_nonzero_mid(mid, "PUBCOMP")
            reason_code = 0
        else:
            comp = PubCompPacket.decode(remaining, self.config.protocol)
            self._engine._validate_inbound_problem_information(
                PacketType.PUBCOMP, comp.properties
            )
            mid = comp.mid
            reason_code = comp.reason_code
        if not self._settle(mid, OutboundQoSState.WAIT_PUBCOMP):
            return
        self.flow.release()
        if reason_code >= 128:
            self._fail(mid, ProtocolError(f"PUBCOMP reason_code={reason_code}"))
        else:
            self._emit(EffectKind.PUBLISH_COMPLETE, mid)
        self.packet_ids.release(mid)
        self.drain()

'''
replace_between(
    outbound,
    "    def on_pubcomp(self, raw: RawPacket) -> None:\n",
    "    # --- launching and retransmission ---------------------------------------\n",
    on_pubcomp,
)

Path("tests/unit/test_ack_fastpath_rpi.py").write_text('''from __future__ import annotations

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.primitives import pack_u16, pack_utf8
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
from mqttium.packets import PubCompPacket, PubRecPacket, PubRelPacket
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties


def _raw(wire: bytes) -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    packet = decoder.next_packet()
    assert packet is not None
    return packet


def _rpi0(engine: ProtocolEngine) -> None:
    engine._sent_request_problem_information = 0


def _assert_rejected(engine: ProtocolEngine) -> None:
    effects = engine.take_effects()
    assert engine.state is ConnectionState.DISCONNECTED
    assert any(effect.kind is EffectKind.PROTOCOL_ERROR for effect in effects)


def test_pubrec_long_form_still_enforces_request_problem_information() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/rpi", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()
    _rpi0(engine)
    engine.handle_raw(_raw(PubRecPacket(mid=handle.mid, properties=Properties(values={"reason_string": "forbidden"})).encode(MQTTProtocolVersion.MQTTv5)))
    _assert_rejected(engine)


def test_pubcomp_long_form_still_enforces_request_problem_information() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    handle = engine.queue_publish("ack/rpi", b"payload", qos=2)
    assert handle.mid is not None
    engine.take_effects()
    engine.handle_raw(RawPacket(PacketType.PUBREC, 0, pack_u16(handle.mid)))
    engine.take_effects()
    _rpi0(engine)
    engine.handle_raw(_raw(PubCompPacket(mid=handle.mid, properties=Properties(values={"reason_string": "forbidden"})).encode(MQTTProtocolVersion.MQTTv5)))
    _assert_rejected(engine)


def test_pubrel_long_form_still_enforces_request_problem_information() -> None:
    engine = ProtocolEngine(EngineConfig(protocol=MQTTProtocolVersion.MQTTv5))
    engine.state = ConnectionState.CONNECTED
    engine.handle_raw(RawPacket(PacketType.PUBLISH, 0x04, pack_utf8("ack/rpi") + pack_u16(9) + b"\\x00payload"))
    engine.take_effects()
    _rpi0(engine)
    engine.handle_raw(_raw(PubRelPacket(mid=9, properties=Properties(values={"reason_string": "forbidden"})).encode(MQTTProtocolVersion.MQTTv5)))
    _assert_rejected(engine)
''')

p = Path("CHANGELOG.md")
text = p.read_text()
marker = "## [Unreleased]\n"
note = "\n- Fast-path common success/no-properties PUBREC, PUBREL and PUBCOMP frames without transient packet objects while retaining full MQTT 5 property/RPI validation for longer acknowledgement forms.\n"
if note.strip() not in text:
    p.write_text(text.replace(marker, marker + note, 1))

p = Path("docs/reports/README.md")
text = p.read_text()
row = "| 2026-08-12 | [`ACK-SUCCESS-FASTPATH.md`](ACK-SUCCESS-FASTPATH.md) | Can common success/no-properties acknowledgement frames bypass transient packet objects without weakening long-form MQTT 5 validation? |\n"
anchor = "| 2026-08-12 | [`QOS2-V311-DECODE.md`](QOS2-V311-DECODE.md) | Should inbound MQTT 3.1.1 QoS 2 PUBLISH use the same field decoder as QoS 1? |\n"
if row not in text:
    p.write_text(text.replace(anchor, anchor + row, 1))
