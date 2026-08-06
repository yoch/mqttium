from __future__ import annotations

import sys
from pathlib import Path


def apply(root: Path) -> None:
    path = root / "src/mqttium/protocol/inbound.py"
    text = path.read_text()

    raw_import = "from mqttium.codec.buffer import RawPacket\n"
    if raw_import not in text:
        raise RuntimeError("RawPacket import did not match")
    text = text.replace(
        raw_import,
        raw_import
        + "from mqttium.codec.packet_validation import require_nonzero_mid\n"
        + "from mqttium.codec.primitives import unpack_u16, unpack_utf8\n",
        1,
    )
    types_import = "from mqttium.types import InboundMessage, Message\n"
    if types_import not in text:
        raise RuntimeError("types import did not match")
    text = text.replace(
        types_import,
        "from mqttium.types import InboundMessage, Message, Properties\n",
        1,
    )

    class_marker = "\n\nclass InboundSession:\n"
    if class_marker not in text:
        raise RuntimeError("InboundSession marker did not match")
    helper = '''

def _decode_v311_qos1_fields(
    raw: RawPacket,
) -> tuple[str, bytes | bytearray | memoryview, int, bool, bool]:
    topic, pos = unpack_utf8(raw.remaining)
    validate_received_publish_topic(topic, utf8_validated=True)
    mid, pos = unpack_u16(raw.remaining, pos)
    require_nonzero_mid(mid, "PUBLISH")
    return topic, raw.remaining[pos:], mid, bool(raw.flags & 0x01), bool(raw.flags & 0x08)
'''
    text = text.replace(class_marker, helper + class_marker, 1)

    start = '''        engine = self._engine
        config = self.config
        store = self.store
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
'''
    replacement = '''        engine = self._engine
        config = self.config
        store = self.store
        if (
            config.protocol is MQTTProtocolVersion.MQTTv311
            and ((raw.flags >> 1) & 0x03) == int(QoS.AT_LEAST_ONCE)
        ):
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
    if start not in text:
        raise RuntimeError("on_publish start did not match")
    text = text.replace(start, replacement, 1)

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

    marker = "    def on_pubrel(self, raw: RawPacket) -> None:\n"
    if marker not in text:
        raise RuntimeError("on_pubrel marker did not match")
    method = '''    def _on_qos1(
        self,
        *,
        topic: str,
        payload: bytes | bytearray | memoryview,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
    ) -> None:
        config = self.config
        store = self.store
        if config.manual_ack:
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
    path.write_text(text.replace(marker, method + marker, 1))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_v311_qos1_decode_candidate_v3.py ROOT")
    apply(Path(sys.argv[1]))
