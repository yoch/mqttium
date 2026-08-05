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

    marker = """        engine = self._engine
        config = self.config
        store = self.store
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
"""
    if marker not in text:
        raise RuntimeError("on_publish marker did not match")
    replacement = """        engine = self._engine
        config = self.config
        store = self.store
        if config.protocol == MQTTProtocolVersion.MQTTv311 and not (raw.flags & 0x06):
            if raw.flags & 0x08:
                raise MalformedPacketError("QoS 0 PUBLISH must not set DUP")
            topic, payload_pos = unpack_utf8(raw.remaining)
            validate_received_publish_topic(topic, utf8_validated=True)
            engine._emit(
                EffectKind.MESSAGE,
                Message(
                    topic=topic,
                    payload=raw.remaining[payload_pos:],
                    qos=QoS.AT_MOST_ONCE,
                    retain=bool(raw.flags & 0x01),
                    dup=False,
                    mid=None,
                    properties=None,
                ),
            )
            return
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
"""
    path.write_text(text.replace(marker, replacement, 1))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_qos0_v311_decode_candidate.py ROOT")
    apply(Path(sys.argv[1]))
