from __future__ import annotations

import sys
from pathlib import Path


def apply(root: Path) -> None:
    path = root / "src/mqttium/protocol/inbound.py"
    text = path.read_text()

    error_import = "from mqttium.errors import ProtocolError\n"
    if error_import not in text:
        raise RuntimeError("ProtocolError import did not match")
    text = text.replace(
        error_import,
        "from mqttium.errors import MalformedPacketError, ProtocolError\n",
        1,
    )

    qos1_marker = '''        if (
            config.protocol is MQTTProtocolVersion.MQTTv311
            and ((raw.flags >> 1) & 0x03) == int(QoS.AT_LEAST_ONCE)
        ):
'''
    if qos1_marker not in text:
        raise RuntimeError("QoS 1 direct branch did not match")
    qos0_branch = '''        if config.protocol is MQTTProtocolVersion.MQTTv311 and not (raw.flags & 0x06):
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
'''
    path.write_text(text.replace(qos1_marker, qos0_branch + qos1_marker, 1))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_qos0_after_qos1_candidate.py ROOT")
    apply(Path(sys.argv[1]))
