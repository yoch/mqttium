from __future__ import annotations

import sys
from pathlib import Path

from build_v311_qos1_decode_pr import build


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_v311_qos1_decode_pr_v2.py ROOT")
    root = Path(sys.argv[1])
    build(root)
    path = root / "tests/unit/test_qos1_v311_decode_fastpath.py"
    text = path.read_text()
    text = text.replace(
        "from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS\n",
        "from mqttium.enums import (\n"
        "    ConnectionState,\n"
        "    InboundQoSState,\n"
        "    MQTTProtocolVersion,\n"
        "    PacketType,\n"
        "    QoS,\n"
        ")\n",
        1,
    )
    text = text.replace("from mqttium.protocol.state import InboundQoSState\n", "", 1)
    path.write_text(text)
