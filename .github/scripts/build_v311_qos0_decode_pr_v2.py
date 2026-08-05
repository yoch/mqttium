from __future__ import annotations

import sys
from pathlib import Path

from build_v311_qos0_decode_pr import build


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_v311_qos0_decode_pr_v2.py ROOT")
    root = Path(sys.argv[1])
    build(root)
    benchmark = root / "benchmarks/qos0_v311_decode.py"
    text = benchmark.read_text()
    old = "from mqttium.enums import MQTTProtocolVersion, PacketType, QoS\n"
    new = "from mqttium.enums import MQTTProtocolVersion, PacketType\n"
    if old not in text:
        raise RuntimeError("benchmark enum import did not match")
    benchmark.write_text(text.replace(old, new, 1))
