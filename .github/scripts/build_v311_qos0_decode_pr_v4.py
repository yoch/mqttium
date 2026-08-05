from __future__ import annotations

import sys
from pathlib import Path

from build_v311_qos0_decode_pr import build


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_v311_qos0_decode_pr_v4.py ROOT")
    root = Path(sys.argv[1])
    build(root)

    benchmark = root / "benchmarks/qos0_v311_decode.py"
    benchmark_text = benchmark.read_text()
    old_import = "from mqttium.enums import MQTTProtocolVersion, PacketType, QoS\n"
    new_import = "from mqttium.enums import MQTTProtocolVersion, PacketType\n"
    if old_import not in benchmark_text:
        raise RuntimeError("benchmark enum import did not match")
    benchmark.write_text(benchmark_text.replace(old_import, new_import, 1))

    tests = root / "tests/unit/test_qos0_v311_decode_fastpath.py"
    test_text = tests.read_text()
    test_text = test_text.replace(
        "from mqttium.errors import MalformedPacketError, ProtocolError\n",
        "from mqttium.errors import MalformedPacketError\n",
        1,
    )
    test_text = test_text.replace(
        '        ("a/b/c", b"x" * 4096, False),\n',
        '        ("a/b/c", b"x" * 4096, False),\n'
        '        ("", b"existing-v311-parity", False),\n',
        1,
    )
    old_errors = (
        '        (0x00, pack_utf8("bench/+") + b"x", ProtocolError, "wildcards"),\n'
        '        (0x00, pack_utf8("") + b"x", ProtocolError, "empty"),\n'
    )
    new_errors = (
        '        (\n'
        '            0x00,\n'
        '            pack_utf8("bench/+") + b"x",\n'
        '            MalformedPacketError,\n'
        '            "wildcards",\n'
        '        ),\n'
    )
    if old_errors not in test_text:
        raise RuntimeError("validation parity cases did not match")
    tests.write_text(test_text.replace(old_errors, new_errors, 1))
