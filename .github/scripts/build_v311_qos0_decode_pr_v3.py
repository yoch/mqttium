from __future__ import annotations

import sys
from pathlib import Path

from build_v311_qos0_decode_pr_v2 import build


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_v311_qos0_decode_pr_v3.py ROOT")
    root = Path(sys.argv[1])
    build(root)
    tests = root / "tests/unit/test_qos0_v311_decode_fastpath.py"
    text = tests.read_text()
    text = text.replace(
        "from mqttium.errors import MalformedPacketError, ProtocolError\n",
        "from mqttium.errors import MalformedPacketError\n",
        1,
    )
    text = text.replace(
        '        ("a/b/c", b"x" * 4096, False),\n',
        '        ("a/b/c", b"x" * 4096, False),\n        ("", b"existing-v311-parity", False),\n',
        1,
    )
    text = text.replace(
        '        (0x00, pack_utf8("bench/+") + b"x", ProtocolError, "wildcards"),\n'
        '        (0x00, pack_utf8("") + b"x", ProtocolError, "empty"),\n',
        '        (\n'
        '            0x00,\n'
        '            pack_utf8("bench/+") + b"x",\n'
        '            MalformedPacketError,\n'
        '            "wildcards",\n'
        '        ),\n',
        1,
    )
    tests.write_text(text)
