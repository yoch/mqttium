from __future__ import annotations

import sys
from pathlib import Path

from apply_v311_qos1_decode_candidate import apply


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_v311_qos1_decode_candidate_v2.py ROOT")
    root = Path(sys.argv[1])
    apply(root)
    path = root / "src/mqttium/protocol/inbound.py"
    text = path.read_text()
    old = "    require_nonzero_mid(mid, PacketType.PUBLISH)\n"
    new = '    require_nonzero_mid(mid, "PUBLISH")\n'
    if old not in text:
        raise RuntimeError("packet identifier validation did not match")
    path.write_text(text.replace(old, new, 1))
