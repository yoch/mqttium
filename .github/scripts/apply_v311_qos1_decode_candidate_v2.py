from __future__ import annotations

import sys
from pathlib import Path

from apply_v311_qos1_decode_candidate_v3 import apply


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_v311_qos1_decode_candidate_v2.py ROOT")
    apply(Path(sys.argv[1]))
