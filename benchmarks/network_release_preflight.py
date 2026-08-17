"""Deterministic quiet-period wrapper for release network preflights.

The one-minute load average used by ``runner_probe.py`` necessarily includes the
benchmark phase that just finished. A release sequence therefore waits a fixed,
predeclared quiet period before each fresh eligibility check instead of probing
repeatedly until an eligible instant happens to appear.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


QUIET_SECONDS = 20.0


def main() -> int:
    time.sleep(QUIET_SECONDS)
    probe = Path(__file__).with_name("runner_probe.py")
    completed = subprocess.run([sys.executable, str(probe), *sys.argv[1:]], check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
