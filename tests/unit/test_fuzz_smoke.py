"""Pytest wrapper: run a bounded fuzz pass as a smoke test (jalon E).

The full fuzzer is ``tests/fuzz/fuzz.py`` (seed-reproducible). This keeps a
fast, deterministic slice in the unit suite so regressions are caught by the
normal test run; long runs stay a separate CI job / manual invocation.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

# Make tests/fuzz importable without packaging it.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "fuzz"))

import fuzz as fuzzmod  # noqa: E402


def test_fuzz_smoke_all_targets() -> None:
    rng = random.Random(1234)
    for name, fn in fuzzmod._TARGETS.items():
        res = fn(rng, 800)
        assert res.crashes == 0, f"{name} crashed: {res.first_failure}"
        assert res.invariant_violations == 0, f"{name} invariant: {res.first_failure}"
