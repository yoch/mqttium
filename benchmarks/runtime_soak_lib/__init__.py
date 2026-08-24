"""Deterministic runtime soak / quiescence harness (research)."""

from __future__ import annotations

from benchmarks.runtime_soak_lib.ownership import (
    OwnershipSnapshot,
    connected_idle_violations,
    take_ownership,
)
from benchmarks.runtime_soak_lib.profiles import PROFILES, SoakProfile
from benchmarks.runtime_soak_lib.runner import SoakFailure, run_soak
from benchmarks.runtime_soak_lib.schedule import Op, reduce_schedule, schedule_for_seed

__all__ = [
    "Op",
    "OwnershipSnapshot",
    "PROFILES",
    "connected_idle_violations",
    "SoakFailure",
    "SoakProfile",
    "reduce_schedule",
    "run_soak",
    "schedule_for_seed",
    "take_ownership",
]
