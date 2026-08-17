from __future__ import annotations

import sys

import pytest

from benchmarks.network_release_gate import parse_args


def test_release_gate_defaults_to_validated_network_cells(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "network_release_gate.py",
            "--base-root",
            "base",
            "--candidate-root",
            "candidate",
        ],
    )

    args = parse_args()

    assert args.windows == "1,20,64"
    assert args.cycle_seeds == [0, 1, 2, 3, 4, 5]
    assert args.target_sample_seconds == 2.0
    assert args.inter_phase_quiet_seconds == 60.0
