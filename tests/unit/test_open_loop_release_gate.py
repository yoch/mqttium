from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def gate(monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "benchmarks"))
    sys.modules.pop("open_loop_release_gate", None)
    return importlib.import_module("open_loop_release_gate")


def _pair(
    order: list[str],
    *,
    base_loop: float = 0.1,
    candidate_loop: float = 0.1,
    base_rate: float = 100.0,
    candidate_rate: float = 100.0,
    base_ack: float = 0.5,
    candidate_ack: float = 0.5,
) -> dict[str, Any]:
    return {
        "order": order,
        "base": {
            "completed_rate": base_rate,
            "loop_lag_p95_ms": base_loop,
            "ack_latency_p50_ms": base_ack,
        },
        "candidate": {
            "completed_rate": candidate_rate,
            "loop_lag_p95_ms": candidate_loop,
            "ack_latency_p50_ms": candidate_ack,
        },
    }


def _cycles(
    count: int,
    *,
    base_loop: float,
    candidate_loop: float,
    base_rate: float = 100.0,
    candidate_rate: float = 100.0,
) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for _ in range(count):
        pairs.extend(
            (
                _pair(
                    ["base", "candidate"],
                    base_loop=base_loop,
                    candidate_loop=candidate_loop,
                    base_rate=base_rate,
                    candidate_rate=candidate_rate,
                ),
                _pair(
                    ["candidate", "base"],
                    base_loop=base_loop,
                    candidate_loop=candidate_loop,
                    base_rate=base_rate,
                    candidate_rate=candidate_rate,
                ),
            )
        )
    return pairs


def test_fractional_target_is_anchored_only_to_baseline_capacity(gate) -> None:
    calibration = gate.Calibration(
        baseline_capacity=10_000.0,
        candidate_capacity=4_000.0,
        count=2_500,
        baseline_samples=(9_900.0, 10_000.0, 10_100.0),
    )

    assert (
        gate.target_rate(calibration, gate.LoadPoint("baseline_capacity_fraction", 0.9)) == 9_000.0
    )
    assert gate.target_rate(calibration, gate.LoadPoint("absolute_rate", 7_500.0)) == 7_500.0


def test_calibration_count_targets_duration_without_shrinking_pilot(gate) -> None:
    assert gate.calibration_count(20_000.0, 2_000, 0.25, 50_000) == 5_000
    assert gate.calibration_count(2_000.0, 2_000, 0.25, 50_000) == 2_000
    assert gate.calibration_count(1_000_000.0, 2_000, 0.25, 50_000) == 50_000


def test_abba_difference_estimate_balances_opposite_orders(gate) -> None:
    differences = [0.02, 0.04, 0.01, 0.03]
    orders = [
        ["base", "candidate"],
        ["candidate", "base"],
        ["base", "candidate"],
        ["candidate", "base"],
    ]

    assert gate.abba_cycle_differences(differences, orders) == [0.03, 0.02]
    estimate = gate.paired_difference_estimate(differences, orders)
    assert estimate.pairs == 4
    assert estimate.cycles == 2
    assert estimate.mean_ms == pytest.approx(0.025)


def test_loop_ratio_alone_does_not_confirm_regression_inside_same_code_noise(gate) -> None:
    ab = _cycles(4, base_loop=0.10, candidate_loop=0.12)
    base_control = _cycles(2, base_loop=0.10, candidate_loop=0.13)
    candidate_control = _cycles(2, base_loop=0.10, candidate_loop=0.11)

    confirmed, noise_floor = gate.confirmed_loop_regression(
        ab,
        base_control_pairs=base_control,
        candidate_control_pairs=candidate_control,
        max_loop_lag_ratio=1.05,
    )

    assert gate.metrics(ab).loop_lag_ratio.geometric_mean == pytest.approx(1.2)
    assert noise_floor == pytest.approx(0.03)
    assert confirmed is False


def test_loop_regression_requires_relative_confidence_and_absolute_materiality(gate) -> None:
    ab = _cycles(4, base_loop=0.10, candidate_loop=0.20)
    base_control = _cycles(2, base_loop=0.10, candidate_loop=0.105)
    candidate_control = _cycles(2, base_loop=0.10, candidate_loop=0.11)

    confirmed, noise_floor = gate.confirmed_loop_regression(
        ab,
        base_control_pairs=base_control,
        candidate_control_pairs=candidate_control,
        max_loop_lag_ratio=1.05,
    )

    estimate = gate.metrics(ab)
    assert estimate.loop_lag_ratio.lower_95 > 1.0
    assert estimate.loop_lag_delta.lower_95_ms > noise_floor
    assert confirmed is True


def test_initial_loop_screen_ignores_an_unconfirmed_point_ratio(gate) -> None:
    pairs = _cycles(1, base_loop=0.10, candidate_loop=0.20)
    pairs.extend(_cycles(1, base_loop=0.10, candidate_loop=0.10))

    estimate = gate.metrics(pairs)

    assert estimate.loop_lag_median_ratio == pytest.approx(1.5)
    assert estimate.loop_lag_ratio.lower_95 < 1.0
    assert estimate.loop_lag_delta.lower_95_ms < 0.0
    assert gate.loop_requires_confirmation(pairs, max_loop_lag_ratio=1.05) is False


def test_initial_loop_screen_keeps_a_consistent_material_regression(gate) -> None:
    pairs = _cycles(2, base_loop=0.10, candidate_loop=0.20)

    assert gate.loop_requires_confirmation(pairs, max_loop_lag_ratio=1.05) is True


def test_strict_cli_explains_an_invalid_decision_without_a_traceback(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "open_loop_release_gate.py"),
            "--base-root",
            str(root),
            "--candidate-root",
            str(root),
            "--policy",
            "strict",
            "--output",
            str(tmp_path / "result.json"),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "strict open-loop release evidence requires --cpu pinning" in completed.stderr
    assert "Traceback" not in completed.stderr


def test_confirmation_budget_overflow_can_be_reevaluated_from_retained_pairs(gate) -> None:
    pairs = _cycles(1, base_loop=0.10, candidate_loop=0.20)
    pairs.extend(_cycles(1, base_loop=0.10, candidate_loop=0.10))
    payload = {
        "status": "invalid",
        "policy": "strict",
        "thresholds": {"min_completed_ratio": 0.97, "max_loop_lag_ratio": 1.05},
        "scenarios": [
            {
                "label": "noisy cell",
                "initial_pairs": pairs,
                "initial_metrics": {},
                "final_metrics": {},
                "throughput_suspect": False,
                "loop_suspect": True,
                "confirmation": None,
            }
        ],
        "failures": [],
        "invalidations": ["15 cells require confirmation; bounded maximum is 4"],
    }

    reevaluated = gate.reevaluate_confirmation_overflow(payload)

    assert reevaluated["status"] == "passed"
    assert reevaluated["scenarios"][0]["loop_suspect"] is False
    assert reevaluated["reevaluation"]["original_status"] == "invalid"
    assert payload["status"] == "invalid"


def test_reevaluation_refuses_to_pass_a_cell_that_still_needs_confirmation(gate) -> None:
    pairs = _cycles(2, base_loop=0.10, candidate_loop=0.20)
    payload = {
        "status": "invalid",
        "policy": "strict",
        "thresholds": {"min_completed_ratio": 0.97, "max_loop_lag_ratio": 1.05},
        "scenarios": [
            {
                "label": "material cell",
                "initial_pairs": pairs,
                "initial_metrics": {},
                "final_metrics": {},
                "throughput_suspect": False,
                "loop_suspect": True,
                "confirmation": None,
            }
        ],
        "failures": [],
        "invalidations": ["5 cells require confirmation; bounded maximum is 4"],
    }

    reevaluated = gate.reevaluate_confirmation_overflow(payload)

    assert reevaluated["status"] == "invalid"
    assert reevaluated["invalidations"] == [
        "1 cells still require fresh confirmation under the corrected screen"
    ]


def test_reevaluation_cli_writes_separate_traceable_evidence(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    pairs = _cycles(1, base_loop=0.10, candidate_loop=0.20)
    pairs.extend(_cycles(1, base_loop=0.10, candidate_loop=0.10))
    source = tmp_path / "original.json"
    output = tmp_path / "reevaluated.json"
    source.write_text(
        json.dumps(
            {
                "status": "invalid",
                "policy": "strict",
                "base_sha": "a" * 40,
                "candidate_sha": "b" * 40,
                "thresholds": {
                    "min_completed_ratio": 0.97,
                    "max_loop_lag_ratio": 1.05,
                },
                "scenarios": [
                    {
                        "label": "noisy cell",
                        "initial_pairs": pairs,
                        "initial_metrics": {},
                        "final_metrics": {},
                        "throughput_suspect": False,
                        "loop_suspect": True,
                        "confirmation": None,
                    }
                ],
                "failures": [],
                "invalidations": ["15 cells require confirmation; bounded maximum is 4"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(root / "benchmarks" / "reevaluate_open_loop_release_gate.py"),
            "--input",
            str(source),
            "--output",
            str(output),
        ],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["status"] == "passed"
    assert result["reevaluation"]["source_artifact_sha256"]
    assert json.loads(source.read_text(encoding="utf-8"))["status"] == "invalid"


def test_same_code_throughput_control_reuses_existing_two_percent_bias_budget(gate) -> None:
    stable = _cycles(
        2,
        base_loop=0.10,
        candidate_loop=0.10,
        base_rate=100.0,
        candidate_rate=101.0,
    )
    biased = _cycles(
        2,
        base_loop=0.10,
        candidate_loop=0.10,
        base_rate=100.0,
        candidate_rate=103.0,
    )

    assert gate.control_is_valid(stable, max_throughput_deviation=0.02) is True
    assert gate.control_is_valid(biased, max_throughput_deviation=0.02) is False


def test_load_points_keep_default_curve_and_support_absolute_rates(gate) -> None:
    default = gate._load_points(None, None)
    fixed = gate._load_points(None, "5000,10000")

    assert [(point.mode, point.value) for point in default] == [
        ("baseline_capacity_fraction", 0.5),
        ("baseline_capacity_fraction", 0.75),
        ("baseline_capacity_fraction", 0.9),
        ("baseline_capacity_fraction", 1.0),
    ]
    assert [(point.mode, point.value) for point in fixed] == [
        ("absolute_rate", 5000.0),
        ("absolute_rate", 10000.0),
    ]
