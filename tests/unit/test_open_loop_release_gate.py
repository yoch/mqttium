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


def test_load_points_default_to_fixed_rates_and_allow_explicit_fractions(gate) -> None:
    default = gate._load_points(None, None)
    fixed = gate._load_points(None, "5000,10000")
    assert gate._load_points("0.5", None) == [gate.LoadPoint("baseline_capacity_fraction", 0.5)]

    assert [(point.mode, point.value) for point in default] == [
        ("absolute_rate", rate) for rate in (5000, 10000, 15000, 20000, 22000, 24000, 26000)
    ]
    assert [(point.mode, point.value) for point in fixed] == [
        ("absolute_rate", 5000.0),
        ("absolute_rate", 10000.0),
    ]


def _measured_cycles(*, target=5_000.0, busy=0.5, candidate_loop=0.2):
    pairs = _cycles(2, base_loop=0.1, candidate_loop=candidate_loop)
    for pair in pairs:
        for sample in (pair["base"], pair["candidate"]):
            sample.update(
                count=5_000,
                cpu_seconds=busy,
                measurement_seconds=1.0,
                pacing_sleeps=100,
                offered_rate=target,
            )
    return pairs


@pytest.mark.parametrize("target,eligible", [(8_999.0, True), (9_000.0, False), (10_000.0, False)])
def test_lag_is_diagnostic_at_ninety_percent_capacity(gate, target, eligible) -> None:
    regime = gate.measurement_regime(
        _measured_cycles(target=target), target=target, capacity_floor=10_000.0
    )
    assert regime["loop_lag_eligible"] is eligible
    assert regime["cpu_cost_eligible"] is True


@pytest.mark.parametrize("busy", [0.95, 1.0])
def test_busy_cpu_is_not_reported_as_equal_per_message_cost(gate, busy) -> None:
    regime = gate.measurement_regime(
        _measured_cycles(busy=busy), target=5_000.0, capacity_floor=26_000.0
    )
    assert regime["loop_lag_eligible"] is False
    assert regime["cpu_us_per_message"] == {"base": None, "candidate": None}


@pytest.mark.parametrize("field,value", [("pacing_sleeps", 0), ("offered_rate", 4_000.0)])
def test_one_arm_leaving_pacing_regime_disables_lag_verdict(gate, field, value) -> None:
    pairs = _measured_cycles()
    pairs[-1]["candidate"][field] = value
    regime = gate.measurement_regime(pairs, target=5_000.0, capacity_floor=26_000.0)
    assert regime["loop_lag_eligible"] is False
    assert regime["diagnostic_reasons"]


def _record_args():
    from argparse import Namespace

    return Namespace(
        count_small=1_000,
        count_large=1_000,
        target_sample_seconds=1.0,
        max_count=50_000,
        initial_cycle_seeds=[10, 11],
        min_completed_ratio=0.97,
        max_loop_lag_ratio=1.05,
        max_confirmation_scenarios=4,
    )


def _initial(gate, monkeypatch, tmp_path, *, target, candidate_rate=100.0, candidate_rates=None):
    pairs = _measured_cycles(target=target)
    for index, pair in enumerate(pairs):
        pair["candidate"]["completed_rate"] = (
            candidate_rates[index] if candidate_rates is not None else candidate_rate
        )
    monkeypatch.setattr(gate, "_acquire_cycles", lambda *_a, **_kw: pairs)
    return gate._initial_record(
        _record_args(),
        spec=gate.ScenarioSpec("311", 64, "receipt", 100),
        point=gate.LoadPoint("absolute_rate", target),
        calibration=gate.Calibration(26_000, 26_000, 1_000, (22_000, 26_000, 26_000)),
        base_root=tmp_path,
        candidate_root=tmp_path,
    )


def test_passing_throughput_median_does_not_hide_saturation_uncertainty(
    gate, monkeypatch, tmp_path
) -> None:
    # Rounded ratios from MQTT 5 / 4096 B / 26k in run 36404229046.
    record = _initial(
        gate,
        monkeypatch,
        tmp_path,
        target=26_000,
        candidate_rates=[84.8023, 100.0214, 94.8331, 100.0079],
    )
    assert record["initial_metrics"]["throughput_median"] > 0.97
    assert record["initial_metrics"]["throughput"]["geometric_mean"] < 0.97
    assert record["loop_suspect"] is False
    assert record["throughput_suspect"] is True


def test_tight_throughput_interval_can_clear_initial_screen(gate, monkeypatch, tmp_path) -> None:
    record = _initial(gate, monkeypatch, tmp_path, target=26_000, candidate_rate=99.0)
    assert record["throughput_suspect"] is False


def test_retained_evidence_reevaluation_keeps_uncertain_throughput_invalid(gate) -> None:
    pairs = _cycles(2, base_loop=0.1, candidate_loop=0.1)
    for pair, rate in zip(pairs, [84.8023, 100.0214, 94.8331, 100.0079], strict=True):
        pair["candidate"]["completed_rate"] = rate
    payload = {
        "status": "invalid",
        "policy": "strict",
        "thresholds": {"min_completed_ratio": 0.97, "max_loop_lag_ratio": 1.05},
        "scenarios": [{"initial_pairs": pairs, "confirmation": None}],
        "failures": [],
        "invalidations": ["5 cells require confirmation; bounded maximum is 4"],
    }

    result = gate.reevaluate_confirmation_overflow(payload)

    assert result["status"] == "invalid"
    assert result["scenarios"][0]["throughput_suspect"] is True
    assert "still require fresh confirmation" in result["invalidations"][0]


def test_saturation_lag_does_not_consume_confirmation_but_throughput_still_does(
    gate, monkeypatch, tmp_path
) -> None:
    low = _initial(gate, monkeypatch, tmp_path, target=5_000)
    high = _initial(gate, monkeypatch, tmp_path, target=24_000)
    slow = _initial(gate, monkeypatch, tmp_path, target=26_000, candidate_rate=90)
    assert low["loop_suspect"] is True
    assert high["loop_suspect"] is False
    assert slow["loop_suspect"] is False
    assert slow["throughput_suspect"] is True
    seen = []

    def confirm(*_a, **kwargs):
        seen.extend(kwargs["suspects"])
        return ["confirmed throughput regression"], []

    monkeypatch.setattr(gate, "_confirm_suspects", confirm)
    result = {}
    status = gate._evaluate_records(
        _record_args(),
        records=[low, high, slow],
        result=result,
        base_root=tmp_path,
        candidate_root=tmp_path,
        raw_dir=tmp_path,
    )
    assert status == 1
    assert seen == [low, slow]
    assert result["status"] == "failed"


def test_saturation_only_matrix_is_invalid_instead_of_a_silent_pass(gate, monkeypatch, tmp_path):
    high = _initial(gate, monkeypatch, tmp_path, target=24_000)
    result = {}
    status = gate._evaluate_records(
        _record_args(),
        records=[high],
        result=result,
        base_root=tmp_path,
        candidate_root=tmp_path,
        raw_dir=tmp_path,
    )
    assert status == 2
    assert "add lower fixed rates" in result["invalidations"][0]


def test_fractional_multilevel_calibration_is_rejected_before_acquisition(gate, tmp_path):
    with pytest.raises(RuntimeError, match="calibration changes level"):
        gate._initial_record(
            _record_args(),
            spec=gate.ScenarioSpec("311", 64, "receipt", 100),
            point=gate.LoadPoint("baseline_capacity_fraction", 0.5),
            calibration=gate.Calibration(22_000, 26_000, 1_000, (16_500, 22_000, 26_000)),
            base_root=tmp_path,
            candidate_root=tmp_path,
        )


def test_preflight_report_must_be_eligible_even_if_probe_returns_zero(gate, monkeypatch, tmp_path):
    from argparse import Namespace

    def probe(command, **_kw):
        output = Path(command[command.index("--output") + 1])
        output.write_text(json.dumps({"eligible": False}))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(gate.subprocess, "run", probe)
    args = Namespace(
        runner_probe=Path("runner_probe.py"),
        preflight_wait_seconds=120,
        preflight_poll_seconds=5,
        preflight_consecutive_eligible=2,
        require_temperature=True,
    )
    with pytest.raises(RuntimeError, match="not eligible"):
        gate._fresh_preflight(args, label="mqtt5", raw_dir=tmp_path)


def test_confirmation_cannot_promote_a_changed_pacing_regime(gate, monkeypatch, tmp_path):
    record = _initial(gate, monkeypatch, tmp_path, target=5_000)
    args = _record_args()
    args.confirmation_cycle_seeds = [12, 13]
    monkeypatch.setattr(gate, "_fresh_preflight", lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(gate, "_acquire_cycles", lambda *_a, **_kw: _measured_cycles(busy=1.0))
    failures, invalidations = gate._confirm_record(
        args,
        record=record,
        index=1,
        base_root=tmp_path,
        candidate_root=tmp_path,
        raw_dir=tmp_path,
    )
    assert not failures
    assert "confirmation changed pacing regime" in invalidations[0]
    assert record["confirmation"]["status"] == "invalid_control"


def test_completed_cells_survive_later_ineligible_preflight(gate, monkeypatch, tmp_path):
    from argparse import Namespace

    def preflight(*_a, **kwargs):
        if kwargs["label"] == "ab-5":
            raise RuntimeError("runner preflight is not eligible")
        return tmp_path

    monkeypatch.setattr(gate, "_fresh_preflight", preflight)
    monkeypatch.setattr(gate, "_initial_record", lambda *_a, **_kw: {"retained": True})
    specs = [gate.ScenarioSpec(protocol, 64, "receipt", 100) for protocol in ("311", "5")]
    retained = []
    with pytest.raises(RuntimeError, match="not eligible"):
        gate._run_initial_matrix(
            Namespace(protocols="311,5"),
            specs=specs,
            load_points=[gate.LoadPoint("absolute_rate", 5_000)],
            calibrations=dict.fromkeys(specs),
            base_root=tmp_path,
            candidate_root=tmp_path,
            raw_dir=tmp_path,
            records=retained,
        )
    assert retained == [{"retained": True}]


@pytest.mark.parametrize(
    "initial_rate,extra_rate,extra_cycles,decision",
    [(90, 90, 2, "regression"), (94, 102, 2, "inconclusive"), (96.9, 97.2, 8, "cleared")],
)
def test_throughput_confirmation_requires_controls_and_confidence(
    gate, monkeypatch, tmp_path, initial_rate, extra_rate, extra_cycles, decision
):
    record = _initial(gate, monkeypatch, tmp_path, target=24_000, candidate_rate=initial_rate)
    args = _record_args()
    args.confirmation_cycle_seeds = [12, 13]
    args.control_max_throughput_deviation = 0.02
    extra = _measured_cycles(target=24_000, busy=1.0)
    extra *= extra_cycles // 2
    for pair in extra:
        pair["candidate"]["completed_rate"] = extra_rate
    controls = _measured_cycles(target=24_000, busy=1.0)
    seen = []

    def same_code(*_a, **kw):
        seen.append(kw["target"])
        return controls, controls

    monkeypatch.setattr(gate, "_fresh_preflight", lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(gate, "_acquire_cycles", lambda *_a, **_kw: extra)
    monkeypatch.setattr(gate, "_same_code_controls", same_code)
    failures, invalidations = gate._confirm_record(
        args, record=record, index=1, base_root=tmp_path, candidate_root=tmp_path, raw_dir=tmp_path
    )
    assert seen == [24_000]
    assert record["confirmation"]["throughput_decision"] == decision
    assert bool(failures) is (decision == "regression")
    assert bool(invalidations) is (decision == "inconclusive")
    assert record["confirmation"]["base_control"] is not None
    assert record["confirmation"]["candidate_control"] is not None


def test_centered_but_noisy_throughput_controls_cannot_confirm_a_regression(
    gate, monkeypatch, tmp_path
):
    record = _initial(gate, monkeypatch, tmp_path, target=24_000, candidate_rate=90)
    args = _record_args()
    args.confirmation_cycle_seeds = [12, 13]
    args.control_max_throughput_deviation = 0.02
    controls = _measured_cycles(target=24_000, busy=1.0)
    for index, pair in enumerate(controls):
        pair["candidate"]["completed_rate"] = 90 if index < 2 else 100 / 0.9
    assert gate.control_is_valid(controls, max_throughput_deviation=0.02)
    monkeypatch.setattr(gate, "_fresh_preflight", lambda *_a, **_kw: tmp_path)
    monkeypatch.setattr(gate, "_same_code_controls", lambda *_a, **_kw: (controls, controls))
    failures, invalidations = gate._confirm_record(
        args, record=record, index=1, base_root=tmp_path, candidate_root=tmp_path, raw_dir=tmp_path
    )
    assert failures  # Retain the observed drop, without treating the noisy run as valid.
    assert len(invalidations) == 2
    assert all("equivalence budget" in reason for reason in invalidations)
    assert record["confirmation"]["status"] == "invalid_control"
