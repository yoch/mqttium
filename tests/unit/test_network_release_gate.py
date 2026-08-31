from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.network_release_gate import (
    _combine_phase_payloads,
    _fresh_preflight,
    _parse_control_cycle_seeds,
    _parse_cycle_seeds,
    _run_engine_cycle,
    abba_cycle_ratios,
    control_failures,
    evaluate_control_payload,
    paired_ratio_estimate,
    regression_failures,
)


ORDERS_12 = [["base", "candidate"], ["candidate", "base"]] * 6


def test_abba_cycle_ratios_cancel_opposite_order_bias() -> None:
    ratios = [0.98, 1.02] * 6

    cycles = abba_cycle_ratios(ratios, ORDERS_12)

    assert all(value == pytest.approx(math.sqrt(0.98 * 1.02)) for value in cycles)
    estimate = paired_ratio_estimate(ratios, ORDERS_12)
    assert estimate.pairs == 12
    assert estimate.cycles == 6
    assert estimate.pair_ratio_cv > 0.02
    assert estimate.upper_95 - estimate.lower_95 < 1e-12


def test_abba_cycle_ratios_reject_incomplete_or_wrong_order() -> None:
    with pytest.raises(ValueError, match="even number"):
        abba_cycle_ratios([1.0, 1.0, 1.0], ORDERS_12[:3])

    wrong = [["base", "candidate"], ["base", "candidate"]] * 2
    with pytest.raises(ValueError, match="complete ABBA"):
        abba_cycle_ratios([1.0] * 4, wrong)


def test_same_code_control_requires_small_bias_and_precise_equivalence() -> None:
    stable = paired_ratio_estimate(
        [0.995, 1.005, 1.002, 0.998, 0.997, 1.003, 1.004, 0.996, 1.0, 1.0, 0.999, 1.001],
        ORDERS_12,
    )
    assert (
        control_failures(
            "throughput",
            stable,
            bias_floor=0.98,
            bias_ceiling=1.02,
            equivalence_floor=0.95,
            equivalence_ceiling=1.05,
        )
        == []
    )

    narrow_but_biased = paired_ratio_estimate([1.03] * 12, ORDERS_12)
    failures = control_failures(
        "throughput",
        narrow_but_biased,
        bias_floor=0.98,
        bias_ceiling=1.02,
        equivalence_floor=0.95,
        equivalence_ceiling=1.05,
    )
    assert any("outside bias budget" in failure for failure in failures)
    assert not any("exceeds equivalence" in failure for failure in failures)

    noisy = paired_ratio_estimate(
        [0.80, 0.90, 1.10, 1.20, 0.85, 0.95, 1.05, 1.15, 0.82, 0.92, 1.08, 1.18],
        ORDERS_12,
    )
    failures = control_failures(
        "throughput",
        noisy,
        bias_floor=0.98,
        bias_ceiling=1.02,
        equivalence_floor=0.95,
        equivalence_ceiling=1.05,
    )
    assert any("exceeds equivalence" in failure for failure in failures)


def test_same_code_control_does_not_require_ci_to_contain_exactly_one() -> None:
    slightly_offset = paired_ratio_estimate([0.997] * 12, ORDERS_12)

    assert (
        control_failures(
            "throughput",
            slightly_offset,
            bias_floor=0.98,
            bias_ceiling=1.02,
            equivalence_floor=0.95,
            equivalence_ceiling=1.05,
        )
        == []
    )
    assert slightly_offset.upper_95 < 1.0


def test_regression_gate_uses_confidence_bounds_not_raw_arm_cv() -> None:
    throughput = paired_ratio_estimate([0.99, 1.01] * 6, ORDERS_12)
    ack = paired_ratio_estimate([0.98, 1.02] * 6, ORDERS_12)
    assert regression_failures("cell", throughput, ack, min_throughput=0.95, max_ack_p50=1.10) == []

    slow = paired_ratio_estimate([0.92] * 12, ORDERS_12)
    failures = regression_failures("cell", slow, ack, min_throughput=0.95, max_ack_p50=1.10)
    assert any("throughput lower 95% bound" in failure for failure in failures)

    latent = paired_ratio_estimate([1.15] * 12, ORDERS_12)
    failures = regression_failures(
        "cell", throughput, latent, min_throughput=0.95, max_ack_p50=1.10
    )
    assert any("ACK p50 upper 95% bound" in failure for failure in failures)


def _worker(rate: float, *, ack: float = 1.0, delivery: float = 2.0) -> dict[str, float]:
    return {
        "publisher_ack_msg_s": rate,
        "ack_latency_p50_ms": ack,
        "latency_p50_ms": delivery,
    }


def _cycle_payload(*, ratio: float = 1.0, window: int = 8) -> dict[str, object]:
    return {
        "eligibility": {"eligible": True},
        "status": "passed",
        "scenarios": [
            {
                "protocol": "311",
                "completion": "callback",
                "payload_bytes": 64,
                "window": window,
                "pairs": [
                    {
                        "order": ["base", "candidate"],
                        "base": _worker(100.0),
                        "candidate": _worker(100.0 * ratio),
                    },
                    {
                        "order": ["candidate", "base"],
                        "base": _worker(100.0),
                        "candidate": _worker(100.0 * ratio),
                    },
                ],
            }
        ],
    }


def test_phase_combiner_preserves_seeded_complete_abba_cycles() -> None:
    combined = _combine_phase_payloads(
        [
            (1, 0, _cycle_payload()),
            (1, 1, _cycle_payload()),
            (2, 0, _cycle_payload()),
            (2, 1, _cycle_payload()),
        ]
    )

    [scenario] = combined["scenarios"]
    pairs = scenario["pairs"]
    assert [pair["hash_seed"] for pair in pairs] == [0, 0, 1, 1, 0, 0, 1, 1]
    assert [pair["block"] for pair in pairs] == [1, 1, 1, 1, 2, 2, 2, 2]

    throughput = paired_ratio_estimate(
        [
            pair["candidate"]["publisher_ack_msg_s"] / pair["base"]["publisher_ack_msg_s"]
            for pair in pairs
        ],
        [pair["order"] for pair in pairs],
    )
    assert throughput.pairs == 8
    assert throughput.cycles == 4
    assert throughput.geometric_mean == pytest.approx(1.0)


def test_phase_combiner_rejects_scenario_drift() -> None:
    with pytest.raises(RuntimeError, match="scenario set/order changed"):
        _combine_phase_payloads(
            [
                (1, 0, _cycle_payload(window=8)),
                (1, 1, _cycle_payload(window=20)),
            ]
        )


def test_cycle_seed_schedule_is_deterministic_and_sufficient() -> None:
    assert _parse_cycle_seeds("0,1,2,3,4,5") == [0, 1, 2, 3, 4, 5]
    assert _parse_control_cycle_seeds("0,1,2") == [0, 1, 2]

    with pytest.raises(argparse.ArgumentTypeError, match="at least 6"):
        _parse_cycle_seeds("0,1,2,3,4")
    with pytest.raises(argparse.ArgumentTypeError, match="at least 2"):
        _parse_control_cycle_seeds("0")
    with pytest.raises(argparse.ArgumentTypeError, match="duplicates"):
        _parse_cycle_seeds("0,1,2,3,4,4")
    with pytest.raises(argparse.ArgumentTypeError, match="invalid deterministic"):
        _parse_cycle_seeds("0,1,2,3,4,random")


def test_seeded_engine_cycle_sets_pythonhashseed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    args = SimpleNamespace(
        engine=Path("benchmarks/paired_network.py"),
        host="127.0.0.1",
        port=11883,
        protocols="311",
        payloads="64",
        windows="8",
        completions="callback",
        count_small=6_000,
        count_large=3_000,
        target_sample_seconds=2.0,
        max_count=60_000,
        timeout=60.0,
        cpu=2,
    )
    preflight_report = tmp_path / "preflight.json"
    preflight_report.write_text('{"eligible": true}\n', encoding="utf-8")
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    def fake_run(command: list[str], *, check: bool, env: dict[str, str]) -> SimpleNamespace:
        assert check is False
        assert env["PYTHONHASHSEED"] == "5"
        output = Path(command[command.index("--output") + 1])
        output.write_text(json.dumps(_cycle_payload()) + "\n", encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("benchmarks.network_release_gate.subprocess.run", fake_run)

    payload = _run_engine_cycle(
        args,
        label="seed-5",
        base_root=tmp_path,
        candidate_root=tmp_path,
        raw_dir=raw_dir,
        preflight_report=preflight_report,
        hash_seed=5,
    )
    assert payload["status"] == "passed"


def test_followup_preflight_ignores_only_historical_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: list[str] = []

    def fake_run(command: list[str], *, check: bool) -> SimpleNamespace:
        assert check is False
        captured.extend(command)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("benchmarks.network_release_gate.subprocess.run", fake_run)
    args = SimpleNamespace(runner_probe=Path("benchmarks/runner_probe.py"))

    report = _fresh_preflight(
        args,
        label="followup",
        raw_dir=tmp_path,
        quiet_seconds=0.0,
        ignore_historical_load=True,
    )

    assert report == tmp_path / "followup-preflight.json"
    assert "--ignore-historical-load" in captured
    assert "--require-temperature" in captured
    assert "--enforce" in captured


def test_raw_arm_cv_is_diagnostic_when_abba_estimator_is_precise() -> None:
    pairs = []
    for index, order in enumerate(ORDERS_12):
        base_rate = 100.0 if index % 3 else 1_000.0
        ratio = 0.99 if index % 2 == 0 else 1 / 0.99
        pairs.append(
            {
                "order": order,
                "base": _worker(base_rate),
                "candidate": _worker(base_rate * ratio, ack=ratio, delivery=2.0 * ratio),
            }
        )
    payload = {
        "scenarios": [
            {
                "protocol": "311",
                "completion": "callback",
                "payload_bytes": 64,
                "window": 8,
                "pairs": pairs,
            }
        ]
    }

    [evaluation] = evaluate_control_payload(
        payload,
        throughput_bias_floor=0.98,
        throughput_bias_ceiling=1.02,
        throughput_equivalence_floor=0.95,
        throughput_equivalence_ceiling=1.05,
        ack_p50_bias_floor=0.95,
        ack_p50_bias_ceiling=1.05,
        ack_p50_equivalence_floor=0.90,
        ack_p50_equivalence_ceiling=1.10,
    )

    assert evaluation.base_ack_cv > 1.0
    assert evaluation.candidate_ack_cv > 1.0
    assert evaluation.failures == ()
