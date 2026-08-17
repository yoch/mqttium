from __future__ import annotations

import math

import pytest

from benchmarks.network_release_gate import (
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


def test_raw_arm_cv_is_diagnostic_when_abba_estimator_is_precise() -> None:
    pairs = []
    for index, order in enumerate(ORDERS_12):
        base_rate = 100.0 if index % 3 else 1_000.0
        ratio = 0.99 if index % 2 == 0 else 1 / 0.99
        pairs.append(
            {
                "order": order,
                "base": {
                    "publisher_ack_msg_s": base_rate,
                    "ack_latency_p50_ms": 1.0,
                    "latency_p50_ms": 2.0,
                },
                "candidate": {
                    "publisher_ack_msg_s": base_rate * ratio,
                    "ack_latency_p50_ms": 1.0 * ratio,
                    "latency_p50_ms": 2.0 * ratio,
                },
            }
        )
    payload = {
        "scenarios": [
            {
                "protocol": "311",
                "completion": "callback",
                "payload_bytes": 64,
                "window": 20,
                "base_ack_cv": 0.80,
                "candidate_ack_cv": 0.79,
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

    assert evaluation.base_ack_cv == 0.80
    assert evaluation.candidate_ack_cv == 0.79
    assert evaluation.failures == ()
