from benchmarks.open_loop_report import diagnose


def test_diagnose_exposes_absolute_loop_lag_for_relative_failure() -> None:
    report = {
        "status": "failed",
        "thresholds": {"max_loop_lag_ratio": 1.05},
        "invalidations": ["diagnostic invalidation"],
        "regressions": ["diagnostic regression"],
        "scenarios": [
            {
                "protocol": "311",
                "payload_bytes": 64,
                "window": 100,
                "completion": "receipt",
                "load_mode": "capacity_fraction",
                "load_fraction": 0.9,
                "median_candidate_over_base_loop_lag_p95": 2.0,
                "pairs": [
                    {
                        "base": {"loop_lag_p95_ms": 0.02},
                        "candidate": {"loop_lag_p95_ms": 0.04},
                    },
                    {
                        "base": {"loop_lag_p95_ms": 0.03},
                        "candidate": {"loop_lag_p95_ms": 0.06},
                    },
                ],
            }
        ],
    }

    diagnostic = diagnose(report)

    assert diagnostic["source_status"] == "failed"
    assert diagnostic["relative_loop_lag_failures"] == 1
    assert diagnostic["invalidations"] == ["diagnostic invalidation"]
    assert diagnostic["regressions"] == ["diagnostic regression"]

    scenario = diagnostic["scenarios"][0]
    assert scenario["relative_failure"] is True
    assert scenario["base_loop_lag_p95_ms_median"] == 0.025
    assert scenario["candidate_loop_lag_p95_ms_median"] == 0.05
    assert scenario["median_absolute_delta_ms"] == 0.025
    assert scenario["median_pair_delta_ms"] == 0.025
    assert scenario["max_abs_pair_delta_ms"] == 0.03
    assert scenario["pair_ratios"] == [2.0, 2.0]
