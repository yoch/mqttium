from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def analysis(monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "benchmarks"))
    sys.modules.pop("open_loop_lag_analysis", None)
    return importlib.import_module("open_loop_lag_analysis")


def _sample(
    *,
    capacity: float,
    target: float,
    count: int = 20_000,
    cpu_us: float = 30.0,
    timer_ms: float = 0.1,
) -> dict[str, Any]:
    offered = min(capacity, target)
    backlog_ms = max(0.0, 0.95 * count * (1 / offered - 1 / target)) * 1000
    return {
        "count": count,
        "target_rate": target,
        "offered_rate": offered,
        "completed_rate": offered,
        "loop_lag_p95_ms": backlog_ms if backlog_ms > 0 else timer_ms,
        "cpu_seconds": cpu_us * count / 1_000_000,
    }


def _pairs(base: dict[str, Any], candidate: dict[str, Any], n: int = 4) -> list[dict[str, Any]]:
    orders = [["base", "candidate"], ["candidate", "base"]]
    return [{"order": orders[i % 2], "base": base, "candidate": candidate} for i in range(n)]


def test_backlog_prediction_matches_a_publisher_running_at_capacity(analysis) -> None:
    sample = _sample(capacity=25_000, target=26_000)

    assert analysis.predicted_backlog_ms(sample) == pytest.approx(sample["loop_lag_p95_ms"])
    assert analysis.view(sample, backlog_pace=0.99).regime == "backlog"


def test_a_small_capacity_gap_becomes_a_large_lag_ratio_in_backlog(analysis) -> None:
    # 4 % less capacity at the same saturating rate: the lag grows far more.
    target = 26_000
    base = _sample(capacity=25_500, target=target, cpu_us=30.0)
    candidate = _sample(capacity=24_500, target=target, cpu_us=31.2)
    cell = analysis.cell_report(
        _pairs(base, candidate), source="s", label="c", phase="p", backlog_pace=0.99, tolerance=0.35
    )

    assert cell.verdict == "backlog"
    assert cell.lag_ratio > 2.0
    assert cell.cpu_ratio == pytest.approx(1.04)
    assert cell.backlog_explained == cell.backlog_samples == 8


def test_paced_cells_compare_timer_lag(analysis) -> None:
    base = _sample(capacity=30_000, target=10_000, timer_ms=0.10)
    candidate = _sample(capacity=29_000, target=10_000, timer_ms=0.12)
    cell = analysis.cell_report(
        _pairs(base, candidate), source="s", label="c", phase="p", backlog_pace=0.99, tolerance=0.35
    )

    assert cell.verdict == "paced"
    assert cell.backlog_samples == 0
    assert cell.lag_ratio == pytest.approx(1.2)


def test_one_arm_falling_behind_is_mixed(analysis) -> None:
    base = _sample(capacity=27_000, target=26_000)
    candidate = _sample(capacity=25_000, target=26_000)
    cell = analysis.cell_report(
        _pairs(base, candidate), source="s", label="c", phase="p", backlog_pace=0.99, tolerance=0.35
    )

    assert cell.verdict == "mixed"


def test_backlog_far_from_the_prediction_is_not_explained(analysis) -> None:
    sample = _sample(capacity=25_000, target=26_000)
    sample["loop_lag_p95_ms"] *= 3
    view = analysis.view(sample, backlog_pace=0.99)

    assert not analysis.explained_by_backlog(view, tolerance=0.35)


def test_retained_gate_payload_is_walked_whatever_its_nesting(analysis, tmp_path: Path) -> None:
    base = _sample(capacity=25_500, target=26_000)
    candidate = _sample(capacity=24_500, target=26_000)
    payload = {
        "scenarios": [
            {
                "label": "protocol=5 payload=64 load=1.00",
                "baseline_capacity": 26_000.0,
                "candidate_capacity_diagnostic": 24_500.0,
                "target_rate": 26_000.0,
                "initial_pairs": _pairs(base, candidate),
                "confirmation": {"base_control": {"pairs": _pairs(base, base)}},
            }
        ],
        "preflight": {"sample": {"cpu_frequencies": {"0": 2400.0}, "firmware_throttled": "0x0"}},
    }
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "open-loop.json").write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "raw" / "broken.json").write_text("{", encoding="utf-8")

    result = analysis.analyse([tmp_path], backlog_pace=0.99, tolerance=0.35)

    assert [(cell["phase"], cell["verdict"]) for cell in result["cells"]] == [
        ("initial_pairs", "backlog"),
        ("pairs", "backlog"),
    ]
    assert result["calibrations"][0]["candidate_capacity_diagnostic"] == 24_500.0
    assert result["preflight"][0]["firmware_throttled"] == "0x0"
    assert result["summary"]["lag_ratio_above_1_05_by_regime"]["backlog"] == 1
    assert "| raw/open-loop.json |" in analysis.markdown(result)


def test_cli_writes_json_and_markdown(analysis, tmp_path: Path) -> None:
    source = tmp_path / "in.json"
    source.write_text(
        json.dumps(
            {"pairs": _pairs(_sample(capacity=9e4, target=1e4), _sample(capacity=9e4, target=1e4))}
        ),
        encoding="utf-8",
    )

    code = analysis.main(
        [
            str(source),
            "--output",
            str(tmp_path / "out.json"),
            "--summary-output",
            str(tmp_path / "out.md"),
        ]
    )

    assert code == 0
    assert json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))["summary"]["cells"] == 1
    assert (tmp_path / "out.md").read_text(encoding="utf-8").startswith("# Open-loop lag analysis")
