from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest

SOFTIRQS = """\
                    CPU0       CPU1       CPU2       CPU3
          HI:          0          0          0          0
       TIMER:     123456      23456      34567      45678
      NET_TX:         10          2         30          4
      NET_RX:       1000        200       3000        400
       BLOCK:          0          0          0          0
"""

SS = """\
ESTAB 0      0      127.0.0.1:11883 127.0.0.1:40000
\t cubic wscale:7,7 rto:201 rtt:0.123/0.05 ato:40 mss:32768 cwnd:10 bytes_acked:1 send 21.3Gbps pacing_rate 42.6Gbps delivery_rate 1.2Gbps busy:120ms rwnd_limited:30ms(25.0%) unacked:3 rcv_space:65483 notsent:4096
ESTAB 0      0      127.0.0.1:40000 127.0.0.1:11883
\t cubic rtt:0.2/0.1 cwnd:12 send 900Mbps delivery_rate 800Mbps busy:100ms sndbuf_limited:5ms(5.0%)
"""


@pytest.fixture
def probe(monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "benchmarks"))
    sys.modules.pop("saturation_regime_probe", None)
    return importlib.import_module("saturation_regime_probe")


def test_softirq_rows_are_read_per_cpu(probe) -> None:
    rows = probe.parse_softirqs(SOFTIRQS)

    assert rows == {"NET_TX": [10, 2, 30, 4], "NET_RX": [1000, 200, 3000, 400]}


def test_ss_tcp_info_is_read_per_connection(probe) -> None:
    first, second = probe.parse_ss(SS)

    assert first["rtt_ms"] == 0.123
    assert first["cwnd"] == 10
    assert first["send_rate"] == pytest.approx(21.3e9)
    assert first["delivery_rate"] == pytest.approx(1.2e9)
    assert first["busy_ms"] == 120
    assert first["rwnd_limited_ms"] == 30
    assert first["unacked"] == 3
    assert first["notsent"] == 4096
    assert second["sndbuf_limited_ms"] == 5
    assert second["send_rate"] == pytest.approx(900e6)


def test_levels_split_at_relative_throughput_gaps(probe) -> None:
    rates = [26_000, 20_500, 25_900, 20_400, 22_300, 26_100]

    levels = probe.split_levels(rates, gap=0.02)

    assert [[rates[i] for i in level] for level in levels] == [
        [20_400, 20_500],
        [22_300],
        [25_900, 26_000, 26_100],
    ]


def _sample(rate: float, broker_cpu: float, *, seed: int) -> dict[str, Any]:
    count = 40_000
    return {
        "cell": "protocol=5 payload=4096 rate=26000",
        "count": count,
        "result": {
            "completed_rate": rate,
            "target_rate": 26_000.0,
            "offered_rate": rate,
            "cpu_seconds": count / rate,
            "measurement_seconds": count / rate,
            "writer_batches": count / 4,
            "writer_batched_items": count,
            "loop_lag_p95_ms": 1.0 + seed * 0.01,
        },
        "system": {
            "children": {
                "user": 1.0 + seed * 0.01,
                "system": 0.5,
                "voluntary": 100 + seed,
                "involuntary": 5,
            },
            "broker": {"cpu": broker_cpu * count / rate, "voluntary": 10, "involuntary": 1},
            "softirqs": {"NET_RX": [10, 0, 20 + seed, 0]},
            "frequencies_mid": {"cpu0": 2_400_000},
            "tcp": [{"cwnd": 10.0, "rwnd_limited_ms": 0.0}],
        },
    }


def test_summary_ranks_the_indicator_that_follows_the_level(probe) -> None:
    # The broker is saturated on the low level only: its busy share separates.
    samples = [_sample(20_500 + i * 10, 1.0, seed=i) for i in range(4)] + [
        _sample(26_000 + i * 10, 0.7, seed=i) for i in range(4)
    ]

    summary = probe.summarise(samples)
    cell = summary["cells"][0]

    assert [level["samples"] for level in cell["levels"]] == [4, 4]
    ranked = [item["indicator"] for item in cell["separating_indicators"]]
    assert "broker_busy_share" in ranked[:4]
    # Indicators that do not move with the level are not reported as separating.
    scores = {item["indicator"]: item["separation"] for item in cell["separating_indicators"]}
    assert scores.get("tcp_max_cwnd", 0.0) == 0.0
    assert scores.get("writer_items_per_batch", 0.0) == 0.0
    low, high = cell["levels"][0]["medians"], cell["levels"][-1]["medians"]
    assert low["broker_busy_share"] == pytest.approx(1.0)
    assert high["broker_busy_share"] == pytest.approx(0.7)
    assert low["writer_items_per_batch"] == pytest.approx(4.0)
    assert "| broker_busy_share |" in probe.markdown(summary)


def test_one_level_reports_no_separation(probe) -> None:
    summary = probe.summarise([_sample(26_000 + i, 0.8, seed=i) for i in range(3)])

    assert len(summary["cells"][0]["levels"]) == 1
    assert summary["cells"][0]["separating_indicators"] == []


def test_summarise_mode_reads_retained_samples(probe, tmp_path: Path) -> None:
    retained = tmp_path / "probe.json"
    samples = [_sample(20_500, 1.0, seed=0), _sample(26_000, 0.7, seed=1)]
    retained.write_text(json.dumps({"root": "candidate", "samples": samples}), encoding="utf-8")
    summary_path = tmp_path / "summary.md"

    assert probe.main(["--summarise", str(retained), "--summary-output", str(summary_path)]) == 0

    assert summary_path.read_text(encoding="utf-8").startswith("# Saturation regime probe")
    assert json.loads((tmp_path / "summary-summary.json").read_text(encoding="utf-8"))["cells"]


def test_paced_cells_size_samples_by_rate(probe) -> None:
    args = probe.parse_args(["--root", "x", "--sample-seconds", "2", "--max-count", "60000"])

    assert probe.sample_count(probe.Cell("5", 4096, 26_000), args) == 52_000
    assert probe.sample_count(probe.Cell("5", 4096, 0), args) == args.count_large
    assert probe.sample_count(probe.Cell("5", 64, 0), args) == args.count_small


def test_rank_correlation_handles_ties_and_constants(probe) -> None:
    assert probe.rank_correlation([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert probe.rank_correlation([1, 2, 3, 4], [4, 3, 3, 1]) == pytest.approx(-0.9486833)
    assert probe.rank_correlation([1, 2, 3], [5, 5, 5]) is None


def test_dispersed_cell_reports_profile_and_correlations(probe) -> None:
    # One continuous spread without a 2 % gap: broker cost rises as throughput falls.
    samples = []
    for i in range(8):
        sample = _sample(21_000 + i * 250, 1.0 - i * 0.02, seed=i)
        sample["result"]["offered_seconds"] = 2.0
        sample["result"]["measurement_seconds"] = 2.0 + 0.001 * (8 - i)
        samples.append(sample)

    summary = probe.summarise(samples)
    cell = summary["cells"][0]

    assert len(cell["levels"]) == 1
    assert cell["completed_cv"] > probe.DISPERSED_CV
    correlations = {item["indicator"]: item["rho"] for item in cell["rate_correlations"]}
    assert correlations["broker_cpu_us_per_msg"] == pytest.approx(-1.0)
    assert correlations["completion_tail_ms"] == pytest.approx(-1.0)
    assert cell["profile"]["completion_tail_ms"] == pytest.approx(4.5)
    assert cell["profile"]["completed_over_target"] == pytest.approx(21_875 / 26_000)
    text = probe.markdown(summary)
    assert "## Cell profiles (medians)" in text
    assert "| broker_cpu_us_per_msg | -1.00 |" in text


def test_variants_expand_cells_and_worker_options(probe) -> None:
    args = probe.parse_args(
        ["--root", "x", "--protocols", "5", "--payloads", "4096", "--rates", "0,26000"]
    )

    cells = probe._cells(args)

    assert [cell.key for cell in cells[:3]] == [
        "protocol=5 payload=4096 rate=unpaced",
        "protocol=5 payload=4096 rate=unpaced variant=bounded",
        "protocol=5 payload=4096 rate=unpaced variant=gc-off",
    ]
    options = {cell.variant: probe.worker_options(cell, args) for cell in cells[:3]}
    assert options["unbounded"].max_unacknowledged_messages == 0
    assert not options["unbounded"].gc_disable
    assert options["bounded"].max_unacknowledged_messages == args.bounded_backlog
    assert options["gc-off"].gc_disable
    assert not hasattr(args, "gc_disable")


def test_unknown_variant_is_refused(probe) -> None:
    with pytest.raises(SystemExit):
        probe.parse_args(["--root", "x", "--variants", "unbounded,tuned"])


def test_collector_activity_is_reported_per_thousand_messages(probe) -> None:
    sample = _sample(26_000, 0.8, seed=0)
    sample["result"]["gc_collections"] = [400, 40, 4]

    row = probe.features(sample)

    assert row["gc_gen0_per_kmsg"] == pytest.approx(10.0)
    assert row["gc_gen2_per_kmsg"] == pytest.approx(0.1)
