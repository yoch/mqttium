from __future__ import annotations

import importlib
import math
import socket
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest


@pytest.fixture
def ext_pacer(monkeypatch: pytest.MonkeyPatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "benchmarks"))
    sys.modules.pop("external_pacer", None)
    return importlib.import_module("external_pacer")


def test_token_roundtrip_preserves_sequence_deadline_and_emit(ext_pacer) -> None:
    packed = ext_pacer.pack_token(12, 1_000, 1_005)
    assert ext_pacer.unpack_token(packed) == (12, 1_000, 1_005)


def test_schedule_uses_absolute_start_plus_sequence_times_interval(ext_pacer) -> None:
    start = 10_000_000
    interval = 200_000.0
    assert ext_pacer.schedule_deadline(start, 0, interval) == start
    assert ext_pacer.schedule_deadline(start, 3, interval) == start + 600_000


def test_wait_until_reports_catchup_when_deadline_already_passed(ext_pacer) -> None:
    past = time.monotonic_ns() - 1_000_000
    assert ext_pacer.wait_until(past, 150_000) is True


def test_wait_until_reaches_a_near_future_deadline(ext_pacer) -> None:
    """The load-bearing property is that it never returns early.

    How closely it lands on the deadline is a property of the host, measured by
    the pacer-qualification run, not by CI on a shared runner.
    """
    delay_ns = 8_000_000
    start = time.monotonic_ns()
    deadline = start + delay_ns
    catchup = ext_pacer.wait_until(deadline, 150_000)
    elapsed = time.monotonic_ns() - start
    assert catchup is False
    assert elapsed >= delay_ns


def test_burst_stats_count_consecutive_catchup_runs(ext_pacer) -> None:
    stats = ext_pacer.burst_stats([False, True, True, False, True, False])
    assert stats["catchup_fraction"] == pytest.approx(0.5)
    assert stats["burst_count"] == 2
    assert stats["burst_max"] == 2
    assert stats["burst_mean"] == pytest.approx(1.5)


def test_percentile_interpolates_between_ranks(ext_pacer) -> None:
    assert ext_pacer.percentile([0.0, 10.0], 50) == pytest.approx(5.0)
    assert ext_pacer.percentile([1.0], 99) == pytest.approx(1.0)
    assert ext_pacer.percentile([], 50) != ext_pacer.percentile([], 50)  # NaN


@pytest.mark.skipif(
    not hasattr(socket, "AF_UNIX"),
    reason="the token transport is a Unix datagram socketpair",
)
def test_qualify_subprocess_keeps_sequence_and_no_loss(ext_pacer) -> None:
    args = Namespace(
        rate=5000.0,
        count=80,
        timeout=5.0,
        safety_margin_ns=150_000,
        harness_cpu=-1,
        pacer_cpu=-1,
        publisher_cpu=-1,
    )
    row = ext_pacer.run_qualify(args)
    assert row["sequence_ok"] is True
    assert row["received"] == 80
    assert row["lost_tokens"] == 0
    assert row["lost_sends"] == 0
    # No wall-clock assertion belongs here. Timing quality is exactly what
    # differs between hosts -- a macOS CI runner showed 130 ms lateness p95
    # where a quiet desktop shows 0.4 us -- and it is what `--mode qualify`
    # exists to measure on a controlled host. This test owns the deterministic
    # properties: the transport delivers every token, in order, losing none.
    assert set(row["emission_interval_us"]) >= {"p50", "p95", "p99"}
    assert math.isfinite(row["lateness_us"]["p95"])
    assert math.isfinite(row["transport_delay_us"]["p95"])


def test_delta_pct_is_relative_to_baseline(ext_pacer) -> None:
    assert ext_pacer._delta_pct(100.0, 125.0) == pytest.approx(25.0)
    assert ext_pacer._delta_pct(0.0, 1.0) != ext_pacer._delta_pct(0.0, 1.0)


def test_host_info_records_transport_and_margin(ext_pacer) -> None:
    info = ext_pacer.host_info()
    assert info["transport"] == "socketpair SOCK_DGRAM"
    assert info["safety_margin_ns"] == 150_000
    assert "affinity" in info


def test_cli_host_info_emits_json(ext_pacer) -> None:
    rc = ext_pacer.main(["--mode", "host-info"])
    assert rc == 0
