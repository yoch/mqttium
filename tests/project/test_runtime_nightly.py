"""Contracts for the permanent V1 runtime-fuzzer nightly."""

from __future__ import annotations

from pathlib import Path

from tools.ci.runtime_campaign import (
    NIGHTLY_BASE_SEED,
    Shard,
    build_shards,
    nightly_seed_start,
)


ROOT = Path(__file__).parents[2]


def test_nightly_rotation_uses_monotonic_disjoint_50k_ranges() -> None:
    assert nightly_seed_start(1, 50_000) == NIGHTLY_BASE_SEED
    assert nightly_seed_start(2, 50_000) == NIGHTLY_BASE_SEED + 50_000
    assert nightly_seed_start(101, 50_000) == NIGHTLY_BASE_SEED + 5_000_000


def test_nightly_campaign_records_ten_contiguous_5k_shards() -> None:
    assert build_shards(seed_start=2_000_000, seeds=50_000, shard_size=5_000) == [
        Shard(shard_id=index, seed_start=2_000_000 + index * 5_000, seed_count=5_000)
        for index in range(10)
    ]


def test_nightly_workflow_is_arm64_non_pr_and_retains_evidence_for_30_days() -> None:
    workflow = (ROOT / ".github/workflows/runtime-fuzz-nightly.yml").read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert "pull_request:" not in workflow
    assert "runs-on: [self-hosted, linux, ARM64]" in workflow
    assert '--nightly-rotation-index "$GITHUB_RUN_NUMBER"' in workflow
    assert "--seeds 50000" in workflow
    assert "--steps 32" in workflow
    assert "--shard-size 5000" in workflow
    assert "retention-days: 30" in workflow
