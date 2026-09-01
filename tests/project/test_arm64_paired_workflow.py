"""Contracts for the dedicated ARM64 paired-regression runner."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_arm64_paired_gate_requires_the_runner_python_version() -> None:
    workflow = (ROOT / ".github/workflows/arm64-paired-regression.yml").read_text(encoding="utf-8")

    assert "sys.version_info[:2] != (3, 14)" in workflow
    assert "expected system Python 3.14" in workflow
    assert "expected system Python 3.13" not in workflow
