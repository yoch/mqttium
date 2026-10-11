"""Contracts for the dedicated ARM64 paired-regression runner."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_arm64_workflows_require_the_runner_python_version() -> None:
    workflows = (
        "arm64-paired-regression.yml",
        "arm64-qos1-rtt-tournament.yml",
        "published-arm64-smoke.yml",
        "runtime-fuzz-nightly.yml",
    )

    for name in workflows:
        workflow = (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
        assert "sys.version_info[:2] != (3, 14)" in workflow
        assert "expected system Python 3.14" in workflow
        assert "expected system Python 3.13" not in workflow


def test_arm64_ci_isolates_python315_and_preserves_the_system_reference() -> None:
    workflow = (ROOT / ".github/workflows/arm64-ci.yml").read_text(encoding="utf-8")
    assert 'python-version: "3.14"' in workflow
    assert 'python-version: "3.15"' in workflow
    assert "executable: python3.15" in workflow
    assert '"$PYTHON_BIN" -m venv' in workflow
    assert "sys.version_info[:2] != expected" in workflow
    assert "max-parallel: 1" in workflow
    assert "ref: ${{ github.sha }}" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
