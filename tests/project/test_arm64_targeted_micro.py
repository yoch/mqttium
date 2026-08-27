"""Security contracts for the temporary ARM64 targeted-micro profile."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[2]
WORKFLOW = ROOT / ".github/workflows/arm64-paired-regression.yml"


def test_targeted_micro_is_ref_bound_confirmed_and_serialized() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "pull_request:" not in workflow
    assert "group: mqttium-arm64-runner" in workflow
    assert "cancel-in-progress: false" in workflow
    assert (
        "if: inputs.profile == 'targeted-micro' && inputs.confirm_trusted_code "
        "&& github.ref == 'refs/heads/codex/arm64-pacer-diagnostics'"
    ) in workflow
    assert "runs-on: [self-hosted, linux, ARM64]" in workflow


def test_targeted_micro_keeps_inputs_out_of_shell_commands() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    targeted = workflow.split("  targeted-micro:\n", maxsplit=1)[1]

    assert "persist-credentials: false" in targeted
    assert '[[ "$value" =~ ^[0-9a-fA-F]{40}$ ]]' in targeted
    assert "REQUESTED_SCENARIOS: ${{ inputs.scenarios }}" in targeted
    assert 'raw = os.environ["REQUESTED_SCENARIOS"]' in targeted
    assert 're.compile(r"[a-z0-9][a-z0-9_-]{0,63}").fullmatch' in targeted
    assert "subprocess.run(" in targeted
    assert 'Path("candidate/benchmarks/paired_regression.py")' in targeted
    assert "eval " not in targeted
    assert "targeted-micro.json" in targeted
