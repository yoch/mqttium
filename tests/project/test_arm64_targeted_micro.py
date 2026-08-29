"""Security contracts for the temporary targeted ARM64 profiles."""

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
    assert "PYTHONPATH: candidate/src" in targeted


def test_targeted_protocol_responses_runs_strict_control_before_gate() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    targeted = workflow.split("  targeted-protocol-responses:\n", maxsplit=1)[1]

    assert (
        "if: inputs.profile == 'targeted-protocol-responses' "
        "&& inputs.confirm_trusted_code "
        "&& github.ref == 'refs/heads/codex/arm64-pacer-diagnostics'"
    ) in targeted
    assert targeted.count("persist-credentials: false") == 3
    assert '[[ "$value" =~ ^[0-9a-fA-F]{40}$ ]]' in targeted
    assert 'test "$(git -C base rev-parse HEAD)" = "${BASE_REF,,}"' in targeted
    assert 'test "$(git -C candidate rev-parse HEAD)" = "${CANDIDATE_REF,,}"' in targeted
    assert "--wait-seconds 60" in targeted
    assert "--consecutive-eligible 2" in targeted
    assert "--base-root base \\\n            --candidate-root base" in targeted
    assert "--base-root base \\\n            --candidate-root candidate" in targeted
    assert targeted.index("--candidate-root base") < targeted.index("--candidate-root candidate")
    assert targeted.count("--repeat 8") == 2
    assert targeted.count("--cpu 2") == 2
    assert targeted.count("--policy strict") == 2
    assert targeted.count('--preflight-report "$JOB_ROOT/runner.json"') == 2
    assert targeted.count("candidate/benchmarks/paired_protocol_responses.py") == 2
    campaign = targeted.split("      - name: Strict protocol-response A/A control\n", maxsplit=1)[
        1
    ].split("      - name: Upload protocol-response measurements\n", maxsplit=1)[0]
    assert "${{" not in campaign
    assert "protocol-responses-aa.json" in targeted
    assert "protocol-responses-ab.json" in targeted


def test_targeted_writer_uses_one_harness_for_both_product_revisions() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    targeted = workflow.split("  targeted-writer-latency-10k:\n", maxsplit=1)[1]

    assert targeted.count("python harness/benchmarks/paired_writer_capacity.py") == 2
    assert "python candidate/benchmarks/paired_writer_capacity.py" not in targeted
