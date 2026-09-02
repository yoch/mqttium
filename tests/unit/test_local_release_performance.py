from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[2]


class _Recorder:
    def __init__(self, output: Path) -> None:
        self.output = output
        self.calls: list[tuple[str, list[str], float | None]] = []

    def run(
        self, name: str, command: list[str], *, timeout: float | None = None, **_kwargs: Any
    ) -> None:
        self.calls.append((name, command, timeout))


def _local_release(monkeypatch: Any) -> Any:
    monkeypatch.syspath_prepend(str(ROOT / "benchmarks"))
    return importlib.import_module("local_release")


def _argument(command: list[str], option: str) -> str:
    return command[command.index(option) + 1]


def test_run_performance_requalifies_external_gates_and_uses_open_loop_gate(
    tmp_path: Path, monkeypatch: Any
) -> None:
    local_release = _local_release(monkeypatch)
    recorder = _Recorder(tmp_path)

    local_release.run_performance(
        recorder,
        base=tmp_path / "base",
        port=11883,
        network_repeat=8,
        cpu=2,
    )

    assert [name for name, _command, _timeout in recorder.calls] == [
        "runner-preflight",
        "paired-micro",
        "runner-preflight-network",
        "paired-network-advisory",
        "open-loop-release-gate-strict",
    ]

    preflight_calls = [call for call in recorder.calls if call[0].startswith("runner-preflight")]
    assert [
        Path(_argument(command, "--output")).name for _name, command, _timeout in preflight_calls
    ] == [
        "runner.json",
        "runner-network.json",
    ]
    for name, command, timeout in preflight_calls:
        assert "--enforce" in command
        assert _argument(command, "--wait-seconds") == "60"
        assert _argument(command, "--poll-seconds") == "5"
        assert _argument(command, "--consecutive-eligible") == "2"
        assert timeout == 90
        assert ("--ignore-historical-load" in command) is (name == "runner-preflight-network")

    network = next(
        command for name, command, _timeout in recorder.calls if name == "paired-network-advisory"
    )
    open_loop = next(
        command
        for name, command, _timeout in recorder.calls
        if name == "open-loop-release-gate-strict"
    )
    assert Path(_argument(network, "--preflight-report")).name == "runner-network.json"
    assert Path(open_loop[1]).name == "open_loop_release_gate.py"
    assert "--preflight-report" not in open_loop
    assert _argument(open_loop, "--engine") == "benchmarks/paired_open_loop.py"
    assert _argument(open_loop, "--runner-probe") == "benchmarks/runner_probe.py"
    assert _argument(open_loop, "--policy") == "strict"
    assert _argument(open_loop, "--port") == "11883"
    assert _argument(open_loop, "--cpu") == "2"
    assert Path(_argument(open_loop, "--output")).name == "paired-open-loop.json"


def test_parse_args_requires_cpu_for_performance_profiles(monkeypatch: Any) -> None:
    local_release = _local_release(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["local_release.py", "performance"])

    with pytest.raises(SystemExit) as exc_info:
        local_release.parse_args()

    assert exc_info.value.code == 2


def test_failed_gate_retains_log_and_raises_a_release_failure(
    tmp_path: Path, monkeypatch: Any
) -> None:
    local_release = _local_release(monkeypatch)
    recorder = local_release.Recorder(tmp_path, "performance")

    with pytest.raises(local_release.ReleaseGateFailed) as exc_info:
        recorder.run(
            "example-gate",
            [sys.executable, "-c", "print('gate reason'); raise SystemExit(2)"],
        )

    failure = exc_info.value
    assert failure.name == "example-gate"
    assert failure.returncode == 2
    assert failure.log == tmp_path / "00-example-gate.log"
    assert failure.manifest == tmp_path / "manifest.json"
    assert failure.log.read_text(encoding="utf-8") == "gate reason\n"
