from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any


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


def test_run_performance_requalifies_before_each_major_phase(
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
        "runner-preflight-open-loop",
        "paired-open-loop-strict",
    ]

    preflight_calls = [call for call in recorder.calls if call[0].startswith("runner-preflight")]
    assert [
        Path(_argument(command, "--output")).name for _name, command, _timeout in preflight_calls
    ] == [
        "runner.json",
        "runner-network.json",
        "runner-open-loop.json",
    ]
    for _name, command, timeout in preflight_calls:
        assert "--enforce" in command
        assert _argument(command, "--wait-seconds") == "60"
        assert _argument(command, "--poll-seconds") == "5"
        assert _argument(command, "--consecutive-eligible") == "2"
        assert timeout == 90

    network = next(
        command for name, command, _timeout in recorder.calls if name == "paired-network-advisory"
    )
    open_loop = next(
        command for name, command, _timeout in recorder.calls if name == "paired-open-loop-strict"
    )
    assert Path(_argument(network, "--preflight-report")).name == "runner-network.json"
    assert Path(_argument(open_loop, "--preflight-report")).name == "runner-open-loop.json"
