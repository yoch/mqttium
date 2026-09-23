"""Local release qualification must not weaken the repository quality gates."""

import contextlib
import importlib
from pathlib import Path
import sys
import tomllib

import pytest

ROOT = Path(__file__).resolve().parents[2]
TARGETS = ["src", "tests", "benchmarks", "tools"]
# Options that would override the canonical pytest/coverage configuration.
OVERRIDES = ("--cov-fail-under", "--cov-branch", "--no-cov", "-o", "--override-ini", "-W", "-p")


@pytest.fixture
def release(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "benchmarks"))
    module = importlib.import_module("local_release")
    monkeypatch.setattr(module, "_assert_tracked_sources_clean", lambda: None)
    return module


class _CommandRecorder:
    def __init__(self, output):
        self.output = output
        self.commands = {}

    def run(self, name, command, **kwargs):
        self.commands[name] = (command, kwargs)


def test_quality_commands_follow_the_maintained_gates(release, tmp_path):
    recorder = _CommandRecorder(tmp_path)
    release.run_quality(recorder, 11883)
    tool = [sys.executable, "-m"]

    fmt, fmt_options = recorder.commands["ruff-format"]
    lint, lint_options = recorder.commands["ruff"]
    assert fmt == [*tool, "ruff", "format", "--check", *TARGETS]
    assert lint == [*tool, "ruff", "check", *TARGETS]

    coverage, coverage_options = recorder.commands["unit-coverage"]
    assert coverage[:3] == [*tool, "pytest"]
    assert {"tests/unit", "tests/project", "--cov=mqttium"} <= set(coverage)
    # Threshold, branches, warnings and strictness come from pyproject.toml.
    assert not [arg for arg in coverage if arg.split("=", 1)[0] in OVERRIDES], coverage

    docs, docs_options = recorder.commands["docs-strict"]
    assert docs == [*tool, "mkdocs", "build", "--strict", "--site-dir", str(tmp_path / "site")]

    integration, options = recorder.commands["integration-required"]
    assert "tests/integration" in integration
    assert options["env"]["MQTTIUM_REQUIRE_BROKER"] == "1"

    # Every quality command runs from the repository root, where that
    # configuration is discovered.
    for kwargs in (fmt_options, lint_options, coverage_options, docs_options, options):
        assert "cwd" not in kwargs
    assert release.ROOT == ROOT


def test_the_canonical_configuration_supplies_the_inherited_gates():
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert config["tool"]["coverage"]["report"]["fail_under"] >= 89
    assert config["tool"]["coverage"]["run"]["branch"] is True
    addopts = config["tool"]["pytest"]["ini_options"]["addopts"]
    assert {"--strict-config", "--strict-markers"} <= set(addopts)
    assert addopts[addopts.index("-W") + 1] == "error"


def test_manifest_records_the_mkdocs_version(release, tmp_path, monkeypatch):
    monkeypatch.setattr(release, "_package_installed", lambda package: True)
    monkeypatch.setattr(release.importlib.metadata, "version", lambda package: f"{package}-v")
    recorder = release.Recorder(tmp_path / "out", "quick")
    assert recorder.package_versions["mkdocs"] == "mkdocs-v"


@pytest.mark.parametrize(
    ("profile", "runs_quality"), [("quick", True), ("rc", True), ("performance", False)]
)
def test_quick_and_rc_share_the_quality_phase(
    release, tmp_path, monkeypatch, profile, runs_quality
):
    calls = []

    class _Recorder(_CommandRecorder):
        def __init__(self, output, profile):
            super().__init__(output)

        def write_manifest(self):
            pass

    arguments = [profile, "--output-dir", str(tmp_path)]
    if profile != "quick":
        arguments += ["--cpu", "0"]
    monkeypatch.setattr(sys, "argv", ["local_release.py", *arguments])
    monkeypatch.setattr(release, "_capture", lambda *args, **kwargs: "candidate")
    monkeypatch.setattr(release, "Recorder", _Recorder)
    monkeypatch.setattr(release, "managed_mosquitto", lambda *args: contextlib.nullcontext())
    monkeypatch.setattr(release, "baseline_root", lambda *args: contextlib.nullcontext(None))
    monkeypatch.setattr(release, "run_quality", lambda *args: calls.append("quality"))
    for phase in ("run_performance", "run_robustness", "run_package"):
        monkeypatch.setattr(
            release, phase, lambda *args, _phase=phase, **kwargs: calls.append(_phase)
        )

    assert release.main() == 0
    assert ("quality" in calls) is runs_quality
    assert calls.count("quality") == int(runs_quality)
