"""Local release qualification must not weaken the repository quality gates."""

import importlib
from pathlib import Path


def test_quality_includes_project_coverage_and_strict_docs(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root / "benchmarks"))
    release = importlib.import_module("local_release")
    monkeypatch.setattr(release, "_assert_tracked_sources_clean", lambda: None)

    class Recorder:
        output = tmp_path

        def __init__(self):
            self.commands = {}

        def run(self, name, command, **kwargs):
            self.commands[name] = (command, kwargs)

    recorder = Recorder()
    release.run_quality(recorder, 11883)
    coverage, _ = recorder.commands["unit-coverage"]
    assert {"tests/unit", "tests/project", "--cov=mqttium"} <= set(coverage)
    # The single threshold source is pyproject.toml, shared with CI.
    assert not any(arg.startswith("--cov-fail-under") for arg in coverage)
    docs, _ = recorder.commands["docs-strict"]
    assert docs[-5:] == ["mkdocs", "build", "--strict", "--site-dir", str(tmp_path / "site")]
    integration, options = recorder.commands["integration-required"]
    assert "tests/integration" in integration
    assert options["env"]["MQTTIUM_REQUIRE_BROKER"] == "1"
