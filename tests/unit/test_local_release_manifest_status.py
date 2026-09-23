"""A release manifest may only pass once its whole profile has succeeded."""

from contextlib import nullcontext
import importlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture
def release(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "benchmarks"))
    module = importlib.import_module("local_release")
    monkeypatch.setattr(module, "_source_fingerprint", lambda: "test-fingerprint")
    monkeypatch.setattr(module, "_capture", lambda *args, **kwargs: "test-revision")
    monkeypatch.setattr(module.subprocess, "check_output", lambda *args, **kwargs: "")
    monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
    return module


def _manifest(recorder):
    return json.loads((recorder.output / "manifest.json").read_text(encoding="utf-8"))


_FAILURES = {
    "timeout": (
        [sys.executable, "-c", "import time; print('partial', flush=True); time.sleep(30)"],
        {"timeout": 0.5},
        "timed out after 0.5s",
    ),
    "missing-executable": (["mqttium-no-such-executable"], {}, "could not start"),
}


@pytest.mark.parametrize("failure", sorted(_FAILURES))
def test_an_unfinished_command_is_recorded_as_a_failure(release, tmp_path, failure):
    command, options, error = _FAILURES[failure]
    recorder = release.Recorder(tmp_path / "run", "rc")
    recorder.run("successful-stage", [sys.executable, "-c", "print('ok')"])
    recorder.profile_completed = True  # even a completion claim cannot hide it
    with pytest.raises(release.ReleaseGateFailed) as caught:
        recorder.run("failing-stage", command, **options)
    assert caught.value.returncode is None
    assert "failing-stage did not complete: " + error in str(caught.value)

    manifest = _manifest(recorder)
    assert manifest["status"] == "failed"
    failed = manifest["commands"][-1]
    assert (failed["name"], failed["returncode"]) == ("failing-stage", None)
    assert failed["error"].startswith(error)
    log = (recorder.output / failed["log"]).read_text(encoding="utf-8")
    if failure == "timeout":
        assert "partial" in log


def test_successful_commands_are_incomplete_until_the_profile_finishes(release, tmp_path):
    recorder = release.Recorder(tmp_path / "run", "quick")
    recorder.write_manifest()
    assert _manifest(recorder)["status"] == "incomplete"
    recorder.run("stage", [sys.executable, "-c", "print('ok')"])
    assert _manifest(recorder)["status"] == "incomplete"
    recorder.profile_completed = True
    recorder.write_manifest()
    assert _manifest(recorder)["status"] == "passed"


def _cli(release, monkeypatch, quality):
    arguments = SimpleNamespace(output_dir=None, profile="quick", port=11883)
    monkeypatch.setattr(release, "parse_args", lambda: arguments)
    monkeypatch.setattr(release, "managed_mosquitto", lambda *args: nullcontext())
    monkeypatch.setattr(release, "run_quality", quality)
    monkeypatch.setattr(release, "run_robustness", lambda *args, **kwargs: None)


def _output(capsys):
    captured = capsys.readouterr()
    line = captured.out.splitlines()[0]
    return Path(line.removeprefix("release output: ")), captured.err


def test_cli_timeout_fails_with_a_failed_manifest(release, monkeypatch, capsys):
    def quality(recorder, port):
        recorder.run("stage", [sys.executable, "-c", "print('ok')"])
        recorder.run("hung", [sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.5)

    _cli(release, monkeypatch, quality)
    assert release.main() == 1
    output, err = _output(capsys)
    assert "hung did not complete: timed out" in err
    assert json.loads((output / "manifest.json").read_text())["status"] == "failed"


@pytest.mark.parametrize("interruption", [RuntimeError("broker vanished"), KeyboardInterrupt()])
def test_cli_interruption_outside_a_command_leaves_the_run_incomplete(
    release, monkeypatch, capsys, interruption
):
    def quality(recorder, port):
        recorder.run("stage", [sys.executable, "-c", "print('ok')"])
        raise interruption

    _cli(release, monkeypatch, quality)
    with pytest.raises(type(interruption)):
        release.main()
    output, _ = _output(capsys)
    assert json.loads((output / "manifest.json").read_text())["status"] == "incomplete"
