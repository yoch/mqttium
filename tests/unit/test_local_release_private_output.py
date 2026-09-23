"""Release evidence is isolated from pre-created paths on shared hosts."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import importlib
import json
import os
from pathlib import Path
import stat
import subprocess
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


def _symlink(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except OSError as exc:
        if os.name == "nt":
            pytest.skip(f"symlink privilege unavailable: {exc}")
        raise


def test_concurrent_default_runs_have_distinct_private_directories(release):
    with ThreadPoolExecutor(max_workers=4) as executor:
        recorders = list(executor.map(lambda _: release.Recorder(None, "quick"), range(8)))
    paths = {recorder.output for recorder in recorders}
    assert len(paths) == 8
    for path in paths:
        assert path.is_dir()
        assert not list(path.iterdir())
        if os.name == "posix":
            assert stat.S_IMODE(path.stat().st_mode) == 0o700


@pytest.mark.parametrize("kind", ["directory", "file", "symlink", "dangling"])
def test_preexisting_output_is_rejected_without_touching_target(release, tmp_path, kind):
    output = tmp_path / "output"
    victim = tmp_path / "victim"
    if kind == "directory":
        output.mkdir()
    elif kind == "file":
        output.write_text("keep", encoding="utf-8")
    else:
        if kind == "symlink":
            victim.mkdir()
            (victim / "evidence").write_text("keep", encoding="utf-8")
        _symlink(output, victim, directory=True)
    with pytest.raises(FileExistsError):
        release.Recorder(output, "quick")
    if kind == "symlink":
        assert (victim / "evidence").read_text(encoding="utf-8") == "keep"
        assert list(victim.iterdir()) == [victim / "evidence"]
    elif kind == "dangling":
        assert not victim.exists()
    elif kind == "file":
        assert output.read_text(encoding="utf-8") == "keep"


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and mode contract")
@pytest.mark.parametrize("kind", ["writable", "foreign-owner"])
def test_untrusted_ancestor_rejected_before_creating_output(release, tmp_path, monkeypatch, kind):
    parent = tmp_path / "unsafe"
    parent.mkdir()
    if kind == "writable":
        parent.chmod(0o777)
    else:
        original = Path.stat

        def foreign_stat(path, **kwargs):
            result = original(path, **kwargs)
            if path == parent:
                values = list(result)
                values[4] = os.geteuid() + 1001
                return os.stat_result(values)
            return result

        monkeypatch.setattr(Path, "stat", foreign_stat)
    with pytest.raises(PermissionError, match="Untrusted"):
        release.Recorder(parent / "output", "quick")
    assert not (parent / "output").exists()


def test_success_and_failure_retain_logs_and_current_manifest(release, tmp_path):
    recorder = release.Recorder(tmp_path / "run", "quick")
    recorder.run("success", [sys.executable, "-c", "print('success')"])
    with pytest.raises(release.ReleaseGateFailed) as caught:
        recorder.run("failure", [sys.executable, "-c", "print('failure'); raise SystemExit(7)"])
    assert caught.value.returncode == 7
    recorder.write_manifest()
    manifest = json.loads((recorder.output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert [item["returncode"] for item in manifest["commands"]] == [0, 7]
    assert (recorder.output / "00-success.log").read_text(encoding="utf-8") == "success\n"
    assert (recorder.output / "01-failure.log").read_text(encoding="utf-8") == "failure\n"
    assert not list(recorder.output.glob(".manifest-*"))
    if os.name == "posix":
        assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in recorder.output.iterdir())


@pytest.mark.parametrize("destination", ["00-command.log", "manifest.json"])
def test_recording_never_follows_a_symlink(release, tmp_path, destination):
    recorder = release.Recorder(tmp_path / "run", "quick")
    victim = tmp_path / "victim"
    victim.write_text("untouched", encoding="utf-8")
    _symlink(recorder.output / destination, victim)
    marker = tmp_path / "executed"
    command = [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"]
    with pytest.raises(FileExistsError):
        if destination.endswith(".log"):
            recorder.run("command", command)
        else:
            recorder.write_manifest()
    assert victim.read_text(encoding="utf-8") == "untouched"
    assert not marker.exists()
    assert not list(recorder.output.glob(".manifest-*"))


def test_failed_manifest_replace_preserves_previous_evidence(release, tmp_path, monkeypatch):
    recorder = release.Recorder(tmp_path / "run", "quick")
    recorder.write_manifest()
    path = recorder.output / "manifest.json"
    previous = path.read_bytes()

    def fail_replace(*args):
        raise OSError("cannot replace manifest")

    monkeypatch.setattr(release.os, "replace", fail_replace)
    with pytest.raises(OSError, match="cannot replace"):
        recorder.write_manifest()
    assert path.read_bytes() == previous
    assert not list(recorder.output.glob(".manifest-*"))


def test_cli_reports_default_output_and_keeps_evidence(release, monkeypatch, capsys):
    arguments = SimpleNamespace(output_dir=None, profile="quick", port=11883)
    monkeypatch.setattr(release, "parse_args", lambda: arguments)
    monkeypatch.setattr(release, "managed_mosquitto", lambda *args: nullcontext())

    def quality(recorder, port):
        recorder.run("quality", [sys.executable, "-c", "print('qualified')"])

    monkeypatch.setattr(release, "run_quality", quality)
    monkeypatch.setattr(release, "run_robustness", lambda *args, **kwargs: None)
    assert release.main() == 0
    line = capsys.readouterr().out.splitlines()[0]
    assert line.startswith("release output: ")
    output = Path(line.removeprefix("release output: "))
    assert output.is_dir()
    assert json.loads((output / "manifest.json").read_text())["status"] == "passed"


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission-bit contract")
def test_private_creation_keeps_the_process_umask(tmp_path):
    # Change umask only in a child process; the gate must not change it itself.
    script = f"""
import os, stat, subprocess, sys, tempfile
from pathlib import Path
sys.path.insert(0, {str(Path(__file__).resolve().parents[2] / "benchmarks")!r})
import local_release as release
release._source_fingerprint = lambda: "fingerprint"
tempfile.tempdir = {str(tmp_path)!r}
os.umask(0o022)
explicit = release.Recorder(Path({str(tmp_path / "explicit")!r}), "quick")
explicit.run("child", [sys.executable, "-c", "print(1)"])
explicit.write_manifest()
default = release.Recorder(None, "quick")
modes = [stat.S_IMODE(p.stat().st_mode) for p in (
    explicit.output, default.output,
    explicit.output / "00-child.log", explicit.output / "manifest.json")]
assert modes == [0o700, 0o700, 0o600, 0o600], [oct(m) for m in modes]
assert os.umask(0o022) == 0o022
"""
    completed = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=30, check=False
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.skipif(os.name != "posix", reason="POSIX sticky-directory contract")
def test_sticky_shared_ancestor_is_within_the_threat_model(release, tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    shared.chmod(0o1777)
    recorder = release.Recorder(shared / "run", "quick")
    assert recorder.output == (shared / "run").resolve()


def test_concurrent_explicit_output_has_exactly_one_owner(release, tmp_path):
    output = tmp_path / "contended"

    def attempt(_):
        try:
            return release.Recorder(output, "quick")
        except FileExistsError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = list(executor.map(attempt, range(8)))
    owners = [item for item in outcomes if not isinstance(item, Exception)]
    assert len(owners) == 1
    assert all(isinstance(item, FileExistsError) for item in outcomes if item not in owners)


def test_missing_parent_is_refused_without_being_created(release, tmp_path):
    with pytest.raises(FileNotFoundError):
        release.Recorder(tmp_path / "absent" / "run", "quick")
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("kind", ["existing", "missing-parent"])
def test_cli_refuses_unusable_output_without_a_traceback(
    release, tmp_path, monkeypatch, capsys, kind
):
    output = tmp_path / "existing" if kind == "existing" else tmp_path / "absent" / "run"
    if kind == "existing":
        output.mkdir()
    arguments = SimpleNamespace(output_dir=output, profile="quick", port=11883)
    monkeypatch.setattr(release, "parse_args", lambda: arguments)
    assert release.main() == 2
    captured = capsys.readouterr()
    assert captured.err.startswith("local release output refused: ")
    assert "Traceback" not in captured.err and not captured.out
    assert not (tmp_path / "absent").exists()
