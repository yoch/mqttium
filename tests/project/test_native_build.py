"""Native builds must not silently degrade or leak failed staging directories."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def hook(tmp_path):
    spec = importlib.util.spec_from_file_location("native_build_hook", ROOT / "hatch_build.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.NativeCodecHook(str(tmp_path), {}, None, None, "dist", "wheel")


def test_native_editable_install_fails_before_compilation(hook, monkeypatch, tmp_path):
    monkeypatch.setenv("MQTTIUM_BUILD_CODEC", "1")
    with pytest.raises(ValueError, match="Native codec editable installs are unsupported"):
        hook.initialize("editable", {})
    assert not list(tmp_path.iterdir())


def test_pure_editable_has_no_compiler_dependencies(hook, monkeypatch, tmp_path):
    monkeypatch.setenv("MQTTIUM_BUILD_CODEC", "0")
    assert hook.dependencies() == []
    hook.initialize("editable", {})
    hook.finalize("editable", {}, None)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("error", [RuntimeError("compiler failed"), KeyboardInterrupt()])
def test_failed_native_build_cleans_only_its_owned_staging(hook, monkeypatch, tmp_path, error):
    monkeypatch.setenv("MQTTIUM_BUILD_CODEC", "1")
    other = tmp_path / "build/another-build"
    other.mkdir(parents=True)
    other.joinpath("keep").write_text("concurrent build")

    def fail(root, staging, build_data):
        staging.joinpath("partial.c").write_text("partial output")
        raise error

    monkeypatch.setattr(hook, "_build", fail)
    with pytest.raises(type(error)):
        hook.initialize("standard", {})
    assert sorted((tmp_path / "build").iterdir()) == [other]
    assert other.joinpath("keep").read_text() == "concurrent build"
