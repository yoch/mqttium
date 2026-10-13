"""Backend selection, complete coverage and the native codec's Python oracle."""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

import mqttium
from mqttium.codec._backend import BACKEND
from mqttium.codec._compile import MODULES, generate

ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = Path(mqttium.__file__).resolve().parent.parent


def _run(mode, source, root=PACKAGE_ROOT):
    return subprocess.run(
        [sys.executable, "-c", source],
        env={**os.environ, "PYTHONPATH": str(root), "MQTTIUM_CODEC_BACKEND": mode},
        cwd=root,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_python_backend_can_be_forced_with_native_wheel():
    result = _run(
        "python",
        "from mqttium.codec._backend import BACKEND; from mqttium.codec.vbi import decode_vbi; print(BACKEND, decode_vbi.__module__)",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "python mqttium.codec.vbi"


def test_invalid_backend_selection_is_rejected():
    result = _run("typo", "import mqttium.codec")
    assert result.returncode != 0
    assert "MQTTIUM_CODEC_BACKEND must be" in result.stderr


@pytest.mark.parametrize("mode", ["auto", "native"])
@pytest.mark.parametrize(
    "failure",
    [
        "raise ImportError('broken ABI')",
        "raise ModuleNotFoundError('broken dependency', name='codec_dependency')",
    ],
)
def test_broken_native_backend_is_never_silently_hidden(tmp_path, mode, failure):
    package = tmp_path / "mqttium" / "codec"
    package.mkdir(parents=True)
    package.parent.joinpath("__init__.py").write_text("")
    package.joinpath("__init__.py").write_text("")
    package.joinpath("_backend.py").write_text(
        (PACKAGE_ROOT / "mqttium/codec/_backend.py").read_text()
    )
    package.joinpath("_native.py").write_text(failure)
    result = _run(mode, "import mqttium.codec._backend", tmp_path)
    assert result.returncode != 0
    assert "broken" in result.stderr


def test_missing_native_backend_only_falls_back_in_auto_mode(tmp_path):
    package = tmp_path / "mqttium" / "codec"
    package.mkdir(parents=True)
    package.parent.joinpath("__init__.py").write_text("")
    package.joinpath("__init__.py").write_text("")
    package.joinpath("_backend.py").write_text(
        (PACKAGE_ROOT / "mqttium/codec/_backend.py").read_text()
    )
    source = "from mqttium.codec._backend import BACKEND; print(BACKEND)"
    assert _run("auto", source, tmp_path).stdout.strip() == "python"
    result = _run("native", source, tmp_path)
    assert result.returncode != 0
    assert "mqttium.codec._native" in result.stderr


def test_generator_is_deterministic_and_keeps_source_unchanged(tmp_path):
    package = ROOT / "src/mqttium"
    sources = {m: (package / (m.replace(".", "/") + ".py")).read_bytes() for m in MODULES}
    first, second = tmp_path / "a.pyx", tmp_path / "b.pyx"
    generate(package, first)
    generate(package, second)
    assert first.read_bytes() == second.read_bytes()
    for name, content in sources.items():
        assert (package / (name.replace(".", "/") + ".py")).read_bytes() == content
    text = first.read_text()
    assert "from mqttium.codec._backend" not in text
    assert "class _DecoderState" in text
    assert "_assemble_publish" in text


@pytest.mark.skipif(BACKEND != "native", reason="native wheel job checks compiled implementation")
def test_all_wire_modules_use_the_same_extension():
    native = importlib.import_module("mqttium.codec._native")
    assert len(native.SOURCE_DIGEST) == 64
    assert set(native.EXPORTS) == {"mqttium." + m for m in MODULES if m != "transport.writes"}
    for name, exports in native.EXPORTS.items():
        module = importlib.import_module(name)
        for symbol, value in exports.items():
            assert getattr(module, symbol) is value, (name, symbol)


@pytest.mark.skipif(BACKEND != "native", reason="native wheel job runs both codec oracles")
def test_native_matches_python_on_wire_corpus():
    script = ROOT / "tests/fuzz/codec_differential.py"
    source = f"import runpy; runpy.run_path({str(script)!r}, run_name='__main__')"
    results = []
    for mode in ("python", "native"):
        result = _run(mode, source)
        assert result.returncode == 0, result.stderr
        results.append(json.loads(result.stdout))
    assert len(results[0]) == len(results[1])
    for index, (reference, native) in enumerate(zip(*results, strict=True)):
        assert reference == native, (index, reference, native)
