"""Build the optional complete codec through MQTTium's existing Hatch backend."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

DIRECTIVES = {"language_level": 3, "annotation_typing": False, "binding": True}


def _flag(name):
    value = os.environ.get(name, "0")
    if value not in {"0", "1"}:
        raise ValueError(f"{name} must be 0 or 1")
    return value == "1"


class NativeCodecHook(BuildHookInterface):
    def dependencies(self):
        if self.target_name == "wheel" and os.environ.get("MQTTIUM_BUILD_CODEC", "0") == "1":
            return ["Cython>=3.2.4,<4", "setuptools>=80"]
        return []

    def initialize(self, version, build_data):
        self._staging = None
        enabled = _flag("MQTTIUM_BUILD_CODEC")
        self._pgo = _flag("MQTTIUM_CODEC_PGO")
        if self._pgo and not enabled:
            raise ValueError("MQTTIUM_CODEC_PGO=1 requires MQTTIUM_BUILD_CODEC=1")
        if self.target_name != "wheel" or not enabled:
            return

        if version == "editable":
            raise ValueError(
                "Native codec editable installs are unsupported; use "
                "MQTTIUM_BUILD_CODEC=1 pip install . or build a wheel. "
                "Use MQTTIUM_BUILD_CODEC=0 for a pure-Python editable install."
            )

        root = Path(self.root)
        (root / "build").mkdir(exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix="native-codec-", dir=root / "build"))
        self._staging = staging
        try:
            self._build(root, staging, build_data)
        except BaseException:
            self.finalize(version, build_data, None)
            raise

    def _build(self, root, staging, build_data):
        from Cython.Build import cythonize
        from setuptools import Extension

        spec = importlib.util.spec_from_file_location(
            "_codec_compile", root / "src/mqttium/codec/_compile.py"
        )
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        source = staging / "_native.pyx"
        generator.generate(root / "src/mqttium", source)
        # Generate C once: every compilation below must see identical source.
        cythonize(
            [Extension("mqttium.codec._native", [str(source)])],
            compiler_directives=DIRECTIVES,
            quiet=True,
        )
        c_source = source.with_suffix(".c")
        if self._pgo:
            artifact = self._profile_guided(root, staging, c_source)
        else:
            artifact = _compile(c_source, staging, [])
        build_data["force_include"][str(artifact)] = "mqttium/codec/" + artifact.name
        build_data["pure_python"] = False
        build_data["infer_tag"] = True

    def _profile_guided(self, root, staging, c_source):
        """Train the instrumented extension on a broker-free client workload."""
        compiler = sysconfig.get_config_var("CC") or "cc"
        banner = subprocess.run(
            [*compiler.split(), "--version"], capture_output=True, text=True, check=True
        ).stdout
        if "Free Software Foundation" not in banner:
            raise RuntimeError("MQTTIUM_CODEC_PGO=1 currently requires GCC")
        profile = staging / "profile"
        instrumented = _compile(c_source, staging, [f"-fprofile-generate={profile}"])
        train = staging / "train"
        shutil.copytree(
            root / "src", train, ignore=shutil.ignore_patterns("__pycache__", "*.so", "*.pyd")
        )
        shutil.copy2(instrumented, train / "mqttium/codec" / instrumented.name)
        subprocess.run(
            [sys.executable, "-m", "mqttium.codec._train"],
            check=True,
            cwd=staging,
            env={**os.environ, "PYTHONPATH": str(train), "MQTTIUM_CODEC_BACKEND": "native"},
        )
        if not list(profile.rglob("*.gcda")):
            raise RuntimeError("Profile-guided training wrote no profile")
        # Untrained functions keep normal optimisation rather than size.
        return _compile(
            c_source,
            staging,
            [f"-fprofile-use={profile}", "-fprofile-partial-training", "-Wno-missing-profile"],
        )

    def finalize(self, version, build_data, artifact_path):
        if self._staging is not None:
            shutil.rmtree(self._staging)
            self._staging = None


def _compile(c_source, staging, flags):
    """Compile with fixed object paths, which name GCC's profile files."""
    from setuptools import Distribution, Extension

    extension = Extension(
        "mqttium.codec._native",
        [str(c_source)],
        extra_compile_args=list(flags),
        extra_link_args=list(flags),
    )
    distribution = Distribution({"name": "mqttium-native-codec", "ext_modules": [extension]})
    command = distribution.get_command_obj("build_ext")
    command.build_lib = str(staging / "lib")
    command.build_temp = str(staging / "objects")
    command.force = True
    distribution.run_command("build_ext")
    return Path(command.get_ext_fullpath("mqttium.codec._native"))
