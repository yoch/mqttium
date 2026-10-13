"""Build the optional complete codec through MQTTium's existing Hatch backend."""

from __future__ import annotations

import importlib.util
import os
import shutil
import tempfile
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class NativeCodecHook(BuildHookInterface):
    def dependencies(self):
        if self.target_name == "wheel" and os.environ.get("MQTTIUM_BUILD_CODEC", "0") == "1":
            return ["Cython>=3.2.4,<4", "setuptools>=80"]
        return []

    def initialize(self, version, build_data):
        self._staging = None
        enabled = os.environ.get("MQTTIUM_BUILD_CODEC", "0")
        if enabled not in {"0", "1"}:
            raise ValueError("MQTTIUM_BUILD_CODEC must be 0 or 1")
        if self.target_name != "wheel" or enabled == "0":
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
        from setuptools import Distribution, Extension

        spec = importlib.util.spec_from_file_location(
            "_codec_compile", root / "src/mqttium/codec/_compile.py"
        )
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        source = staging / "_native.pyx"
        generator.generate(root / "src/mqttium", source)
        extension = Extension("mqttium.codec._native", [str(source)])
        distribution = Distribution(
            {
                "name": "mqttium-native-codec",
                "ext_modules": cythonize(
                    [extension],
                    compiler_directives={
                        "language_level": 3,
                        "annotation_typing": False,
                        "binding": True,
                    },
                    quiet=True,
                ),
            }
        )
        command = distribution.get_command_obj("build_ext")
        command.build_lib = str(staging / "lib")
        command.build_temp = str(staging / "objects")
        distribution.run_command("build_ext")
        artifact = Path(command.get_ext_fullpath("mqttium.codec._native"))
        build_data["force_include"][str(artifact)] = "mqttium/codec/" + artifact.name
        build_data["pure_python"] = False
        build_data["infer_tag"] = True

    def finalize(self, version, build_data, artifact_path):
        if self._staging is not None:
            shutil.rmtree(self._staging)
            self._staging = None
