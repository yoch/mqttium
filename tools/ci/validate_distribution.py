#!/usr/bin/env python3
"""Validate MQTTium wheel and sdist metadata and required contents."""

from __future__ import annotations

import argparse
import ast
import email
import tarfile
from pathlib import Path
from zipfile import ZipFile


def _source_version() -> str:
    source = ast.parse(Path("src/mqttium/__init__.py").read_text(encoding="utf-8"))
    for node in source.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__version__"
                for target in node.targets
            )
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            return node.value.value
    raise RuntimeError("src/mqttium/__init__.py does not define a literal __version__")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", default=Path("dist"), type=Path)
    args = parser.parse_args()
    expected_version = _source_version()
    wheel = next(args.directory.glob(f"mqttium-{expected_version}-*.whl"))
    sdist = next(args.directory.glob(f"mqttium-{expected_version}.tar.gz"))
    with ZipFile(wheel) as archive:
        names = archive.namelist()
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = email.message_from_bytes(archive.read(metadata_name))
        assert metadata["Name"] == "mqttium"
        assert metadata["Version"] == expected_version
        assert metadata["Requires-Python"] == ">=3.11"
        assert metadata["License-Expression"] == "Apache-2.0"
        assert "mqttium/py.typed" in names
        assert any(name.endswith(".dist-info/licenses/LICENSE") for name in names)
        assert any(name.endswith(".dist-info/licenses/NOTICE") for name in names)
    with tarfile.open(sdist, "r:gz") as archive:
        names = archive.getnames()
        for filename in (
            "LICENSE",
            "NOTICE",
            "README.md",
            "CHANGELOG.md",
            "PROVENANCE.md",
            "SECURITY.md",
        ):
            assert any(name.endswith("/" + filename) for name in names), filename
    print(f"validated mqttium {expected_version} wheel and sdist")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
