#!/usr/bin/env python3
"""Validate MQTTium wheel and sdist metadata and required contents."""

from __future__ import annotations

import argparse
import ast
import email
import tarfile
from pathlib import Path
from zipfile import ZipFile


# Development material that must never reach a distribution. A silent
# reappearance is the failure this list exists to catch.
_FORBIDDEN_SEGMENTS = (
    "tests",
    "benchmarks",
    "docs",
    "examples",
    "tools",
    "compat",
    "__pycache__",
    ".pytest_cache",
    ".github",
    ".venv",
)

# Everything the sdist is allowed to carry. PKG-INFO and .gitignore are added
# by hatchling itself and cannot be excluded from the target.
_SDIST_ALLOWED_FILES = frozenset(
    {
        "README.md",
        "LICENSE",
        "NOTICE",
        "pyproject.toml",
        "PKG-INFO",
        ".gitignore",
    }
)


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


def _check_no_forbidden(kind: str, names: list[str], *, strip_root: bool) -> list[str]:
    failures: list[str] = []
    for name in names:
        parts = name.split("/")
        if strip_root:
            parts = parts[1:]
        if any(part in _FORBIDDEN_SEGMENTS for part in parts):
            failures.append(f"{kind} contains development material: {name}")
        if name.endswith(".pyc"):
            failures.append(f"{kind} contains a compiled artefact: {name}")
    return failures


def _check_wheel(path: Path, expected_version: str) -> list[str]:
    failures: list[str] = []
    with ZipFile(path) as archive:
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
        dist_info = f"mqttium-{expected_version}.dist-info/"
        for name in names:
            if not name.startswith(("mqttium/", dist_info)):
                failures.append(f"wheel contains an unexpected top-level entry: {name}")
        # dist-info carries the licence files, so only the package tree is swept.
        package_names = [name for name in names if name.startswith("mqttium/")]
        failures.extend(_check_no_forbidden("wheel", package_names, strip_root=False))
    return failures


def _check_sdist(path: Path, expected_version: str) -> list[str]:
    failures: list[str] = []
    root = f"mqttium-{expected_version}"
    with tarfile.open(path, "r:gz") as archive:
        names = archive.getnames()
        for filename in ("LICENSE", "NOTICE", "README.md", "pyproject.toml"):
            if not any(name == f"{root}/{filename}" for name in names):
                failures.append(f"sdist is missing {filename}")
        if not any(name.startswith(f"{root}/src/mqttium/") for name in names):
            failures.append("sdist is missing the src/mqttium package tree")
        for name in names:
            if name == root:
                continue
            if not name.startswith(f"{root}/"):
                failures.append(f"sdist entry escapes its root directory: {name}")
                continue
            relative = name[len(root) + 1 :]
            if relative == "src" or relative.startswith("src/mqttium"):
                continue
            if relative in _SDIST_ALLOWED_FILES:
                continue
            failures.append(f"sdist contains an unexpected entry: {relative}")
        failures.extend(_check_no_forbidden("sdist", names, strip_root=True))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", default=Path("dist"), type=Path)
    args = parser.parse_args()
    expected_version = _source_version()
    wheel = next(args.directory.glob(f"mqttium-{expected_version}-*.whl"))
    sdist = next(args.directory.glob(f"mqttium-{expected_version}.tar.gz"))
    failures = _check_wheel(wheel, expected_version) + _check_sdist(sdist, expected_version)
    if failures:
        for failure in failures:
            print(f"error: {failure}")
        return 1
    print(f"validated mqttium {expected_version} wheel and sdist")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
