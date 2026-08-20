#!/usr/bin/env python3
"""Validate release source metadata and emit the package version."""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path


def _version() -> str:
    tree = ast.parse(Path("src/mqttium/__init__.py").read_text(encoding="utf-8"))
    for node in tree.body:
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
    parser.add_argument("--tag", default="")
    parser.add_argument("--prerelease", choices=("true", "false"))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    version = _version()
    if args.tag and args.tag != f"v{version}":
        raise SystemExit(f"release tag {args.tag!r} does not match {f'v{version}'!r}")
    is_prerelease = re.search(r"(?:a|b|rc)\d+|\.dev\d+", version) is not None
    if args.prerelease is not None and is_prerelease != (args.prerelease == "true"):
        raise SystemExit("GitHub pre-release flag does not match the package version")
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            print(f"version={version}", file=output)
    else:
        print(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
