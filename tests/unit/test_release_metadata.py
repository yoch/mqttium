"""Keep user-visible release metadata aligned with the package version."""

from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _package_version() -> str:
    tree = ast.parse((ROOT / "src/mqttium/__init__.py").read_text(encoding="utf-8"))
    return next(
        node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        )
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    )


def test_release_version_matches_readme_and_changelog() -> None:
    version = _package_version()
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")

    assert f"> **Status:** beta (`{version}`)." in readme
    assert re.search(
        rf"^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}$",
        changelog,
        re.M,
    )
    assert f"[{version}]: https://github.com/yoch/mqttium/compare/" in changelog
    assert f"...v{version}" in changelog


def test_unreleased_link_starts_at_current_version() -> None:
    version = _package_version()
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert f"[Unreleased]: https://github.com/yoch/mqttium/compare/v{version}...HEAD" in changelog
