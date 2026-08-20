"""Keep the published documentation complete and navigable."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[2]
MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
EXTERNAL_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]*:", re.IGNORECASE)


def _active_documents() -> list[Path]:
    return [
        ROOT / "README.md",
        *sorted((ROOT / "docs").rglob("*.md")),
        ROOT / "src/mqttium/compat/README.md",
    ]


def test_relative_markdown_links_resolve() -> None:
    missing: list[str] = []
    for document in _active_documents():
        text = document.read_text(encoding="utf-8")
        for match in MARKDOWN_LINK.finditer(text):
            target = match.group(1).strip().strip("<>")
            if not target or target.startswith("#") or EXTERNAL_SCHEME.match(target):
                continue
            path_text = unquote(target.split("#", 1)[0])
            if not (document.parent / path_text).resolve().exists():
                missing.append(f"{document.relative_to(ROOT)} -> {target}")

    assert not missing, "missing relative Markdown targets:\n" + "\n".join(missing)


def test_active_documentation_has_no_obsolete_french_marker() -> None:
    offenders = [
        str(document.relative_to(ROOT))
        for document in _active_documents()
        if "🇫🇷" in document.read_text(encoding="utf-8")
    ]
    assert not offenders, f"obsolete French markers in: {offenders}"


def test_documented_examples_are_shipped() -> None:
    expected = {
        "pubsub.py",
        "durable_session.py",
        "paho_compat.py",
        "runtime_stats.py",
    }
    assert expected <= {path.name for path in (ROOT / "examples").glob("*.py")}
