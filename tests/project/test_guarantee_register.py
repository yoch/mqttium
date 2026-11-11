"""The guarantee register names evidence that exists.

``docs/guarantees.md`` tells an application what it may rely on. An entry is
only worth its tests: this keeps a guarantee from outliving them, and keeps an
unbacked claim from being presented as checked.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REGISTER = ROOT / "docs" / "guarantees.md"

_HEADING = re.compile(r"^### (G\d+) — (.+)$", re.MULTILINE)
_STATUS = re.compile(r"^\*\*Status:\*\* (Checked|Partial|Open)\.$", re.MULTILINE)
_EVIDENCE = re.compile(r"^- `((?:tests|formal)/[^`]+)`$", re.MULTILINE)
_ANY_PATH = re.compile(r"`((?:tests|formal|src)/[^`\s]+\.(?:py|tla))`")


def _entries() -> dict[str, tuple[str, str, list[str]]]:
    text = REGISTER.read_text(encoding="utf-8")
    headings = list(_HEADING.finditer(text))
    entries = {}
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        body = text[heading.end() : end]
        statuses = _STATUS.findall(body)
        assert len(statuses) == 1, f"{heading.group(1)} needs exactly one status line"
        entries[heading.group(1)] = (heading.group(2), statuses[0], _EVIDENCE.findall(body))
    return entries


def test_guarantees_are_numbered_without_gaps() -> None:
    identifiers = list(_entries())
    assert identifiers, "the register is empty"
    assert identifiers == [f"G{number}" for number in range(1, len(identifiers) + 1)]


def test_every_cited_path_exists() -> None:
    missing = [
        path
        for path in _ANY_PATH.findall(REGISTER.read_text(encoding="utf-8"))
        if not (ROOT / path).is_file()
    ]
    assert missing == []


def test_claimed_guarantees_have_executable_evidence() -> None:
    """A model alone is not evidence for the Python code: a test must back the claim."""
    unbacked = [
        identifier
        for identifier, (_title, status, evidence) in _entries().items()
        if status != "Open" and not any(path.startswith("tests/") for path in evidence)
    ]
    assert unbacked == []


def test_open_guarantees_claim_no_evidence() -> None:
    claimed = [
        identifier
        for identifier, (_title, status, evidence) in _entries().items()
        if status == "Open" and evidence
    ]
    assert claimed == [], "an Open guarantee lists evidence: either check it or reword it"
