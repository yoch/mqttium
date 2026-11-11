"""The guarantee register and the threat model name evidence that exists.

``docs/guarantees.md`` tells an application what it may rely on, and
``docs/threat-model.md`` what the library defends against. An entry is only
worth its tests: this keeps a claim from outliving them, and keeps an unbacked
claim from being presented as checked.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REGISTERS = {"G": ROOT / "docs" / "guarantees.md", "T": ROOT / "docs" / "threat-model.md"}

_STATUS = re.compile(r"^\*\*Status:\*\* (Checked|Partial|Open)\.$", re.MULTILINE)
_EVIDENCE = re.compile(r"^- `((?:tests|formal)/[^`]+)`$", re.MULTILINE)
_ANY_PATH = re.compile(r"`((?:tests|formal|src)/[^`\s]+\.(?:py|tla))`")

pytestmark = pytest.mark.parametrize("prefix", sorted(REGISTERS))


def _entries(prefix: str) -> dict[str, tuple[str, str, list[str]]]:
    text = REGISTERS[prefix].read_text(encoding="utf-8")
    headings = list(re.finditer(rf"^### ({prefix}\d+) — (.+)$", text, re.MULTILINE))
    entries = {}
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
        body = text[heading.end() : end]
        statuses = _STATUS.findall(body)
        assert len(statuses) == 1, f"{heading.group(1)} needs exactly one status line"
        entries[heading.group(1)] = (heading.group(2), statuses[0], _EVIDENCE.findall(body))
    return entries


def test_entries_are_numbered_without_gaps(prefix: str) -> None:
    identifiers = list(_entries(prefix))
    assert identifiers, "the register is empty"
    assert identifiers == [f"{prefix}{number}" for number in range(1, len(identifiers) + 1)]


def test_every_cited_path_exists(prefix: str) -> None:
    missing = [
        path
        for path in _ANY_PATH.findall(REGISTERS[prefix].read_text(encoding="utf-8"))
        if not (ROOT / path).is_file()
    ]
    assert missing == []


def test_claimed_entries_have_executable_evidence(prefix: str) -> None:
    """A model alone is not evidence for the Python code: a test must back the claim."""
    unbacked = [
        identifier
        for identifier, (_title, status, evidence) in _entries(prefix).items()
        if status != "Open" and not any(path.startswith("tests/") for path in evidence)
    ]
    assert unbacked == []


def test_open_entries_claim_no_evidence(prefix: str) -> None:
    claimed = [
        identifier
        for identifier, (_title, status, evidence) in _entries(prefix).items()
        if status == "Open" and evidence
    ]
    assert claimed == [], "an Open entry lists evidence: either check it or reword it"
