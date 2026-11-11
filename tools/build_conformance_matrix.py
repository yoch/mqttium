"""Build the statement-to-test conformance matrix from the tree.

Every numbered statement of the two vendored MQTT specifications is keyed by
``(version, label)``: 119 labels exist in both documents with different text,
so a label alone names nothing. For each statement the matrix records

* its *applicability* -- whether it binds a client, a server or both -- from
  ``docs/spec/applicability.json``;
* its *evidence*:

  - ``verified``: a test function's docstring opens with ``[label]`` followed
    by a quotation that matches this version's text;
  - ``cited``: the label appears in a test or in ``src/`` without such a
    quotation (a paraphrase or a cross-reference). Reported, never counted;
  - ``none``.

The outputs are generated and committed, like the statement indexes:

* ``docs/spec/conformance-matrix.json``
* ``docs/conformance-matrix.md``

Usage::

    python tools/build_conformance_matrix.py           # rewrite both outputs
    python tools/build_conformance_matrix.py --check   # fail if they are stale

Offline and standard-library only. Coverage here counts citations that were
checked against the text; it is not a measure of conformance.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = ROOT / "docs" / "spec"
VERSIONS = ("3.1.1", "5.0")
APPLICABILITY = SPEC_DIR / "applicability.json"
MATRIX_JSON = SPEC_DIR / "conformance-matrix.json"
MATRIX_PAGE = ROOT / "docs" / "conformance-matrix.md"

CLASSES = ("client", "server", "both")
# Files whose labels are test data, not claims about the implementation: the
# extractor's synthetic labels, and the matrix's own tests.
SYNTHETIC_LABEL_SOURCES = frozenset(
    {
        "tests/unit/test_spec_extractor.py",
        "tests/project/test_conformance_matrix.py",
    }
)

LABEL = re.compile(r"MQTT-\d+(?:\.\d+)*-\d+")
_BRACKETED = re.compile(r"\[(MQTT-\d+(?:\.\d+)*-\d+)\]")
# A quotation opens a docstring: ``[MQTT-x.y-n] <verbatim text>``.
_QUOTATION = re.compile(r"\A\s*\[(MQTT-\d+(?:\.\d+)*-\d+)\]\s*(.+?)(?:\n\s*\n|\Z)", re.DOTALL)


@dataclass(slots=True)
class Statement:
    version: str
    label: str
    section: str
    text: str
    applicability: str = "both"
    basis: str = "mention"
    verified_by: list[str] = field(default_factory=list)
    cited_in: list[str] = field(default_factory=list)

    @property
    def evidence(self) -> str:
        if self.verified_by:
            return "verified"
        return "cited" if self.cited_in else "none"

    @property
    def binds_client(self) -> bool:
        return self.applicability in ("client", "both")


def normalise(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def quote_matches(quoted: str, text: str) -> bool:
    """Whether ``quoted`` reproduces ``text`` in full.

    Case, punctuation and line breaks are ignored; words are not. A quotation
    that stops early is refused: the part left out is often the obligation
    (a statement cut before "NOT" reads as its opposite).
    """
    quoted_words = normalise(quoted)
    return bool(quoted_words) and quoted_words == normalise(text)


# A test named for one specification version is evidence for that version
# only, even where the other version states the same words under the label.
_VERSION_PREFIXES = {"test_mqtt311_": "3.1.1", "test_mqtt5_": "5.0"}
_NEVER_RUN = ("skip", "xfail")


def _is_evidence(node: ast.stmt) -> bool:
    """A collected test function that pytest runs and that does something."""
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    if not node.name.startswith("test_"):
        return False
    if any(
        word in ast.unparse(decorator) for decorator in node.decorator_list for word in _NEVER_RUN
    ):
        return False
    # More than a docstring: a body that asserts nothing cannot be told from
    # one that does by reading, but an empty one can.
    return len(node.body) > 1


def _index(version: str) -> list[dict[str, str]]:
    path = SPEC_DIR / f"mqtt-v{version}-statements.json"
    return json.loads(path.read_text(encoding="utf-8"))["statements"]


def load_statements() -> dict[tuple[str, str], Statement]:
    statements: dict[tuple[str, str], Statement] = {}
    for version in VERSIONS:
        for item in _index(version):
            statements[(version, item["id"])] = Statement(
                version, item["id"], item["section"], item["text"]
            )
    return statements


def appendix_aliases() -> set[str]:
    """Labels the specification's own appendix misprints for an indexed statement."""
    return {
        item["appendix_id"]
        for version in VERSIONS
        for item in _index(version)
        if "appendix_id" in item
    }


def names_both_parties(text: str) -> bool:
    return "Client" in text and "Server" in text


def apply_applicability(statements: dict[tuple[str, str], Statement]) -> list[str]:
    """Classify every statement; return the problems found in the reviewed file.

    A statement that names one party binds that party, and one that names
    neither is a rule of the format that binds both. Those two rules are wrong
    for passive constructions ("a PUBLISH sent to a Client MUST...") and say
    nothing when both parties are named, so every such statement carries a
    reviewed class instead.
    """
    data = json.loads(APPLICABILITY.read_text(encoding="utf-8"))
    reviewed: dict[str, dict[str, str]] = data["reviewed"]
    problems: list[str] = []
    for version, labels in reviewed.items():
        for label, value in labels.items():
            if (version, label) not in statements:
                problems.append(f"{version} {label}: reviewed but not a statement")
            elif value not in CLASSES:
                problems.append(f"{version} {label}: unknown class {value!r}")
    for (version, label), statement in statements.items():
        value = reviewed.get(version, {}).get(label)
        if value in CLASSES:
            statement.applicability, statement.basis = value, "reviewed"
            continue
        if names_both_parties(statement.text):
            problems.append(f"{version} {label}: names both parties and is not reviewed")
        elif "Client" in statement.text:
            statement.applicability = "client"
        elif "Server" in statement.text:
            statement.applicability = "server"
    return problems


def _relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _verify_quotations(
    source: str, relative: str, statements: dict[tuple[str, str], Statement]
) -> set[tuple[str, str]]:
    """Record the statements whose text a test docstring of ``source`` quotes."""
    verified: set[tuple[str, str]] = set()
    if not Path(relative).name.startswith("test_"):
        return verified  # pytest does not collect it
    # Module level only: a function nested in another is never collected.
    for node in ast.parse(source).body:
        if not _is_evidence(node):
            continue
        assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        match = _QUOTATION.match(ast.get_docstring(node, clean=True) or "")
        if match is None:
            continue
        label, quoted = match.groups()
        only = next(
            (v for prefix, v in _VERSION_PREFIXES.items() if node.name.startswith(prefix)), None
        )
        for version in VERSIONS:
            if only is not None and version != only:
                continue
            statement = statements.get((version, label))
            if statement is not None and quote_matches(quoted, statement.text):
                statement.verified_by.append(f"{relative}::{node.name}")
                verified.add((version, label))
    return verified


def collect_evidence(statements: dict[tuple[str, str], Statement]) -> list[str]:
    """Attach verified and cited evidence; return labels that name no statement."""
    known_labels = {label for _version, label in statements} | appendix_aliases()
    unknown: list[str] = []
    for directory in ("tests", "src"):
        for path in sorted((ROOT / directory).rglob("*.py")):
            relative = _relative(path)
            if relative in SYNTHETIC_LABEL_SOURCES:
                continue
            source = path.read_text(encoding="utf-8")
            if "MQTT-" not in source:
                continue
            verified_here = (
                _verify_quotations(source, relative, statements) if directory == "tests" else set()
            )
            for label in sorted(set(LABEL.findall(source))):
                if label not in known_labels:
                    unknown.append(f"{relative}: {label}")
                    continue
                for version in VERSIONS:
                    statement = statements.get((version, label))
                    if statement is not None and (version, label) not in verified_here:
                        statement.cited_in.append(relative)
    for statement in statements.values():
        statement.verified_by.sort()
        statement.cited_in = sorted(set(statement.cited_in))
    return unknown


def _section_key(section: str) -> tuple[int, ...]:
    return tuple(int(part) for part in section.split("."))


def _label_key(statement: Statement) -> tuple[tuple[int, ...], int]:
    body, _, index = statement.label.removeprefix("MQTT-").rpartition("-")
    return _section_key(body), int(index)


def _ordered(statements: dict[tuple[str, str], Statement], version: str) -> list[Statement]:
    return sorted((s for s in statements.values() if s.version == version), key=_label_key)


def render_json(statements: dict[tuple[str, str], Statement]) -> str:
    payload = {
        "_comment": (
            "Generated by tools/build_conformance_matrix.py; do not edit. "
            "Evidence counts quotations checked against the vendored text, "
            "not conformance."
        ),
        "versions": {
            version: [
                {
                    "label": statement.label,
                    "section": statement.section,
                    "applicability": statement.applicability,
                    "applicability_basis": statement.basis,
                    "evidence": statement.evidence,
                    "verified_by": statement.verified_by,
                    "cited_in": statement.cited_in,
                }
                for statement in _ordered(statements, version)
            ]
            for version in VERSIONS
        },
    }
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def _chapter(statement: Statement) -> str:
    return ".".join(statement.section.split(".")[:2])


def render_page(statements: dict[tuple[str, str], Statement]) -> str:
    lines = [
        "# Conformance matrix",
        "",
        "<!-- Generated by tools/build_conformance_matrix.py; do not edit. -->",
        "",
        "Which numbered MQTT statements a test verifies, generated from the tree.",
        "A statement is identified by its specification version *and* its label:",
        "119 labels exist in both documents with different text.",
        "",
        "- **Verified** — a test's docstring opens with the label and a quotation",
        "  that matches this version's text.",
        "- **Cited** — the label appears in a test or in the library without such a",
        "  quotation. Shown for orientation, never counted as evidence.",
        "- **Binds a client** — the statement obliges a client, alone or together",
        "  with the server, according to `docs/spec/applicability.json`. Statements",
        "  naming one party are classified by that mention; every statement naming",
        "  both, and each passive construction found, was classified by reading it.",
        "",
        "A verified statement is one checked behaviour, not proof that the statement",
        "holds in every case, and an unverified one is not a known violation. See",
        "[Protocol conformance](conformance.md) for the method and its limits.",
        "",
        "## Summary",
        "",
        "| Version | Statements | Bind a client | Verified | Cited only | No evidence |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for version in VERSIONS:
        ordered = _ordered(statements, version)
        binding = [s for s in ordered if s.binds_client]
        counts = Counter(s.evidence for s in binding)
        lines.append(
            f"| MQTT {version} | {len(ordered)} | {len(binding)} | {counts['verified']} "
            f"| {counts['cited']} | {counts['none']} |"
        )
    lines += [
        "",
        "The last three columns count only the statements that bind a client.",
    ]
    for version in VERSIONS:
        ordered = _ordered(statements, version)
        binding = [s for s in ordered if s.binds_client]
        chapters = sorted({_chapter(s) for s in binding}, key=_section_key)
        lines += [
            "",
            f"## MQTT {version}",
            "",
            "| Section | Bind a client | Verified | Cited only | No evidence |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
        for chapter in chapters:
            inside = [s for s in binding if _chapter(s) == chapter]
            counts = Counter(s.evidence for s in inside)
            lines.append(
                f"| {chapter} | {len(inside)} | {counts['verified']} | {counts['cited']} "
                f"| {counts['none']} |"
            )
        verified = [s for s in binding if s.evidence == "verified"]
        lines += ["", "### Verified", ""]
        lines += [
            f"- `{s.label}` — " + ", ".join(f"`{test.split('::')[1]}`" for test in s.verified_by)
            for s in verified
        ] or ["None."]
        for title, evidence in (("Cited only", "cited"), ("No evidence", "none")):
            labels = [f"`{s.label}`" for s in binding if s.evidence == evidence]
            lines += ["", f"### {title}", "", ", ".join(labels) if labels else "None."]
    return "\n".join(lines) + "\n"


def build() -> tuple[str, str, list[str]]:
    statements = load_statements()
    problems = apply_applicability(statements)
    problems += [f"unknown label, {entry}" for entry in collect_evidence(statements)]
    return render_json(statements), render_page(statements), problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="fail if the outputs are stale")
    args = parser.parse_args(argv)
    matrix, page, problems = build()
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        return 1
    stale = [
        _relative(path)
        for path, content in ((MATRIX_JSON, matrix), (MATRIX_PAGE, page))
        if not path.exists() or path.read_text(encoding="utf-8") != content
    ]
    if args.check:
        for path in stale:
            print(f"{path} is stale; run python tools/build_conformance_matrix.py", file=sys.stderr)
        return 1 if stale else 0
    MATRIX_JSON.write_text(matrix, encoding="utf-8")
    MATRIX_PAGE.write_text(page, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
