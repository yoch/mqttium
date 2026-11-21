#!/usr/bin/env python3
"""Check the broker's register of server-side normative statements.

The register `server-statements.json` says, for every numbered normative
statement of MQTT 3.1.1 and MQTT 5.0, whether it binds a server and what
evidence the broker has for it. This script keeps that register honest
where a machine can: the statement set, the fields, and every citation.

    python3 mqttiumd/conformance/check.py             # check, rewrite SUMMARY.md
    python3 mqttiumd/conformance/check.py --check     # check only (what CI runs)
    python3 mqttiumd/conformance/check.py --selftest  # the checker refuses bad input

Standard library only. It does not run any test and proves no behaviour:
it shows that each cited test and file exists, not that the test passes
or that it is sufficient.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path, PurePosixPath

REGISTER = "mqttiumd/conformance/server-statements.json"
SUMMARY = "mqttiumd/conformance/SUMMARY.md"
SOURCES = {
    "5.0": "docs/spec/mqtt-v5.0-statements.json",
    "3.1.1": "docs/spec/mqtt-v3.1.1-statements.json",
}
STATUSES = (
    "covered",
    "implemented-untested",
    "profile-refused",
    "open",
    "not-applicable",
)
# Statuses whose claim rests on a place in the code.
NEEDS_CODE = ("implemented-untested", "profile-refused")
ENTRY_KEYS = {"id", "feature", "applies_to_server", "status", "evidence", "code", "note"}
REQUIRED_KEYS = ENTRY_KEYS - {"code"}
EVIDENCE_KEYS = {"file", "test"}
# Frozen receptions (ADR 0044) are not maintained against the runtime:
# they may be named in a note, never counted as evidence.
FROZEN = "mqttiumd/tests/system/"
# A Rust test function is `fn name(` with `#[test]` among the attribute
# lines just above it.
ATTRIBUTE_WINDOW = 4
FEATURE = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*\Z")
SYMBOL = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class Refused(Exception):
    """The register, or what it cites, is not acceptable."""

    def __init__(self, errors: list[str]):
        super().__init__("\n".join(errors))
        self.errors = errors


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key {key!r}")
        result[key] = value
    return result


def read_json(path: Path, errors: list[str], what: str):
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle, object_pairs_hook=_no_duplicate_keys)
    except FileNotFoundError:
        errors.append(f"{what}: {path} does not exist")
    except (ValueError, UnicodeDecodeError) as error:
        errors.append(f"{what}: {path} is not valid JSON: {error}")
    return None


def source_ids(root: Path, errors: list[str]) -> dict[str, list[str]]:
    """Statement identifiers of the two source registers, in their order."""
    result: dict[str, list[str]] = {}
    for version, relative in SOURCES.items():
        document = read_json(root / relative, errors, f"source register {version}")
        if document is None:
            continue
        try:
            ids = [statement["id"] for statement in document["statements"]]
        except (KeyError, TypeError):
            errors.append(f"source register {version}: {relative} has no statement list")
            continue
        if len(set(ids)) != len(ids):
            errors.append(f"source register {version}: {relative} repeats an id")
            continue
        result[version] = ids
    return result


def repo_file(root: Path, relative, errors: list[str], where: str) -> Path | None:
    """A repository-relative path that names an existing file."""
    if not isinstance(relative, str) or not relative:
        errors.append(f"{where}: a file must be a non-empty repository-relative path")
        return None
    pure = PurePosixPath(relative)
    if pure.is_absolute() or ".." in pure.parts or "\\" in relative:
        errors.append(f"{where}: {relative!r} is not a repository-relative path")
        return None
    path = root / pure
    if not path.is_file():
        errors.append(f"{where}: file {relative} does not exist")
        return None
    return path


def _lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return []


def _follows_test_attribute(lines: list[str], index: int) -> bool:
    """`#[test]` above line `index`, separated by attributes and comments only."""
    for previous in reversed(lines[max(0, index - ATTRIBUTE_WINDOW) : index]):
        stripped = previous.strip()
        if stripped == "#[test]":
            return True
        if stripped and not stripped.startswith(("#[", "//")):
            return False
    return False


def has_test(path: Path, name: str) -> bool:
    """Whether `path` defines a test function called exactly `name`."""
    lines = _lines(path)
    if path.suffix == ".py":
        definition = re.compile(r"\s*(async\s+)?def\s+" + re.escape(name) + r"\s*\(")
        return any(definition.match(line) for line in lines)
    if path.suffix != ".rs":
        return False
    definition = re.compile(
        r"\s*(pub(\([a-z]+\))?\s+)?(async\s+)?fn\s+" + re.escape(name) + r"\s*[(<]"
    )
    return any(
        definition.match(line) and _follows_test_attribute(lines, index)
        for index, line in enumerate(lines)
    )


def has_symbol(path: Path, symbol: str) -> bool:
    word = re.compile(r"(?<![A-Za-z0-9_])" + re.escape(symbol) + r"(?![A-Za-z0-9_])")
    return any(word.search(line) for line in _lines(path))


def check_fields(entry: dict, where: str, errors: list[str]) -> bool:
    """The shape of one entry. False when the rest cannot be checked."""
    unknown = set(entry) - ENTRY_KEYS
    missing = REQUIRED_KEYS - set(entry)
    if unknown:
        errors.append(f"{where}: unknown field(s) {sorted(unknown)}")
    if missing:
        errors.append(f"{where}: missing field(s) {sorted(missing)}")
        return False
    status = entry["status"]
    if status not in STATUSES:
        errors.append(f"{where}: invalid status {status!r}; expected one of {list(STATUSES)}")
        return False
    if not isinstance(entry["applies_to_server"], bool):
        errors.append(f"{where}: applies_to_server must be true or false")
    elif not entry["applies_to_server"] and status != "not-applicable":
        errors.append(
            f"{where}: a statement that does not bind a server is not-applicable, not {status}"
        )
    if not isinstance(entry["feature"], str) or not FEATURE.match(entry["feature"]):
        errors.append(f"{where}: feature must be a lower-case slug such as 'topic-alias'")
    if not isinstance(entry["note"], str) or not entry["note"].strip():
        errors.append(f"{where}: note must be a non-empty sentence")
    return True


def check_citation(root: Path, item, where: str, errors: list[str]) -> tuple | None:
    """One evidence entry. Returns its (file, test) pair when well formed."""
    if not isinstance(item, dict) or set(item) != EVIDENCE_KEYS:
        errors.append(f'{where}: an evidence entry is {{"file": ..., "test": ...}}')
        return None
    path = repo_file(root, item["file"], errors, where)
    test = item["test"]
    if not isinstance(test, str) or not SYMBOL.match(test):
        errors.append(f"{where}: {test!r} is not a test function name")
        return None
    if path is None:
        return None
    if item["file"].startswith(FROZEN):
        errors.append(
            f"{where}: {item['file']} is a frozen reception; name it in the note, not as evidence"
        )
    elif not has_test(path, test):
        errors.append(f"{where}: {item['file']} has no test function named {test}")
    return item["file"], test


def check_evidence(root: Path, entry: dict, where: str, errors: list[str]) -> None:
    evidence = entry["evidence"]
    if not isinstance(evidence, list):
        errors.append(f"{where}: evidence must be a list")
        return
    if entry["status"] == "covered" and not evidence:
        errors.append(f"{where}: a covered statement names at least one test")
    if entry["status"] != "covered" and evidence:
        errors.append(
            f"{where}: only a covered statement carries evidence; say the rest in the note"
        )
    seen = set()
    for item in evidence:
        pair = check_citation(root, item, where, errors)
        if pair is not None and pair in seen:
            errors.append(f"{where}: evidence {pair[0]}::{pair[1]} is listed twice")
        seen.add(pair)


def check_code(root: Path, entry: dict, where: str, errors: list[str]) -> None:
    code = entry.get("code", [])
    if not isinstance(code, list):
        errors.append(f'{where}: code must be a list of "path:symbol" strings')
        return
    if entry["status"] in NEEDS_CODE and not code:
        errors.append(f"{where}: a {entry['status']} statement cites the code it rests on")
    for citation in code:
        if not isinstance(citation, str):
            errors.append(f'{where}: a code citation is a "path:symbol" string')
            continue
        relative, _, symbol = citation.partition(":")
        path = repo_file(root, relative, errors, where)
        if path is None:
            continue
        if symbol and (not SYMBOL.match(symbol) or not has_symbol(path, symbol)):
            errors.append(f"{where}: {relative} does not contain {symbol!r}")


def check_entry(root: Path, version: str, entry, errors: list[str]) -> None:
    if not isinstance(entry, dict):
        errors.append(f"{version}: an entry is not an object")
        return
    where = f"{version} {entry.get('id', '<no id>')}"
    if check_fields(entry, where, errors):
        check_evidence(root, entry, where, errors)
        check_code(root, entry, where, errors)


def check_statement_set(version: str, ids: list, expected: list[str], errors: list[str]) -> None:
    """Every identifier of the source register exactly once, in its order."""
    known = set(expected)
    counted: dict = {}
    for identifier in ids:
        counted[identifier] = counted.get(identifier, 0) + 1
    before = len(errors)
    for identifier in expected:
        if identifier not in counted:
            errors.append(f"{version} {identifier}: missing from the register")
    for identifier, count in counted.items():
        if identifier not in known:
            errors.append(f"{version} {identifier}: not a statement of {SOURCES[version]}")
        elif count > 1:
            errors.append(f"{version} {identifier}: appears {count} times")
    if len(errors) == before and ids != expected:
        first = next(index for index in range(len(ids)) if ids[index] != expected[index])
        errors.append(
            f"{version}: statements are not in the order of {SOURCES[version]}; "
            f"position {first} holds {ids[first]}, expected {expected[first]}"
        )


def validate(root: Path) -> tuple[dict, list[str]]:
    """Read and check the register. Returns it with every error found."""
    errors: list[str] = []
    sources = source_ids(root, errors)
    register = read_json(root / REGISTER, errors, "register")
    if register is None or errors:
        return {}, errors
    versions = register.get("versions") if isinstance(register, dict) else None
    if not isinstance(versions, dict):
        return {}, ["register: no 'versions' object"]
    for version in sorted(set(versions) - set(SOURCES)):
        errors.append(f"register: unknown protocol version {version!r}")
    for version, expected in sources.items():
        entries = versions.get(version)
        if not isinstance(entries, list):
            errors.append(f"register: no statement list for protocol version {version}")
            continue
        ids = [entry.get("id") if isinstance(entry, dict) else None for entry in entries]
        check_statement_set(version, ids, expected, errors)
        for entry in entries:
            check_entry(root, version, entry, errors)
    return register, errors


def cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def count(entries: list[dict], status: str) -> int:
    return sum(1 for entry in entries if entry["status"] == status)


SECTIONS = (
    (
        "open",
        "Open",
        "Applies to a server, and either no implementation and no deliberate "
        "refusal was found, or the behaviour looks non-conformant. The note says which.",
    ),
    (
        "profile-refused",
        "Refused by the profile",
        "The broker deliberately rejects or does not offer the feature today. "
        "The note says what a client observes.",
    ),
)


def render_counts(versions: dict) -> list[str]:
    lines = [
        "## Counts",
        "",
        "| Status | " + " | ".join(f"MQTT {version}" for version in SOURCES) + " |",
        "| - | " + " | ".join("-:" for _ in SOURCES) + " |",
    ]
    for status in STATUSES:
        counts = [str(count(versions[version], status)) for version in SOURCES]
        lines.append(f"| `{status}` | " + " | ".join(counts) + " |")
    totals = [str(len(versions[version])) for version in SOURCES]
    binding = [
        str(sum(1 for entry in versions[version] if entry["applies_to_server"]))
        for version in SOURCES
    ]
    lines.append("| statements | " + " | ".join(totals) + " |")
    lines.append("| of which bind a server | " + " | ".join(binding) + " |")
    return lines


def render_section(versions: dict, status: str, title: str, meaning: str) -> list[str]:
    rows: dict[str, list[tuple[str, dict]]] = {}
    for version in SOURCES:
        for entry in versions[version]:
            if entry["status"] == status:
                rows.setdefault(entry["feature"], []).append((version, entry))
    total = sum(len(group) for group in rows.values())
    with_code = status == "profile-refused"
    lines = ["", f"## {title} ({total})", "", meaning]
    for feature in sorted(rows):
        lines += ["", f"### {feature} ({len(rows[feature])})", ""]
        if with_code:
            lines += ["| MQTT | Statement | Code | Note |", "| - | - | - | - |"]
        else:
            lines += ["| MQTT | Statement | Note |", "| - | - | - |"]
        for version, entry in rows[feature]:
            columns = [version, f"`{entry['id']}`"]
            if with_code:
                columns.append("<br>".join(f"`{cell(item)}`" for item in entry.get("code", [])))
            columns.append(cell(entry["note"]))
            lines.append("| " + " | ".join(columns) + " |")
    return lines


def render_summary(register: dict) -> str:
    versions = register["versions"]
    lines = [
        "# Server-side conformance register: summary",
        "",
        "Generated by `python3 mqttiumd/conformance/check.py` from",
        "`server-statements.json`; do not edit. A `covered` row names a test that",
        "exercises the statement. It is not a proof of conformance: see `README.md`.",
        "",
    ]
    lines += render_counts(versions)
    for status, title, meaning in SECTIONS:
        lines += render_section(versions, status, title, meaning)
    return "\n".join(lines) + "\n"


def run(root: Path, check_only: bool, out=sys.stdout) -> int:
    register, errors = validate(root)
    if errors:
        raise Refused(errors)
    summary = render_summary(register)
    path = root / SUMMARY
    current = path.read_text(encoding="utf-8") if path.is_file() else None
    if check_only and current != summary:
        raise Refused([f"{SUMMARY} is not up to date; run python3 mqttiumd/conformance/check.py"])
    if current != summary:
        path.write_text(summary, encoding="utf-8")
        print(f"wrote {SUMMARY}", file=out)
    for version in SOURCES:
        entries = register["versions"][version]
        counts = ", ".join(f"{status} {count(entries, status)}" for status in STATUSES)
        print(f"MQTT {version}: {len(entries)} statements: {counts}", file=out)
    return 0


# ----- self-test: the checker refuses what it claims to check -----

DEMO = "mqttiumd/crates/demo/src/"


def _fixture(root: Path) -> dict:
    """A two-statement repository the checker accepts."""
    for version, relative in SOURCES.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        statements = [{"id": "MQTT-1.0.0-1"}, {"id": "MQTT-1.0.0-2"}]
        path.write_text(json.dumps({"mqtt_version": version, "statements": statements}))
    rust = root / DEMO / "lib.rs"
    rust.parent.mkdir(parents=True)
    rust.write_text(
        "fn refuse() {}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n"
        '    #[cfg(feature = "x")]\n    fn a_real_test() {}\n\n'
        "    fn a_helper() {}\n}\n"
    )
    frozen = root / FROZEN / "test_frozen.py"
    frozen.parent.mkdir(parents=True)
    frozen.write_text("def test_frozen():\n    pass\n")
    python = root / "mqttiumd/tools/test_demo.py"
    python.parent.mkdir(parents=True)
    python.write_text("class T:\n    def test_python(self):\n        pass\n")

    def entries():
        return [
            {
                "id": "MQTT-1.0.0-1",
                "feature": "demo",
                "applies_to_server": True,
                "status": "covered",
                "evidence": [
                    {"file": DEMO + "lib.rs", "test": "a_real_test"},
                    {"file": "mqttiumd/tools/test_demo.py", "test": "test_python"},
                ],
                "evidence_kind": "direct",
                "note": "Exercised.",
            },
            {
                "id": "MQTT-1.0.0-2",
                "feature": "demo",
                "applies_to_server": True,
                "status": "profile-refused",
                "evidence": [],
                "code": [DEMO + "lib.rs:refuse"],
                "note": "Refused.",
            },
        ]

    return {"versions": {version: entries() for version in SOURCES}}


def _install(root: Path, register: dict, summary: bool = True) -> None:
    path = root / REGISTER
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(register, indent=2))
    if summary:
        (root / SUMMARY).write_text(render_summary(register))


def _first(register: dict) -> dict:
    return register["versions"]["5.0"][0]


def _second(register: dict) -> dict:
    return register["versions"]["5.0"][1]


def _set(entry, key: str, value):
    def mutate(register: dict) -> None:
        entry(register)[key] = value

    return mutate


def _drop(entry, key: str):
    def mutate(register: dict) -> None:
        del entry(register)[key]

    return mutate


def _evidence(key: str, value: str):
    def mutate(register: dict) -> None:
        _first(register)["evidence"][0][key] = value

    return mutate


# Each case damages the valid fixture in one way and names the refusal expected.
CASES = (
    (
        "a missing id",
        lambda r: r["versions"]["5.0"].pop(),
        "MQTT-1.0.0-2: missing from the register",
    ),
    (
        "a duplicate id",
        lambda r: r["versions"]["5.0"].append(dict(_first(r))),
        "MQTT-1.0.0-1: appears 2 times",
    ),
    ("an unknown id", _set(_second, "id", "MQTT-9.9.9-9"), "MQTT-9.9.9-9: not a statement of"),
    (
        "another order",
        lambda r: r["versions"]["3.1.1"].reverse(),
        "statements are not in the order of",
    ),
    (
        "a cited test that does not exist",
        _evidence("test", "a_test_that_does_not_exist"),
        "has no test function named a_test_that_does_not_exist",
    ),
    (
        "a helper cited as a test",
        _evidence("test", "a_helper"),
        "has no test function named a_helper",
    ),
    (
        "a cited file that does not exist",
        _evidence("file", DEMO + "gone.rs"),
        "gone.rs does not exist",
    ),
    ("an invalid status", _set(_first, "status", "done"), "invalid status 'done'"),
    (
        "covered without evidence",
        _set(_first, "evidence", []),
        "a covered statement names at least one test",
    ),
    (
        "a frozen reception as evidence",
        _set(_first, "evidence", [{"file": FROZEN + "test_frozen.py", "test": "test_frozen"}]),
        "is a frozen reception",
    ),
    (
        "a refusal without code",
        _drop(_second, "code"),
        "a profile-refused statement cites the code",
    ),
    (
        "a code file that does not exist",
        _set(_second, "code", [DEMO + "gone.rs:refuse"]),
        "gone.rs does not exist",
    ),
    (
        "a code symbol that does not exist",
        _set(_second, "code", [DEMO + "lib.rs:accept"]),
        "does not contain 'accept'",
    ),
    ("a missing field", _drop(_first, "note"), "missing field(s) ['note']"),
    (
        "a client statement with a server status",
        _set(_first, "applies_to_server", False),
        "does not bind a server is not-applicable",
    ),
    (
        "evidence on a refusal",
        lambda r: _second(r).update(evidence=list(_first(r)["evidence"])),
        "only a covered statement carries evidence",
    ),
    (
        "an evidence kind that is not direct or partial",
        _set(_first, "evidence_kind", "indirect"),
        "evidence_kind 'indirect'; expected one of ['direct', 'partial']",
    ),
    (
        "an evidence kind on a statement that is not covered",
        _set(_second, "evidence_kind", "partial"),
        "only a covered statement has an evidence_kind",
    ),
)


def _stale_summary(root: Path, good: dict, quiet) -> list[str]:
    """A stale summary is refused by --check and repaired by the default mode."""
    _install(root, good)
    (root / SUMMARY).write_text("stale\n")
    try:
        run(root, check_only=True, out=quiet)
        return ["a stale summary: --check accepted it"]
    except Refused as error:
        if "not up to date" not in str(error):
            return [f"a stale summary: unexpected refusal {error}"]
    run(root, check_only=False, out=quiet)
    try:
        run(root, check_only=True, out=quiet)
    except Refused as error:
        return [f"the rewritten summary was refused: {error}"]
    return []


def _evidence_kind_counts(good: dict) -> list[str]:
    """The summary counts the covered rows by the kind of their evidence."""
    register = json.loads(json.dumps(good))
    summary = render_summary(register)
    wanted = ["| of which `direct` | 1 | 1 |", "| of which `partial` | 0 | 0 |", "| not reviewed | 0 | 0 |"]
    _first(register)["evidence_kind"] = "partial"
    del register["versions"]["3.1.1"][0]["evidence_kind"]
    changed = render_summary(register)
    wanted_changed = ["| of which `direct` | 0 | 0 |", "| of which `partial` | 1 | 0 |", "| not reviewed | 0 | 1 |"]
    return [
        f"the counts by evidence kind: the summary has no line {line!r}"
        for text, lines in ((summary, wanted), (changed, wanted_changed))
        for line in lines
        if line not in text.splitlines()
    ]


def _selftest_failures(root: Path, quiet) -> list[str]:
    failures = []
    good = _fixture(root)
    _install(root, good)
    try:
        run(root, check_only=True, out=quiet)
    except Refused as error:
        failures.append(f"the valid fixture was refused: {error}")
    failures += _evidence_kind_counts(good)
    for name, mutate, expected in CASES:
        register = json.loads(json.dumps(good))
        mutate(register)
        _install(root, register, summary=False)
        _, errors = validate(root)
        if not any(expected in error for error in errors):
            failures.append(f"{name}: expected {expected!r}, got {errors}")
    failures += _stale_summary(root, good, quiet)
    # A register with a repeated JSON key is not silently collapsed.
    (root / REGISTER).write_text('{"versions": {}, "versions": {}}')
    _, errors = validate(root)
    if not any("duplicate key" in error for error in errors):
        failures.append(f"a repeated JSON key: got {errors}")
    return failures


def selftest() -> int:
    scratch = Path(tempfile.mkdtemp(prefix="mqttiumd-conformance-selftest-"))
    try:
        root = scratch / "repo"
        root.mkdir()
        with open(scratch / "out.txt", "w", encoding="utf-8") as quiet:
            failures = _selftest_failures(root, quiet)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    for failure in failures:
        print(f"selftest FAILED: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("selftest: the valid fixture passes, and the checker refuses:")
    for name in [name for name, _, _ in CASES] + ["a stale summary", "a repeated JSON key"]:
        print(f"  - {name}")
    print("selftest: the summary counts the covered rows by the kind of their evidence")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check", action="store_true", help="verify only; fail when SUMMARY.md is stale"
    )
    mode.add_argument(
        "--selftest", action="store_true", help="show that the checker refuses bad input"
    )
    arguments = parser.parse_args(argv)
    if arguments.selftest:
        return selftest()
    try:
        return run(repository_root(), check_only=arguments.check)
    except Refused as refused:
        for error in refused.errors:
            print(f"error: {error}", file=sys.stderr)
        print(f"{len(refused.errors)} error(s) in {REGISTER}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
