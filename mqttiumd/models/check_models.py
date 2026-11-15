"""Check the model directories of mqttiumd without Java or TLC.

Run from ``mqttiumd/``::

    python models/check_models.py

Every directory under ``models/`` that holds a ``.tla``, ``.cfg`` or
``.json`` file is checked. The script exits with 1 and one line per defect
when:

* a ``.tla``, ``.cfg`` or ``.json`` file belongs to no sidecar, or a sidecar
  names a file that does not exist;
* the TLC runner would refuse the sidecar (``tools/formal/run_tlc.py``):
  the reading of a sidecar and the refusals are the runner's own code;
* a sidecar is not well formed beyond that: module name, status, issues,
  tests;
* a path listed in ``tests`` does not exist. The list holds only files that
  replay a counterexample of the model; a ``refinement`` model has at least
  one, a ``specification`` model may have none yet;
* a ``refinement`` model does not name, under ``replays``, the test
  functions that replay it, or one of them is not a function of a file
  listed in ``tests``;
* a configuration does not check what its sidecar entry declares. A passing
  entry states its ``invariants`` or its ``property``, and the list is the
  one of the ``.cfg``; a ``violation`` checks its invariant; a temporal
  check has exactly one property, whether it is written under ``PROPERTY``
  or ``PROPERTIES``; deadlock checking; a specification that states
  fairness; operators that the module defines;
* a property violated by a ``liveness`` configuration has no configuration
  that must satisfy it;
* the README of the directory has no correspondence table, or the table
  does not name every action of ``Next``, or a row does not say where the
  action is replayed.

``invariants`` is a member of the sidecar entries of mqttiumd; the runner
ignores it.

What is shared with the client, and how. Imported from the runner:
``EXPECTATIONS``, ``load_sidecars``, ``validated_checks``, ``SidecarError``.
Copied from ``tests/project/test_formal_models.py``, which is a pytest file
and cannot be imported here: the module header pattern, the operator
pattern, and the rules on ``CHECK_DEADLOCK``, on the single property and on
its repaired configuration. A change to one of them there is made here too.
Two differences are deliberate:

* statuses. The client knows ``refinement`` and ``legacy-flag``
  (``docs/formal-models.md``). Here: ``refinement``, with the same meaning
  and at least one test, and ``specification``: the actions encode the
  predicates of the specification and of the decision records, and no
  implementation exists yet to refine. ``legacy-flag`` is not accepted;
* fairness. The client's test looks for ``WF_``, ``SF_`` or ``Fairness`` in
  the specification; here a conjunct whose name starts with ``Fair`` counts
  too, since the specifications name their fairness (``FairBroker``).

It uses the standard library only. TLC itself is run by the runner with
``--models-dir``.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

MODELS = Path(__file__).resolve().parent
REPOSITORY = MODELS.parents[1]

_SPEC = importlib.util.spec_from_file_location(
    "run_tlc", REPOSITORY / "tools" / "formal" / "run_tlc.py"
)
assert _SPEC is not None and _SPEC.loader is not None
run_tlc = importlib.util.module_from_spec(_SPEC)
sys.modules["run_tlc"] = run_tlc
_SPEC.loader.exec_module(run_tlc)

STATUSES = frozenset({"specification", "refinement"})
MODEL_SUFFIXES = frozenset({".tla", ".cfg", ".json"})

# Every keyword of a TLC configuration file. The names that follow one, up
# to the next keyword, belong to it, on the same line or on later ones.
_CFG_KEYWORDS = frozenset(
    {
        "SPECIFICATION",
        "INIT",
        "NEXT",
        "INVARIANT",
        "INVARIANTS",
        "PROPERTY",
        "PROPERTIES",
        "CONSTANT",
        "CONSTANTS",
        "CONSTRAINT",
        "CONSTRAINTS",
        "ACTION_CONSTRAINT",
        "ACTION_CONSTRAINTS",
        "SYMMETRY",
        "VIEW",
        "ALIAS",
        "POSTCONDITION",
        "CHECK_DEADLOCK",
    }
)
_SAME_SECTION = {
    "INVARIANTS": "INVARIANT",
    "PROPERTIES": "PROPERTY",
    "CONSTRAINTS": "CONSTRAINT",
    "CONSTANTS": "CONSTANT",
    "ACTION_CONSTRAINTS": "ACTION_CONSTRAINT",
}

_MODULE_HEADER = re.compile(r"^-{4,}\s*MODULE\s+(\w+)\s*-{4,}", re.MULTILINE)
_OPERATOR = r"^{name}\s*(?:\([^)]*\))?\s*=="
_DEFINITION = r"^{name}\s*==(.*?)(?=^\S|\Z)"
_CORRESPONDENCE = re.compile(r"^## Correspondence\b.*?$(.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL)


def read_config(text: str) -> dict[str, list[str]]:
    """The tokens of each section of a TLC configuration, plurals merged."""
    text = re.sub(r"\(\*.*?\*\)", " ", text, flags=re.DOTALL)
    text = re.sub(r"\\\*.*", " ", text)
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for token in re.findall(r"[A-Za-z_]\w*|\S", text):
        if token in _CFG_KEYWORDS:
            current = sections.setdefault(_SAME_SECTION.get(token, token), [])
        elif current is not None:
            current.append(token)
    return sections


def _defines(module_text: str, operator: str) -> bool:
    return (
        re.search(_OPERATOR.format(name=re.escape(operator)), module_text, re.MULTILINE) is not None
    )


def _definition(module_text: str, operator: str) -> str | None:
    found = re.search(
        _DEFINITION.format(name=re.escape(operator)), module_text, re.MULTILINE | re.DOTALL
    )
    return found.group(1) if found else None


def actions_of(module_text: str) -> list[str]:
    """The operators that ``Next`` is made of, in order of appearance."""
    body = _definition(module_text, "Next")
    if body is None:
        return []
    names: list[str] = []
    # The sets that a quantifier ranges over are not actions.
    body = re.sub(r"\\in\s+\w+", "", body)
    for name in re.findall(r"\b([A-Z]\w*)\b", body):
        if name not in names and _defines(module_text, name):
            names.append(name)
    return names


def check_correspondence(directory: Path, module_text: str) -> list[str]:
    readme = directory / "README.md"
    if not readme.is_file():
        return [f"{directory.name}: no README.md with a correspondence table"]
    section = _CORRESPONDENCE.search(readme.read_text(encoding="utf-8"))
    if section is None:
        return [f"{directory.name}/README.md: no '## Correspondence' section"]
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in section.group(1).splitlines()
        if line.lstrip().startswith("|")
    ]
    if len(rows) < 3 or rows[0][0] != "Model action" or rows[0][-1] != "Replay":
        return [
            f"{directory.name}/README.md: the correspondence table starts with "
            "'Model action' and ends with 'Replay'"
        ]
    defects = []
    named: set[str] = set()
    for row in rows[2:]:
        named.update(re.findall(r"`(\w+)`", row[0]))
        if len(row) != len(rows[0]) or not row[-1]:
            defects.append(f"{directory.name}/README.md: row '{row[0]}' does not say its replay")
    for action in actions_of(module_text):
        if action not in named:
            defects.append(
                f"{directory.name}/README.md: the correspondence table does not name {action}"
            )
    return defects


def check_config(
    name: str, entry: dict[str, object], directory: Path, module_text: str
) -> list[str]:
    """One entry the runner accepts, against its configuration file."""
    config = str(entry["config"])
    where = f"{name}/{config}"
    expect = entry["expect"]
    defects = []
    if ("invariant" in entry) != (expect == "violation"):
        defects.append(f"{where}: 'invariant' is named by a violation and by nothing else")
    if "property" in entry and expect not in ("liveness", "pass"):
        defects.append(f"{where}: 'property' is named by a temporal check only")
    stated = entry.get("invariants")
    if stated is not None and (
        not isinstance(stated, list) or not all(isinstance(item, str) for item in stated)
    ):
        return [*defects, f"{where}: 'invariants' is a list of names"]
    sections = read_config((directory / config).read_text(encoding="utf-8"))
    specification = sections.get("SPECIFICATION", [])
    if len(specification) != 1:
        return [*defects, f"{where}: exactly one SPECIFICATION is required"]
    invariants = sections.get("INVARIANT", [])
    properties = sections.get("PROPERTY", [])
    if expect == "pass" and not stated and "property" not in entry:
        defects.append(f"{where}: a passing configuration states its invariants or its property")
    if expect == "pass" or stated is not None:
        # What the configuration checks is what the entry states, no less.
        if sorted(invariants) != sorted(stated or []):
            defects.append(
                f"{where}: INVARIANT {sorted(invariants)} differs from the sidecar's "
                f"{sorted(stated or [])}"
            )
    if expect == "violation" and entry.get("invariant") not in invariants:
        defects.append(f"{where}: the declared invariant is not checked by the configuration")
    declared = [entry["property"]] if "property" in entry else []
    if properties != declared:
        defects.append(f"{where}: PROPERTY {properties} differs from the sidecar's {declared}")
    defects.extend(check_running(where, str(expect), sections, module_text))
    return defects


def check_running(
    where: str, expect: str, sections: dict[str, list[str]], module_text: str
) -> list[str]:
    """Deadlock checking, fairness and the operators a configuration names."""
    defects = []
    specification = sections["SPECIFICATION"]
    properties = sections.get("PROPERTY", [])
    no_deadlock = sections.get("CHECK_DEADLOCK") == ["FALSE"]
    if expect == "liveness" and not no_deadlock:
        defects.append(f"{where}: a liveness violation sets CHECK_DEADLOCK FALSE")
    if expect == "pass" and no_deadlock:
        defects.append(f"{where}: a passing configuration keeps deadlock checking on")
    if properties:
        body = _definition(module_text, specification[0])
        if body is None or not re.search(r"\b[WS]F_|Fair", body):
            defects.append(f"{where}: {specification[0]} states no fairness")
    named = [*specification, *sections.get("INVARIANT", []), *properties]
    for operator in [*named, *sections.get("CONSTRAINT", [])]:
        if not _defines(module_text, operator):
            defects.append(f"{where}: {operator} is not defined in the module")
    return defects


def check_sidecar(sidecar: dict[str, object]) -> list[str]:
    path = sidecar["_path"]
    assert isinstance(path, Path)
    name = path.stem
    try:
        run_tlc.validated_checks(sidecar)
    except run_tlc.SidecarError as refusal:
        return [str(refusal)]
    defects = check_metadata(name, sidecar)
    configs = sidecar["configs"]
    assert isinstance(configs, list)
    module_text = path.with_suffix(".tla").read_text(encoding="utf-8")
    header = _MODULE_HEADER.search(module_text)
    if header is None or header.group(1) != name:
        defects.append(f"{name}: the module header does not name {name}")
    for entry in configs:
        defects.extend(check_config(name, entry, path.parent, module_text))
    violated = {e.get("property") for e in configs if e["expect"] == "liveness"}
    satisfied = {e.get("property") for e in configs if e["expect"] == "pass"}
    for missing in sorted(str(item) for item in violated - satisfied):
        defects.append(f"{name}: no configuration must satisfy the property {missing}")
    defects.extend(check_correspondence(path.parent, module_text))
    return defects


def check_metadata(name: str, sidecar: dict[str, object]) -> list[str]:
    defects = []
    if sidecar["module"] != f"{name}.tla":
        defects.append(f"{name}: 'module' is {name}.tla")
    status = sidecar["status"]
    if status not in STATUSES:
        defects.append(f"{name}: unknown status {status!r}")
    if not str(sidecar["summary"]).strip():
        defects.append(f"{name}: 'summary' is a sentence")
    issues = sidecar.get("issues")
    if (
        not isinstance(issues, list)
        or not issues
        or not all(isinstance(number, int) and number > 0 for number in issues)
    ):
        defects.append(f"{name}: 'issues' lists at least one issue or pull request number")
    tests = sidecar.get("tests")
    if not isinstance(tests, list) or not all(isinstance(test, str) for test in tests):
        defects.append(f"{name}: 'tests' is a list of paths")
        tests = []
    for test in tests:
        if not (REPOSITORY / test).is_file():
            defects.append(f"{name}: test {test} does not exist")
    if status == "refinement" and not tests:
        defects.append(f"{name}: a refinement model names the tests that replay it")
    defects.extend(check_replays(name, sidecar, tests))
    return defects


def check_replays(name: str, sidecar: dict[str, object], tests: list[str]) -> list[str]:
    """Each replay names a test function that exists in a file of 'tests'."""
    replays = sidecar.get("replays")
    if replays is None:
        if sidecar["status"] == "refinement":
            return [f"{name}: a refinement model names the test functions that replay it"]
        return []
    if not isinstance(replays, list) or not all(
        isinstance(item, dict)
        and isinstance(item.get("file"), str)
        and isinstance(item.get("test"), str)
        for item in replays
    ):
        return [f"{name}: 'replays' is a list of {{file, test}}"]
    defects = []
    for item in replays:
        file, test = item["file"], item["test"]
        if file not in tests:
            defects.append(f"{name}: replay file {file} is not in 'tests'")
            continue
        path = REPOSITORY / file
        text = path.read_text(encoding="utf-8") if path.is_file() else ""
        if not re.search(rf"^\s*(?:pub\s+)?fn\s+{re.escape(test)}\s*\(", text, re.MULTILINE):
            defects.append(f"{name}: replay {test} is not a test of {file}")
    return defects


def check_directory(directory: Path) -> list[str]:
    try:
        sidecars = run_tlc.load_sidecars(directory)
    except run_tlc.SidecarError as refusal:
        return [f"{directory.name}/{refusal}"]
    defects = []
    claimed: set[str] = set()
    for sidecar in sidecars:
        path = sidecar["_path"]
        assert isinstance(path, Path)
        defects.extend(check_sidecar(sidecar))
        claimed.add(path.name)
        claimed.add(str(sidecar.get("module")))
        configs = sidecar.get("configs")
        if isinstance(configs, list):
            claimed.update(str(e.get("config")) for e in configs if isinstance(e, dict))
    present = {path.name for path in directory.iterdir() if path.suffix in MODEL_SUFFIXES}
    for name in sorted(present - claimed):
        defects.append(f"{directory.name}/{name}: belongs to no sidecar")
    for name in sorted(claimed - present):
        defects.append(f"{directory.name}/{name}: named by a sidecar, does not exist")
    return defects


def model_directories(root: Path) -> list[Path]:
    """Every directory that holds a model file, with or without a sidecar."""
    return sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and any(item.suffix in MODEL_SUFFIXES for item in path.iterdir())
    )


def main(root: Path = MODELS) -> int:
    directories = model_directories(root)
    if not directories:
        print(f"no model directory under {root}", file=sys.stderr)
        return 1
    defects = [defect for directory in directories for defect in check_directory(directory)]
    for defect in defects:
        print(defect, file=sys.stderr)
    if defects:
        return 1
    for directory in directories:
        sidecars = sorted(path.stem for path in directory.glob("*.json"))
        print(f"{directory.name}: {', '.join(sidecars)} consistent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
