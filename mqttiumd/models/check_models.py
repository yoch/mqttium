"""Check the model directories of mqttiumd without Java or TLC.

Run from ``mqttiumd/``::

    python models/check_models.py

Every directory under ``models/`` that holds a sidecar is checked. The
script exits with 1 and one line per defect when:

* a ``.tla``, ``.cfg`` or ``.json`` file belongs to no sidecar, or a sidecar
  names a file that does not exist;
* a sidecar is not well formed: module name, status, summary, issues, tests,
  configurations and their declared outcomes;
* a path listed in ``tests`` does not exist. The list holds only files that
  replay a counterexample of the model; a ``refinement`` model has at least
  one, a ``specification`` model may have none yet;
* a configuration does not check what its sidecar entry declares: the
  invariant of a ``violation``, the single ``PROPERTY`` of a temporal check,
  deadlock checking, a specification that states fairness, operators that
  the module defines;
* a property violated by a ``liveness`` configuration has no configuration
  that must satisfy it;
* the README of the directory has no correspondence table, or the table
  does not name every action of ``Next``, or a row does not say where the
  action is replayed.

It uses the standard library only. TLC itself is run by
``tools/formal/run_tlc.py --models-dir``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

MODELS = Path(__file__).resolve().parent
REPOSITORY = MODELS.parents[1]

EXPECTATIONS = frozenset({"pass", "violation", "deadlock", "liveness"})
# ``specification``: the actions encode the predicates of the specification
# and of the decision records; no implementation exists yet to refine.
# ``refinement``: the actions encode the predicates of the implementation,
# and deterministic tests replay the counterexamples.
STATUSES = frozenset({"specification", "refinement"})
MODEL_SUFFIXES = frozenset({".tla", ".cfg", ".json"})

_MODULE_HEADER = re.compile(r"^-{4,}\s*MODULE\s+(\w+)\s*-{4,}", re.MULTILINE)
_OPERATOR = r"^{name}\s*(?:\([^)]*\))?\s*=="
_DEFINITION = r"^{name}\s*==(.*?)(?=^\S|\Z)"
_CORRESPONDENCE = re.compile(r"^## Correspondence\b.*?$(.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL)


def _directive(text: str, name: str) -> list[str]:
    return re.findall(rf"^{name}\s+(\w+)", text, re.MULTILINE)


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
    config = str(entry.get("config"))
    where = f"{name}/{config}"
    expect = entry.get("expect")
    if expect not in EXPECTATIONS:
        return [f"{where}: unknown expectation {expect!r}"]
    defects = []
    if ("invariant" in entry) != (expect == "violation"):
        defects.append(f"{where}: 'invariant' is named by a violation and by nothing else")
    if "property" in entry and expect not in ("liveness", "pass"):
        defects.append(f"{where}: 'property' is named by a temporal check only")
    if expect == "liveness" and "property" not in entry:
        defects.append(f"{where}: a liveness violation names its property")
    path = directory / config
    if not path.is_file():
        return [*defects, f"{where}: the configuration file does not exist"]
    text = path.read_text(encoding="utf-8")
    specification = _directive(text, "SPECIFICATION")
    if len(specification) != 1:
        return [*defects, f"{where}: exactly one SPECIFICATION is required"]
    invariants = _directive(text, "INVARIANT")
    properties = _directive(text, "PROPERTY")
    if expect == "violation" and entry.get("invariant") not in invariants:
        defects.append(f"{where}: the declared invariant is not checked by the configuration")
    declared = [entry["property"]] if "property" in entry else []
    if properties != declared:
        defects.append(f"{where}: PROPERTY {properties} differs from the sidecar's {declared}")
    no_deadlock = re.search(r"^CHECK_DEADLOCK\s+FALSE", text, re.MULTILINE) is not None
    if expect == "liveness" and not no_deadlock:
        defects.append(f"{where}: a liveness violation sets CHECK_DEADLOCK FALSE")
    if expect == "pass" and no_deadlock:
        defects.append(f"{where}: a passing configuration keeps deadlock checking on")
    if properties:
        body = _definition(module_text, specification[0])
        if body is None or not re.search(r"\b[WS]F_|Fair", body):
            defects.append(f"{where}: {specification[0]} states no fairness")
    for operator in [*specification, *invariants, *properties, *_directive(text, "CONSTRAINT")]:
        if not _defines(module_text, operator):
            defects.append(f"{where}: {operator} is not defined in the module")
    return defects


def check_sidecar(path: Path) -> list[str]:
    name = path.stem
    try:
        sidecar = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return [f"{name}: unreadable sidecar: {error}"]
    if not isinstance(sidecar, dict):
        return [f"{name}: the sidecar is not an object"]
    defects = check_metadata(name, sidecar)
    configs = sidecar.get("configs")
    if not isinstance(configs, list) or not configs:
        return [*defects, f"{name}: no configuration"]
    if not all(isinstance(entry, dict) for entry in configs):
        return [*defects, f"{name}: a configuration entry is an object"]
    module = path.with_suffix(".tla")
    if not module.is_file():
        return [*defects, f"{name}: {module.name} does not exist"]
    module_text = module.read_text(encoding="utf-8")
    header = _MODULE_HEADER.search(module_text)
    if header is None or header.group(1) != name:
        defects.append(f"{name}: the module header does not name {name}")
    for entry in configs:
        defects.extend(check_config(name, entry, path.parent, module_text))
    violated = {e.get("property") for e in configs if e.get("expect") == "liveness"}
    satisfied = {e.get("property") for e in configs if e.get("expect") == "pass"}
    for missing in sorted(str(item) for item in violated - satisfied):
        defects.append(f"{name}: no configuration must satisfy the property {missing}")
    defects.extend(check_correspondence(path.parent, module_text))
    return defects


def check_metadata(name: str, sidecar: dict[str, object]) -> list[str]:
    defects = []
    if sidecar.get("module") != f"{name}.tla":
        defects.append(f"{name}: 'module' is {name}.tla")
    status = sidecar.get("status")
    if status not in STATUSES:
        defects.append(f"{name}: unknown status {status!r}")
    summary = sidecar.get("summary")
    if not isinstance(summary, str) or not summary.strip():
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
    return defects


def check_directory(directory: Path) -> list[str]:
    defects = []
    claimed: set[str] = set()
    for path in sorted(directory.glob("*.json")):
        defects.extend(check_sidecar(path))
        claimed.add(path.name)
        try:
            sidecar = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(sidecar, dict):
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
    return sorted(path for path in root.iterdir() if path.is_dir() and any(path.glob("*.json")))


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
