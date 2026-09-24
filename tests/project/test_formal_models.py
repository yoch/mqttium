"""Keep formal model sidecars consistent without requiring Java or TLC."""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "formal" / "models"
_SPEC = importlib.util.spec_from_file_location("run_tlc", ROOT / "tools" / "formal" / "run_tlc.py")
assert _SPEC is not None and _SPEC.loader is not None
run_tlc = importlib.util.module_from_spec(_SPEC)
sys.modules["run_tlc"] = run_tlc
_SPEC.loader.exec_module(run_tlc)

_MODULE_HEADER = re.compile(r"^-{4,}\s*MODULE\s+(\w+)\s*-{4,}", re.MULTILINE)
_OPERATOR = r"^{name}\s*(?:\([^)]*\))?\s*=="


def _sidecars() -> list[dict[str, object]]:
    return run_tlc.load_sidecars(MODELS)


def test_every_model_file_belongs_to_a_sidecar() -> None:
    claimed: set[str] = set()
    for sidecar in _sidecars():
        claimed.add(str(sidecar["module"]))
        claimed.add(sidecar["_path"].name)  # type: ignore[union-attr]
        claimed.update(entry["config"] for entry in sidecar["configs"])  # type: ignore[union-attr]
    present = {path.name for path in MODELS.iterdir() if path.suffix in {".tla", ".cfg", ".json"}}
    assert present - claimed == set(), "unreferenced formal model files"
    assert claimed - present == set(), "sidecars reference missing files"


def test_sidecars_are_well_formed() -> None:
    sidecars = _sidecars()
    assert sidecars, "no formal models found"
    for sidecar in sidecars:
        name = sidecar["_path"].stem  # type: ignore[union-attr]
        assert sidecar["module"] == f"{name}.tla", name
        assert sidecar["status"] in run_tlc.STATUSES, name
        assert isinstance(sidecar["summary"], str) and sidecar["summary"].strip(), name
        issues = sidecar["issues"]
        assert isinstance(issues, list) and issues, name
        assert all(isinstance(number, int) and number > 0 for number in issues), name
        tests = sidecar["tests"]
        assert isinstance(tests, list), name
        for test in tests:
            assert (ROOT / test).is_file(), f"{name}: missing test {test}"
        if sidecar["status"] == "refinement":
            assert tests, f"{name}: a refinement model names its executable tests"
        configs = sidecar["configs"]
        assert isinstance(configs, list) and configs, name
        for entry in configs:
            assert entry["expect"] in run_tlc.EXPECTATIONS, name
            assert ("invariant" in entry) == (entry["expect"] == "violation"), name


def test_modules_and_configs_are_runnable_by_tlc() -> None:
    for sidecar in _sidecars():
        for check in run_tlc.checks_for(sidecar):
            module_text = check.module.read_text(encoding="utf-8")
            header = _MODULE_HEADER.search(module_text)
            assert header and header.group(1) == check.module.stem, check.module.name
            config_text = check.config.read_text(encoding="utf-8")
            # TLC refuses a configuration without a behaviour specification.
            assert re.search(r"^SPECIFICATION\s+\w+", config_text, re.MULTILINE), check.config.name
            named = re.findall(r"^(?:INVARIANT|CONSTRAINT)\s+(\w+)", config_text, re.MULTILINE)
            if check.invariant is not None:
                assert check.invariant in named, check.config.name
            for operator in named:
                assert re.search(_OPERATOR.format(name=operator), module_text, re.MULTILINE), (
                    f"{check.config.name}: {operator} is not defined in {check.module.name}"
                )


def test_tlc_outcome_classification() -> None:
    passed = run_tlc.classify(
        "7 states generated, 5 distinct states found, 0 states left on queue.\n"
        "Model checking completed. No error has been found.\n"
    )
    assert (passed.kind, passed.distinct_states) == ("pass", 5)
    violated = run_tlc.classify("Error: Invariant NoStaleCommit is violated.\n")
    assert (violated.kind, violated.invariant) == ("violation", "NoStaleCommit")
    assert run_tlc.classify("Error: Deadlock reached.\n").kind == "deadlock"
    assert run_tlc.classify("Error: The configuration file did not specify").kind == "error"
    check = run_tlc.Check("M", MODELS / "M.tla", MODELS / "M.cfg", "violation", "Inv")
    assert run_tlc.matches(check, violated) is False
    assert run_tlc.matches(check, run_tlc.Outcome("violation", "Inv", 1, "")) is True
