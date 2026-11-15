"""Keep formal model sidecars consistent without requiring Java or TLC."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

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
            # A temporal property is named by the configuration expected to
            # violate it and by the repaired one expected to satisfy it.
            assert "property" not in entry or entry["expect"] in ("liveness", "pass"), name
            assert "property" in entry or entry["expect"] != "liveness", name
        violated = {e["property"] for e in configs if e["expect"] == "liveness"}
        satisfied = {e["property"] for e in configs if e["expect"] == "pass" and "property" in e}
        assert violated <= satisfied, (
            f"{name}: no repaired configuration checks {violated - satisfied}"
        )


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
            properties = re.findall(r"^PROPERTY\s+(\w+)", config_text, re.MULTILINE)
            # What the configuration checks is what its sidecar entry names:
            # a repaired configuration that lost its PROPERTY would otherwise
            # still "pass", having checked nothing temporal.
            assert properties == ([check.property] if check.property else []), check.config.name
            if check.expect == "pass" and check.property:
                # Deadlock checking stays on where the property must hold.
                assert not re.search(r"^CHECK_DEADLOCK\s+FALSE", config_text, re.MULTILINE), (
                    check.config.name
                )
            if check.expect == "liveness":
                # TLC does not say which temporal property failed: with exactly
                # one declared, the reported violation can only be that one.
                assert properties == [check.property], check.config.name
                # A stuck state must surface as the temporal violation, not as
                # the deadlock TLC would otherwise report first.
                assert re.search(r"^CHECK_DEADLOCK\s+FALSE", config_text, re.MULTILINE), (
                    check.config.name
                )
            if properties:
                # A temporal property means nothing without a fairness condition.
                specification = re.search(r"^SPECIFICATION\s+(\w+)", config_text, re.MULTILINE)
                assert specification is not None
                definition = re.search(
                    rf"^{specification.group(1)}\s*==(.*?)(?=^\S|\Z)",
                    module_text,
                    re.MULTILINE | re.DOTALL,
                )
                assert definition is not None, check.config.name
                assert re.search(r"\b[WS]F_|Fairness", definition.group(1)), (
                    f"{check.config.name}: {specification.group(1)} states no fairness"
                )
            named = named + properties
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
    stuck = run_tlc.classify(
        "Error: Temporal properties were violated.\n"
        "Error: The following behavior constitutes a counter-example:\n"
    )
    assert stuck.kind == "liveness"
    liveness = run_tlc.Check("M", MODELS / "M.tla", MODELS / "M.cfg", "liveness", None, "Ends")
    assert run_tlc.matches(liveness, stuck) is True
    assert run_tlc.matches(liveness, passed) is False
    assert run_tlc.classify("Error: The configuration file did not specify").kind == "error"
    check = run_tlc.Check("M", MODELS / "M.tla", MODELS / "M.cfg", "violation", "Inv")
    assert run_tlc.matches(check, violated) is False
    assert run_tlc.matches(check, run_tlc.Outcome("violation", "Inv", 1, "")) is True


# --- Selection of another models directory (--models-dir) -------------------


def _write_model(root: Path, name: str, configs: list[dict[str, str]]) -> None:
    """Write a sidecar with the module and configuration files it names."""
    (root / f"{name}.tla").write_text(f"---- MODULE {name} ----\n====\n", encoding="utf-8")
    for entry in configs:
        (root / entry["config"]).write_text("SPECIFICATION Spec\n", encoding="utf-8")
    sidecar = {
        "module": f"{name}.tla",
        "status": "refinement",
        "summary": "A model of another directory.",
        "issues": [1],
        "tests": [],
        "configs": configs,
    }
    (root / f"{name}.json").write_text(json.dumps(sidecar), encoding="utf-8")


def _no_tlc(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail the test if Java or the jar is looked up: a refusal comes first."""

    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the campaign was refused too late")

    monkeypatch.setattr(run_tlc.shutil, "which", unexpected)
    monkeypatch.setattr(run_tlc, "tla2tools_jar", unexpected)


def _stub_tlc(monkeypatch: pytest.MonkeyPatch, outcome: object) -> list[object]:
    """Replace TLC by a fixed outcome; return the checks that were run."""
    ran: list[object] = []

    def run_check(check: object, _jar: object, _java: object) -> object:
        ran.append(check)
        return outcome

    monkeypatch.setattr(run_tlc.shutil, "which", lambda _name: "java")
    monkeypatch.setattr(run_tlc, "tla2tools_jar", lambda: Path("tla2tools.jar"))
    monkeypatch.setattr(run_tlc, "run_check", run_check)
    return ran


def test_default_models_directory_is_the_client_one(capsys: pytest.CaptureFixture[str]) -> None:
    assert run_tlc.main(["--list"]) == 0
    default = capsys.readouterr().out
    assert run_tlc.main(["--models-dir", str(MODELS), "--list"]) == 0
    assert capsys.readouterr().out == default
    listed = set(re.findall(r"^(\w+) \[", default, re.MULTILINE))
    assert listed == {path.stem for path in MODELS.glob("*.json")}


def test_models_dir_selects_that_directory_and_no_other(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    models = tmp_path / "models"
    models.mkdir()
    _write_model(models, "Elsewhere", [{"config": "Elsewhere.cfg", "expect": "pass"}])
    assert run_tlc.main(["--models-dir", str(models), "--list"]) == 0
    listed = set(re.findall(r"^(\w+) \[", capsys.readouterr().out, re.MULTILINE))
    assert listed == {"Elsewhere"}

    # A client model is unknown there, and the other way round.
    client_model = sorted(path.stem for path in MODELS.glob("*.json"))[0]
    for argv in (["--models-dir", str(models), client_model], ["Elsewhere"]):
        with pytest.raises(SystemExit) as refused:
            run_tlc.main([*argv, "--list"])
        assert refused.value.code == 2

    # A relative directory is resolved: TLC runs from the module's directory.
    monkeypatch.chdir(tmp_path)
    ran = _stub_tlc(monkeypatch, run_tlc.Outcome("pass", None, 1, ""))
    assert run_tlc.main(["--models-dir", "models"]) == 0
    assert [(check.model, check.module, check.config) for check in ran] == [  # type: ignore[attr-defined]
        ("Elsewhere", models.resolve() / "Elsewhere.tla", models.resolve() / "Elsewhere.cfg")
    ]


def test_missing_or_empty_models_directory_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _no_tlc(monkeypatch)
    empty = tmp_path / "empty"
    empty.mkdir()
    a_file = tmp_path / "file"
    a_file.write_text("", encoding="utf-8")
    reasons = {tmp_path / "absent": "not a directory", a_file: "not a directory", empty: "no model"}
    for directory, reason in reasons.items():
        for mode in ([], ["--list"]):
            with pytest.raises(SystemExit) as refused:
                run_tlc.main(["--models-dir", str(directory), *mode])
            assert refused.value.code == 2, directory
            assert reason in capsys.readouterr().err, directory


def test_model_without_configuration_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _no_tlc(monkeypatch)
    _write_model(tmp_path, "Full", [{"config": "Full.cfg", "expect": "pass"}])
    _write_model(tmp_path, "Hollow", [])
    # One model that would run does not excuse one that runs nothing.
    for selection in ([], ["Hollow"], ["Full", "Hollow"], ["--list"], ["Hollow", "--list"]):
        with pytest.raises(SystemExit) as refused:
            run_tlc.main(["--models-dir", str(tmp_path), *selection])
        assert refused.value.code == 2, selection
        captured = capsys.readouterr()
        assert "Hollow: no configuration" in captured.err, selection
        assert captured.out == "", selection
    # The model that holds one is still selectable on its own.
    assert run_tlc.main(["--models-dir", str(tmp_path), "Full", "--list"]) == 0


@pytest.mark.parametrize(
    ("entry", "remove", "reason"),
    [
        ({"config": "Bad.cfg", "expect": "violation"}, None, "names no invariant"),
        ({"config": "Bad.cfg", "expect": "liveness"}, None, "names no property"),
        ({"config": "Bad.cfg", "expect": "maybe"}, None, "unknown expectation 'maybe'"),
        ({"config": "Bad.cfg", "expect": "pass"}, "Bad.cfg", "Bad.cfg does not exist"),
        ({"config": "Bad.cfg", "expect": "pass"}, "Bad.tla", "Bad.tla does not exist"),
        ({"config": "Bad.cfg"}, None, "names no 'expect'"),
        ({"expect": "pass"}, None, "names no 'config'"),
    ],
    ids=[
        "violation-unnamed",
        "liveness-unnamed",
        "unknown-expect",
        "no-config",
        "no-module",
        "expect-missing",
        "config-missing",
    ],
)
def test_unusable_sidecar_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    entry: dict[str, str],
    remove: str | None,
    reason: str,
) -> None:
    _no_tlc(monkeypatch)
    _write_model(tmp_path, "Bad", [{"config": "Bad.cfg", **entry}])
    (tmp_path / "Bad.json").write_text(
        json.dumps(
            {"module": "Bad.tla", "status": "refinement", "summary": "Bad.", "configs": [entry]}
        ),
        encoding="utf-8",
    )
    if remove is not None:
        (tmp_path / remove).unlink()
    # The index is the Java-free check of a directory: it refuses what a
    # campaign refuses, and prints nothing as an index.
    for mode in ([], ["--list"]):
        with pytest.raises(SystemExit) as refused:
            run_tlc.main(["--models-dir", str(tmp_path), *mode])
        assert refused.value.code == 2, mode
        captured = capsys.readouterr()
        assert reason in captured.err, mode
        assert captured.out == "", mode


@pytest.mark.parametrize("missing", ["module", "configs"])
def test_sidecar_without_a_required_key_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    missing: str,
) -> None:
    _no_tlc(monkeypatch)
    _write_model(tmp_path, "Bad", [{"config": "Bad.cfg", "expect": "pass"}])
    sidecar = json.loads((tmp_path / "Bad.json").read_text(encoding="utf-8"))
    del sidecar[missing]
    (tmp_path / "Bad.json").write_text(json.dumps(sidecar), encoding="utf-8")
    for mode in ([], ["--list"]):
        with pytest.raises(SystemExit) as refused:
            run_tlc.main(["--models-dir", str(tmp_path), *mode])
        assert refused.value.code == 2, mode
        captured = capsys.readouterr()
        assert f"Bad: names no '{missing}'" in captured.err, mode
        assert captured.out == "", mode


@pytest.mark.parametrize(
    "outcome",
    [
        run_tlc.Outcome("violation", "AnotherInvariant", 3, ""),
        run_tlc.Outcome("deadlock", None, 3, ""),
        run_tlc.Outcome("liveness", None, 3, ""),
        run_tlc.Outcome("pass", None, 3, ""),
        run_tlc.Outcome("error", None, None, ""),
    ],
    ids=lambda outcome: outcome.kind,
)
def test_violation_fails_unless_tlc_names_the_declared_invariant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    outcome: object,
) -> None:
    entry = {"config": "Model.cfg", "expect": "violation", "invariant": "DeclaredInvariant"}
    _write_model(tmp_path, "Model", [entry])
    assert len(_stub_tlc(monkeypatch, outcome)) == 0
    assert run_tlc.main(["--models-dir", str(tmp_path)]) == 1
    # The failure names the exact invariant that was expected.
    assert "FAIL Model/Model.cfg: expected violation DeclaredInvariant, got " in (
        capsys.readouterr().out
    )

    _stub_tlc(monkeypatch, run_tlc.Outcome("violation", "DeclaredInvariant", 3, ""))
    assert run_tlc.main(["--models-dir", str(tmp_path)]) == 0
    assert "ok   Model/Model.cfg: expected violation DeclaredInvariant, got " in (
        capsys.readouterr().out
    )


def test_campaign_that_ran_no_configuration_is_not_a_success() -> None:
    assert run_tlc.run_campaign([], Path("tla2tools.jar"), "java", verbose=False) != 0


# --- Sidecars the runner cannot read, and configurations that do not end ----


def _refused(
    argv: list[str], capsys: pytest.CaptureFixture[str], reason: str, *, index: bool = True
) -> None:
    """The campaign, and the index, refuse with exit 2, the reason and no index."""
    for mode in ([], ["--list"]) if index else ([],):
        with pytest.raises(SystemExit) as refused:
            run_tlc.main([*argv, *mode])
        assert refused.value.code == 2, mode
        captured = capsys.readouterr()
        assert reason in captured.err, (mode, captured.err)
        assert captured.out == "", mode


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"status": None}, "Bad: names no 'status'"),
        ({"summary": None}, "Bad: names no 'summary'"),
        ({"status": 3}, "Bad: 'status' is not a string"),
        ({"summary": ["a"]}, "Bad: 'summary' is not a string"),
        ({"module": 5}, "Bad: 'module' is not a string"),
        ({"issues": "647"}, "Bad: 'issues' is not a list"),
        ({"configs": {"config": "Bad.cfg"}}, "Bad: 'configs' is not a list of entries"),
        ({"configs": ["Bad.cfg"]}, "Bad: 'configs' is not a list of entries"),
        ({"configs": [{"config": 5, "expect": "pass"}]}, "Bad: 'config' is not a string"),
        ({"configs": [{"config": "Bad.cfg", "expect": ["pass"]}]}, "'expect' is not a string"),
        (
            {"configs": [{"config": "Bad.cfg", "expect": "violation", "invariant": 1}]},
            "'invariant' is not a string",
        ),
        (
            {"configs": [{"config": "Bad.cfg", "expect": "liveness", "property": ["P"]}]},
            "'property' is not a string",
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_sidecar_with_a_wrong_value_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    change: dict[str, object],
    reason: str,
) -> None:
    _no_tlc(monkeypatch)
    _write_model(tmp_path, "Bad", [{"config": "Bad.cfg", "expect": "pass"}])
    sidecar = json.loads((tmp_path / "Bad.json").read_text(encoding="utf-8"))
    for key, value in change.items():
        if value is None:
            del sidecar[key]
        else:
            sidecar[key] = value
    (tmp_path / "Bad.json").write_text(json.dumps(sidecar), encoding="utf-8")
    _refused(["--models-dir", str(tmp_path)], capsys, reason)


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("[]", "Stray.json: a sidecar is an object"),
        ('"text"', "Stray.json: a sidecar is an object"),
        ("{not json", "Stray.json: not JSON"),
    ],
    ids=["list", "string", "malformed"],
)
def test_directory_with_an_unreadable_sidecar_is_refused_as_a_whole(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    text: str,
    reason: str,
) -> None:
    _no_tlc(monkeypatch)
    _write_model(tmp_path, "Good", [{"config": "Good.cfg", "expect": "pass"}])
    (tmp_path / "Stray.json").write_text(text, encoding="utf-8")
    # Also when only the sound model is selected: a directory that holds a
    # file nobody can read is not a directory whose campaign means something.
    _refused(["--models-dir", str(tmp_path)], capsys, reason)
    _refused(["--models-dir", str(tmp_path), "Good"], capsys, reason)


def test_configuration_that_does_not_end_is_a_failed_check_and_the_campaign_goes_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    configs = [
        {"config": "Slow.cfg", "expect": "pass"},
        {"config": "Quick.cfg", "expect": "pass"},
    ]
    _write_model(tmp_path, "Model", configs)

    def run(command: list[str], **_kwargs: object) -> object:
        if "Slow.cfg" in command[-2]:
            raise run_tlc.subprocess.TimeoutExpired(command, 900, output="partial", stderr="")
        passed = "Model checking completed. No error has been found.\n"
        return run_tlc.subprocess.CompletedProcess(command, 0, stdout=passed, stderr="")

    monkeypatch.setattr(run_tlc.shutil, "which", lambda _name: "java")
    monkeypatch.setattr(run_tlc, "tla2tools_jar", lambda: Path("tla2tools.jar"))
    monkeypatch.setattr(run_tlc.subprocess, "run", run)
    assert run_tlc.main(["--models-dir", str(tmp_path)]) == 1
    lines = capsys.readouterr().out.splitlines()
    assert lines == [
        "FAIL Model/Slow.cfg: expected pass, got timeout",
        "ok   Model/Quick.cfg: expected pass, got pass",
    ]
    # An interrupted exploration never matches, whatever was declared.
    interrupted = run_tlc.Outcome("timeout", None, None, "")
    for expect in sorted(run_tlc.EXPECTATIONS):
        check = run_tlc.Check("M", tmp_path / "M.tla", tmp_path / "M.cfg", expect, "Inv", "P")
        assert run_tlc.matches(check, interrupted) is False
