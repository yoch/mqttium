"""Model-check every formal model against its declared expected outcome.

Each ``<Model>.json`` sidecar of the models directory (``formal/models/``
unless ``--models-dir`` selects another one) names one TLA+ module, the issues
and executable tests it is tied to, and one or more TLC configurations with the
outcome TLC must produce:

* ``pass`` -- exhaustive check, no invariant violation and no deadlock;
* ``violation`` -- TLC must report exactly the named invariant as violated;
* ``deadlock`` -- TLC must report a reachable deadlock;
* ``liveness`` -- TLC must report that the configuration's one temporal
  ``PROPERTY``, named by the sidecar, is violated.

A ``violation`` expectation is how a model documents a defect: the
configuration describing the released behaviour must keep producing its
counterexample, and the configuration describing the repaired behaviour must
pass. A model that silently stops finding its counterexample is as much a
regression as one that starts failing.

TLC is fetched once from a pinned release and verified by SHA-256. It is a
maintainer tool only; mqttium keeps no runtime dependencies.

Usage::

    python tools/formal/run_tlc.py            # check every model
    python tools/formal/run_tlc.py Foo Bar    # check selected models
    python tools/formal/run_tlc.py --list     # print the model index
    python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1

A campaign is refused, before TLC is looked up, when the models directory
does not exist or holds no sidecar, when a selected model holds no
configuration, or when a sidecar entry cannot be checked. A campaign that ran
no configuration is never a success.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess  # nosec B404 - runs the pinned, hash-verified TLC jar only
import sys
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "formal" / "models"

TLA2TOOLS_VERSION = "1.7.4"
TLA2TOOLS_URL = (
    f"https://github.com/tlaplus/tlaplus/releases/download/v{TLA2TOOLS_VERSION}/tla2tools.jar"
)
TLA2TOOLS_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

EXPECTATIONS = frozenset({"pass", "violation", "deadlock", "liveness"})
STATUSES = frozenset({"refinement", "legacy-flag"})

_PASSED = "Model checking completed. No error has been found."
_INVARIANT = re.compile(r"^Error: Invariant (\w+) is violated", re.MULTILINE)
_DEADLOCK = "Error: Deadlock reached."
# TLC does not name the violated property, so a ``liveness`` configuration
# declares exactly one: the violation is then attributable to it.
_TEMPORAL = "Error: Temporal properties were violated."
_STATES = re.compile(r"^(\d+) states generated, (\d+) distinct states found", re.MULTILINE)


@dataclass(frozen=True, slots=True)
class Check:
    model: str
    module: Path
    config: Path
    expect: str
    invariant: str | None
    property: str | None = None


@dataclass(frozen=True, slots=True)
class Outcome:
    kind: str
    invariant: str | None
    distinct_states: int | None
    output: str


def load_sidecars(root: Path = MODELS) -> list[dict[str, object]]:
    sidecars = []
    for path in sorted(root.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        data["_path"] = path
        sidecars.append(data)
    return sidecars


def checks_for(sidecar: dict[str, object]) -> list[Check]:
    path = sidecar["_path"]
    assert isinstance(path, Path)
    module = path.parent / str(sidecar["module"])
    configs = sidecar["configs"]
    assert isinstance(configs, list)
    return [
        Check(
            model=path.stem,
            module=module,
            config=path.parent / entry["config"],
            expect=entry["expect"],
            invariant=entry.get("invariant"),
            property=entry.get("property"),
        )
        for entry in configs
    ]


class SidecarError(ValueError):
    """A sidecar entry that cannot be checked as written."""


def validated_checks(sidecar: dict[str, object]) -> list[Check]:
    """Return the checks of a sidecar, or refuse it.

    Only what would make a campaign run nothing, or accept an outcome nobody
    declared, is refused here. The well-formedness of the project's own
    sidecars stays with ``tests/project/test_formal_models.py``.
    """
    checks = checks_for(sidecar)
    path = sidecar["_path"]
    assert isinstance(path, Path)
    if not checks:
        raise SidecarError(f"{path.stem}: no configuration")
    for check in checks:
        where = f"{check.model}/{check.config.name}"
        if check.expect not in EXPECTATIONS:
            raise SidecarError(f"{where}: unknown expectation {check.expect!r}")
        if check.expect == "violation" and not check.invariant:
            raise SidecarError(f"{where}: a violation names no invariant")
        if check.expect == "liveness" and not check.property:
            raise SidecarError(f"{where}: a liveness violation names no property")
        for needed in (check.module, check.config):
            if not needed.is_file():
                raise SidecarError(f"{where}: {needed.name} does not exist")
    return checks


def _cache_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME")
    return (Path(base) if base else Path.home() / ".cache") / "mqttium" / "formal"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def tla2tools_jar() -> Path:
    """Return the verified pinned TLC jar, downloading it on first use."""
    override = os.environ.get("MQTTIUM_TLA2TOOLS_JAR")
    jar = Path(override) if override else _cache_dir() / f"tla2tools-{TLA2TOOLS_VERSION}.jar"
    if not jar.exists():
        if override:
            raise SystemExit(f"MQTTIUM_TLA2TOOLS_JAR does not exist: {jar}")
        jar.parent.mkdir(parents=True, exist_ok=True)
        partial = jar.with_suffix(".partial")
        with urllib.request.urlopen(TLA2TOOLS_URL, timeout=120) as response:  # nosec B310
            with partial.open("wb") as handle:
                shutil.copyfileobj(response, handle)
        partial.replace(jar)
    actual = _sha256(jar)
    if actual != TLA2TOOLS_SHA256:
        raise SystemExit(
            f"{jar} has SHA-256 {actual}; expected pinned {TLA2TOOLS_SHA256}. "
            "Delete it to download the pinned release again."
        )
    return jar


def classify(output: str) -> Outcome:
    states = _STATES.search(output)
    distinct = int(states.group(2)) if states else None
    invariant = _INVARIANT.search(output)
    if invariant:
        return Outcome("violation", invariant.group(1), distinct, output)
    if _DEADLOCK in output:
        return Outcome("deadlock", None, distinct, output)
    if _TEMPORAL in output:
        return Outcome("liveness", None, distinct, output)
    if _PASSED in output:
        return Outcome("pass", None, distinct, output)
    return Outcome("error", None, distinct, output)


def run_check(check: Check, jar: Path, java: str) -> Outcome:
    with tempfile.TemporaryDirectory(prefix="mqttium-tlc-") as metadir:
        command = [
            java,
            "-XX:+UseParallelGC",
            "-cp",
            str(jar),
            "tlc2.TLC",
            # One worker keeps breadth-first order deterministic: with several,
            # a deadlock one level below a violation can be reported first, so
            # the declared outcome would depend on thread scheduling.
            "-workers",
            "1",
            "-metadir",
            metadir,
            "-config",
            str(check.config),
            str(check.module),
        ]
        completed = subprocess.run(  # nosec B603 - fixed argv, no shell
            command,
            cwd=check.module.parent,
            capture_output=True,
            text=True,
            check=False,
            timeout=900,
        )
    return classify(completed.stdout + completed.stderr)


def matches(check: Check, outcome: Outcome) -> bool:
    if outcome.kind != check.expect:
        return False
    return check.expect != "violation" or outcome.invariant == check.invariant


def _describe(outcome: Outcome) -> str:
    detail = outcome.kind
    if outcome.invariant:
        detail += f" {outcome.invariant}"
    if outcome.distinct_states is not None:
        detail += f", {outcome.distinct_states} distinct states"
    return detail


def _expected(check: Check) -> str:
    named = check.invariant or check.property
    return check.expect + (f" {named}" if named else "")


def print_index(sidecars: list[dict[str, object]]) -> None:
    for sidecar in sidecars:
        path = sidecar["_path"]
        assert isinstance(path, Path)
        issues = ", ".join(f"#{number}" for number in sidecar.get("issues", []))  # type: ignore[union-attr]
        print(f"{path.stem} [{sidecar['status']}] {issues}")
        print(f"    {sidecar['summary']}")
        for check in checks_for(sidecar):
            print(f"    {check.config.name}: expect {_expected(check)}")


def run_campaign(checks: list[Check], jar: Path, java: str, *, verbose: bool) -> int:
    """Run the checks; succeed only if every one gave its declared outcome."""
    failures = 0
    for check in checks:
        outcome = run_check(check, jar, java)
        ok = matches(check, outcome)
        failures += not ok
        expected = _expected(check)
        status = "ok  " if ok else "FAIL"
        print(
            f"{status} {check.model}/{check.config.name}: "
            f"expected {expected}, got {_describe(outcome)}"
        )
        if not ok and (verbose or outcome.kind == "error"):
            print(outcome.output, file=sys.stderr)
    if not checks:
        print("no configuration was run", file=sys.stderr)
        return 1
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("models", nargs="*", help="model names (default: all)")
    parser.add_argument("--list", action="store_true", help="print the model index and exit")
    parser.add_argument("--verbose", action="store_true", help="print TLC output on mismatch")
    parser.add_argument(
        "--models-dir",
        type=Path,
        default=MODELS,
        metavar="DIR",
        help="directory holding the sidecars (default: formal/models)",
    )
    args = parser.parse_args(argv)

    # Resolved: TLC runs from the module's directory, so a relative path
    # would no longer name the files once the working directory changed.
    root = args.models_dir.resolve()
    if not root.is_dir():
        parser.error(f"--models-dir is not a directory: {args.models_dir}")
    sidecars = load_sidecars(root)
    if not sidecars:
        parser.error(f"no model sidecar (*.json) in {root}")
    if args.models:
        wanted = set(args.models)
        sidecars = [s for s in sidecars if s["_path"].stem in wanted]  # type: ignore[union-attr]
        missing = wanted - {s["_path"].stem for s in sidecars}  # type: ignore[union-attr]
        if missing:
            parser.error(f"unknown models: {', '.join(sorted(missing))}")
    try:
        checks = [check for sidecar in sidecars for check in validated_checks(sidecar)]
    except SidecarError as refusal:
        parser.error(str(refusal))
    if args.list:
        print_index(sidecars)
        return 0

    java = shutil.which("java")
    if java is None:
        print("java is required to run TLC", file=sys.stderr)
        return 2
    return run_campaign(checks, tla2tools_jar(), java, verbose=args.verbose)


if __name__ == "__main__":
    raise SystemExit(main())
