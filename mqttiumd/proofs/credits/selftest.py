#!/usr/bin/env python3
"""Discriminating tests of the proof scripts: what a PASS is worth.

    python3 proofs/credits/selftest.py [--only NAME]

Each case copies the sources to a temporary directory, breaks one thing
the scripts claim to check, runs the script of the copy and expects it to
refuse, by the exit code of that very check. The production tree is never
touched. The last case runs the unmodified copy and expects a PASS: a
script that refuses everything would pass the others.

Exit codes of `run.sh` that the cases expect:

    2  unknown argument
    5  a recorded digest differs from the sources
    6  the harnesses Kani ran are not the listed ones
    7  a harness was not verified with all its witnesses
    9  the function allocated

Exit code of this script: 0 when every case gives its expected verdict.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRODUCT = HERE.parent.parent
CREDITS = Path("crates/mqttiumd-core/src/credits.rs")
CAPACITY = Path("crates/mqttiumd-contract/src/capacity.rs")
HARNESSES = Path("proofs/credits/src/lib.rs")
RECORD = Path("proofs/credits/BODY.sha256")


def copy_sources(destination: Path) -> None:
    skip = shutil.ignore_patterns("target", "runs", "__pycache__")
    for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
        shutil.copy2(PRODUCT / name, destination / name)
    for name in ("crates", "proofs", "tools"):
        shutil.copytree(PRODUCT / name, destination / name, ignore=skip)


def edit(copy: Path, file: Path, find: str, replace: str) -> None:
    path = copy / file
    text = path.read_text()
    if text.count(find) != 1:
        sys.exit(f"selftest: `{find.strip()}` occurs {text.count(find)} times in {file}")
    path.write_text(text.replace(find, replace))


def run(copy: Path, *command: str, env: dict[str, str] | None = None) -> tuple[int, str]:
    done = subprocess.run(
        command, cwd=copy, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        check=False, env={**os.environ, **(env or {})},
    )
    return done.returncode, done.stdout


RUN = ("proofs/credits/run.sh",)
BODY_START = "\n) -> Result<ReleaseOutcome, ReleaseError> {\n"
END_OF_HARNESSES = "\n}\n"


def case_allocation_through_a_contract_type(copy: Path) -> list[str]:
    """The function allocates through a type no grep for `Vec` sees."""
    edit(copy, CREDITS, BODY_START,
         BODY_START
         + "    let _probe = crate::contract::capacity::BoundedVec::<u8>::with_capacity(1);\n")
    code, output = run(copy, *RUN, "--record")
    return expect_code(code, 9, output)


def case_allocation_through_a_method(copy: Path) -> list[str]:
    """The function allocates with `.to_vec()`, which names no type."""
    edit(copy, CREDITS, BODY_START, BODY_START + "    let _probe = [0u8; 4].to_vec();\n")
    code, output = run(copy, *RUN, "--record")
    return expect_code(code, 9, output)


def append_harness(copy: Path, text: str) -> None:
    path = copy / HARNESSES
    source = path.read_text()
    if not source.endswith(END_OF_HARNESSES):
        sys.exit("selftest: the harness file does not end with the module's brace")
    path.write_text(source[: -len(END_OF_HARNESSES)] + "\n" + text + END_OF_HARNESSES)


def case_unlisted_harness_with_a_second_attribute(copy: Path) -> list[str]:
    """A harness the list does not name, behind a second attribute."""
    append_harness(copy, "    #[kani::proof]\n    #[kani::unwind(2)]\n"
                         "    fn p99_unlisted() {\n        assert!(kani::any::<u8>() < 255);\n    }\n")
    code, output = run(copy, *RUN, "--record")
    return expect_code(code, 6, output)


def case_unlisted_public_harness(copy: Path) -> list[str]:
    """A harness the list does not name, declared `pub fn`."""
    append_harness(copy, "    #[kani::proof]\n"
                         "    pub fn p99_unlisted() {\n        assert!(kani::any::<u8>() < 255);\n    }\n")
    code, output = run(copy, *RUN, "--record")
    return expect_code(code, 6, output)


def case_listed_harness_missing(copy: Path) -> list[str]:
    """A harness the list names and the source no longer holds."""
    edit(copy, HARNESSES, "    #[kani::proof]\n    fn p10_coherence_is_preserved() {",
         "    #[allow(dead_code)]\n    fn p10_coherence_is_preserved() {")
    code, output = run(copy, *RUN, "--record")
    return expect_code(code, 6, output)


def case_contract_function_changed(copy: Path) -> list[str]:
    """`checked_sub`, which the body calls, changes in the contract crate."""
    edit(copy, CAPACITY, "            /// Difference, or `None` when `other` exceeds `self`.\n",
         "            /// Difference, or `None` when `other` exceeds `self`. Changed.\n")
    code, output = run(copy, *RUN)
    return expect_code(code, 5, output)


def case_assertion_weakened(copy: Path) -> list[str]:
    """An assertion of a harness is weakened; the proof still passes."""
    edit(copy, HARNESSES, 'assert!(!after.held, "P6d: the lease is released");',
         'assert!(!after.held || after.held, "P6d: the lease is released");')
    code, output = run(copy, *RUN)
    return expect_code(code, 5, output)


def case_unknown_argument(copy: Path) -> list[str]:
    """An argument the script does not know is not ignored."""
    code, output = run(copy, *RUN, "--recrod")
    return expect_code(code, 2, output)


def case_record_after_a_failed_harness(copy: Path) -> list[str]:
    """`--record` on a body that is not proved records nothing."""
    before = (copy / RECORD).read_text()
    edit(copy, CREDITS, "    lease.state = LeaseState::Released;\n    Ok(ReleaseOutcome::Released)\n",
         "    Ok(ReleaseOutcome::Released)\n")
    code, output = run(copy, *RUN, "--record")
    problems = expect_code(code, 7, output)
    if (copy / RECORD).read_text() != before:
        problems.append("BODY.sha256 was rewritten for a body that is not proved")
    return problems


def case_mutant_not_found_is_reported(copy: Path) -> list[str]:
    """A mutant whose text is gone has its row, and the report its verdict."""
    edit(copy, CREDITS, "    if key != lease.key {\n", "    if lease.key != key {\n")
    run(copy, *RUN, "--record")
    report = copy / "report.md"
    code, output = run(copy, "python3", "proofs/credits/mutants.py",
                       "--only", "generation-ignored", "--report", str(report))
    problems = []
    if code != 1:
        problems.append(f"expected exit 1, got {code}")
    text = report.read_text() if report.exists() else ""
    if "`generation-ignored`" not in text or "inconclusive" not in text.lower():
        problems.append("the report has no inconclusive row for the mutant")
    if "mutants: FAIL" not in text:
        problems.append("the report does not carry the verdict")
    return problems + ([output[-1500:]] if problems else [])


def case_timeout_leaves_no_process(copy: Path) -> list[str]:
    """A run that exceeds its time limit leaves no tool running behind it."""
    report = copy / "report.md"
    code, output = run(copy, "python3", "proofs/credits/mutants.py", "--report", str(report),
                       env={"MUTANTS_TIME_LIMIT_SECONDS": "3"})
    problems = []
    if code != 1 or "timed out" not in output:
        problems.append(f"expected exit 1 and a timeout, got exit {code}")
    time.sleep(1)
    listing = subprocess.run(["ps", "-eo", "pid,args"], stdout=subprocess.PIPE, text=True,
                             check=False).stdout
    left = [line for line in listing.splitlines()
            if "mqttiumd-mutants-" in line and "selftest" not in line]
    if left:
        problems.append("processes still run in the mutants' directory: " + "; ".join(left)[:400])
    text = report.read_text() if report.exists() else ""
    if "mutants: FAIL" not in text:
        problems.append("the report does not carry the verdict")
    return problems + ([output[-1500:]] if problems else [])


def case_unmodified_copy_passes(copy: Path) -> list[str]:
    """The unmodified copy passes: the script does not refuse everything."""
    code, output = run(copy, *RUN)
    return expect_code(code, 0, output)


def expect_code(code: int, expected: int, output: str) -> list[str]:
    if code == expected:
        return []
    return [f"expected exit {expected}, got {code}", output[-1500:]]


CASES = [
    case_allocation_through_a_contract_type,
    case_allocation_through_a_method,
    case_unlisted_harness_with_a_second_attribute,
    case_unlisted_public_harness,
    case_listed_harness_missing,
    case_contract_function_changed,
    case_assertion_weakened,
    case_unknown_argument,
    case_record_after_a_failed_harness,
    case_mutant_not_found_is_reported,
    case_timeout_leaves_no_process,
    case_unmodified_copy_passes,
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", help="run this case only (name without `case_`)")
    arguments = parser.parse_args()
    cases = [c for c in CASES if arguments.only in (None, c.__name__.removeprefix("case_"))]
    if not cases:
        print(f"selftest: no case is named {arguments.only}", file=sys.stderr)
        return 1

    failed = 0
    for case in cases:
        name = case.__name__.removeprefix("case_")
        with tempfile.TemporaryDirectory(prefix="mqttiumd-selftest-") as directory:
            copy = Path(directory)
            copy_sources(copy)
            problems = case(copy)
        if problems:
            failed += 1
            print(f"FAIL  {name}: {case.__doc__}")
            for problem in problems:
                for line in problem.splitlines():
                    print(f"        {line}")
        else:
            print(f"ok    {name}: {case.__doc__}")
    print(f"selftest: {len(cases) - failed} of {len(cases)} cases as expected")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
