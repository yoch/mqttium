#!/usr/bin/env python3
"""Mutants of release_if_current, each caught for its own reason (DEM-021).

    python3 proofs/credits/mutants.py [--only NAME] [--report FILE]

The production tree is never touched. The sources (the product workspace
and the proof workspace, without build output) are copied to a temporary
directory; each mutant rewrites `crates/mqttiumd-core/src/credits.rs` of
that copy, and the same proof crate and the same unit tests are run on it.

A mutant counts as caught only when all of this holds:

- its text was found exactly once in the function, so it was applied;
- the mutated crate still builds with the product toolchain: a compilation
  failure is not a detection;
- every harness was built and run by the tool to its verdict, within the
  time limit: an error or a timeout is not a detection. On a timeout the
  whole process group of the tool is killed;
- the checks the mutant aims at are among the failed checks, and the
  checks it must leave alone still hold: the failure shows the violation
  of the property aimed at, not a side effect;
- the unit test named for it fails.

Before any mutant, the unmodified copy is run as a control: every harness
verified, every unit test passing. Without it a failure could not be
attributed to the mutation.

Every mutant selected has its line in the output and in the report,
caught or not: a mutant that could not be evaluated is `inconclusive`,
with the reason. The report ends with the verdict.

The time limit of each command is 900 seconds;
`MUTANTS_TIME_LIMIT_SECONDS` changes it, for the self-test.

Exit code: 0 when the control passes and every mutant is caught by the
proof and by its unit test; 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

import prooflib
from prooflib import CREDITS, MUTANTS, PACKAGE, Mutant

TIME_LIMIT_SECONDS = float(os.environ.get("MUTANTS_TIME_LIMIT_SECONDS", "900"))
HEADER = [
    "| Mutant | What it does | Checks aimed at | Every failed check | Unit tests that fail | Verdict |",
    "| - | - | - | - | - | - |",
]


def run(command: list[str], cwd: Path) -> tuple[int | None, str, float]:
    return prooflib.run_bounded(command, cwd, TIME_LIMIT_SECONDS)


def parse_tests(output: str) -> tuple[list[str], list[str]] | None:
    """Passed and failed unit tests of the credits module, or None when the
    test binary did not report."""
    if not re.search(r"^test result: ", output, flags=re.M):
        return None
    passed = re.findall(r"^test credits::tests::(\w+) \.\.\. ok$", output, flags=re.M)
    failed = re.findall(r"^test credits::tests::(\w+) \.\.\. FAILED$", output, flags=re.M)
    return passed, failed


def checks(by_harness: dict[str, list[str]]) -> str:
    return "; ".join(f"`{h.split('_')[0]}` {', '.join(c)}" for h, c in by_harness.items())


def evaluate(mutant: Mutant, copy: Path, original: str, commands: dict) -> tuple[str, str, str]:
    """Verdict, every failed check, failed unit tests. The verdict is
    `caught`, `NOT CAUGHT AS AIMED: …` or `inconclusive: …`."""
    target = copy / CREDITS
    start, end = prooflib.function_span(original)
    body = original[start:end]
    if body.count(mutant.find) != 1:
        return (f"inconclusive: the text to mutate occurs {body.count(mutant.find)} times "
                "in the function", "", "")
    target.write_text(original[:start] + body.replace(mutant.find, mutant.replace) + original[end:])

    code, output, _ = run(commands["build"], copy)
    if code != 0:
        reason = "timed out" if code is None else "does not build"
        print(output[-2000:])
        return (f"inconclusive: the mutant {reason} (a compilation failure is not a detection)",
                "", "")

    code, output, proof_seconds = run(commands["kani"], copy / "proofs")
    results = None if code is None else prooflib.parse_kani(output)
    if results is None or sorted(results) != sorted(commands["harnesses"]):
        reason = "timed out" if code is None else "did not run every harness to a verdict"
        print(output[-2000:])
        return f"inconclusive: the tool {reason}", "", ""

    problems = []
    for harness, aimed in mutant.aimed.items():
        missing = [c for c in aimed if c not in results[harness]["failed"]]
        if missing:
            problems.append(f"{harness}: {', '.join(missing)} did not fail")
    for harness, untouched in mutant.untouched.items():
        broken = [c for c in untouched if c in results[harness]["failed"]]
        if broken:
            problems.append(f"{harness}: {', '.join(broken)} failed and should hold")
    others = {h: r["other"] for h, r in results.items() if r["other"]}
    if others:
        problems.append(f"checks that are not properties failed: {others}")
    failing = {h: r["failed"] for h, r in results.items() if r["failed"]}

    code, output, test_seconds = run(commands["tests"], copy)
    outcome = None if code is None else parse_tests(output)
    if outcome is None:
        reason = "timed out" if code is None else "did not report"
        print(output[-2000:])
        return f"inconclusive: the unit tests {reason}", checks(failing), ""
    failed_tests = sorted(outcome[1])
    for test in mutant.tests:
        if test not in failed_tests:
            problems.append(f"unit test {test} did not fail")

    print(f"    aimed at      {mutant.aimed}")
    print(f"    proof failed  {failing} ({proof_seconds:.1f} s)")
    print(f"    tests failed  {failed_tests} ({test_seconds:.1f} s)")
    verdict = "caught" if not problems else "NOT CAUGHT AS AIMED: " + "; ".join(problems)
    return verdict, checks(failing), ", ".join(f"`{t}`" for t in failed_tests)


def control(copy: Path, commands: dict) -> str | None:
    """None when the unmodified copy passes, or what went wrong."""
    code, output, seconds = run(commands["kani"], copy / "proofs")
    results = None if code is None else prooflib.parse_kani(output)
    if code is None:
        return "the control run of the proof timed out"
    if (code != 0 or results is None or sorted(results) != sorted(commands["harnesses"])
            or any(r["verdict"] != "verified" for r in results.values())):
        print(output[-3000:], file=sys.stderr)
        return "the control run of the proof did not pass"
    print(f"control  proof: {len(results)} harnesses verified ({seconds:.1f} s)")
    code, output, seconds = run(commands["tests"], copy)
    outcome = None if code is None else parse_tests(output)
    if code is None:
        return "the control run of the unit tests timed out"
    if code != 0 or outcome is None or outcome[1] or not outcome[0]:
        print(output[-3000:], file=sys.stderr)
        return "the control run of the unit tests did not pass"
    print(f"control  tests: {len(outcome[0])} passed ({seconds:.1f} s)")
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", help="run the control and this mutant only")
    parser.add_argument("--report", type=Path, help="write the Markdown report to this file")
    arguments = parser.parse_args()

    tool = shutil.which("cargo-kani")
    if tool is None:
        print("mutants: cargo-kani is not on PATH", file=sys.stderr)
        return 1
    selected = [m for m in MUTANTS if arguments.only in (None, m.name)]
    if not selected:
        print(f"mutants: no mutant is named {arguments.only}", file=sys.stderr)
        return 1
    commands = {
        "kani": [tool, "kani", "-p", PACKAGE],
        "build": ["cargo", "build", "--locked", "-p", "mqttiumd-core"],
        "tests": ["cargo", "test", "--locked", "-p", "mqttiumd-core", "--lib", "credits::"],
        "harnesses": list(prooflib.listed_harnesses()),
    }

    rows: list[str] = []
    with tempfile.TemporaryDirectory(prefix="mqttiumd-mutants-") as directory:
        copy = Path(directory)
        prooflib.copy_sources(copy)
        original = (copy / CREDITS).read_text()
        broken = control(copy, commands)
        verdicts = []
        for mutant in selected:
            if broken is not None:
                verdict, failing, failed_tests = f"inconclusive: {broken}", "", ""
            else:
                verdict, failing, failed_tests = evaluate(mutant, copy, original, commands)
            (copy / CREDITS).write_text(original)
            verdicts.append(verdict)
            print(f"{mutant.name}: {verdict}")
            rows.append(f"| `{mutant.name}` | {mutant.intent} | {checks(mutant.aimed)} | "
                        f"{failing} | {failed_tests} | {verdict} |")

    caught = sum(verdict == "caught" for verdict in verdicts)
    ok = broken is None and caught == len(selected)
    summary = [
        f"Control: {'passed' if broken is None else broken}.",
        f"{caught} of {len(selected)} mutants caught, "
        f"{sum(v.startswith('inconclusive') for v in verdicts)} inconclusive, "
        f"{sum(v.startswith('NOT CAUGHT') for v in verdicts)} not caught as aimed.",
        "mutants: PASS" if ok else "mutants: FAIL",
    ]
    report = "\n".join([*HEADER, *rows, "", *summary]) + "\n"
    if arguments.report:
        arguments.report.write_text(report)
    print()
    print(report, end="")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
