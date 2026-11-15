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
  time limit: an error or a timeout is not a detection;
- the checks the mutant aims at are among the failed checks, and the
  checks it must leave alone still hold: the failure shows the violation
  of the property aimed at, not a side effect;
- the unit test named for it fails.

Before any mutant, the unmodified copy is run as a control: every harness
verified, every unit test passing. Without it a failure could not be
attributed to the mutation.

Exit code: 0 when the control passes and every mutant is caught by the
proof and by its unit test; 1 otherwise.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRODUCT = HERE.parent.parent
SOURCE = Path("crates/mqttiumd-core/src/credits.rs")
PACKAGE = "mqttiumd-proof-credits"
TIME_LIMIT_SECONDS = 900


@dataclass(frozen=True)
class Mutant:
    name: str
    intent: str
    find: str
    replace: str
    # Checks that must fail, by harness.
    aimed: dict[str, list[str]]
    # Unit tests that must fail.
    tests: list[str]
    # Checks that must still hold: the mutation does not touch them. Only
    # checks asserted before the aimed ones are listed: a failed assertion
    # ends its path, so a later check says nothing about the paths it ended.
    untouched: dict[str, list[str]] = field(default_factory=dict)


STALE_GUARD = """    if key != lease.key {
        return Ok(ReleaseOutcome::Stale);
    }
"""
ACCOUNT_GUARD = """    if account.id != lease.key.account {
        return Err(ReleaseError::WrongAccount);
    }
"""
RELEASED_GUARD = """    if lease.state == LeaseState::Released {
        return Ok(ReleaseOutcome::AlreadyReleased);
    }
"""
BYTES_CHECK = """    let Some(bytes) = account.used.bytes.checked_sub(lease.cost.bytes) else {
        return Err(ReleaseError::CostExceedsUsage(Resource::PayloadBytes));
    };
"""
SLOTS_CHECK = """    let Some(slots) = account.used.slots.checked_sub(lease.cost.slots) else {
        return Err(ReleaseError::CostExceedsUsage(Resource::ExchangeSlots));
    };
"""
WRITE_USAGE = "    account.used = Credits { bytes, slots };\n"
WRITE_STATE = "    lease.state = LeaseState::Released;\n    Ok(ReleaseOutcome::Released)\n"

MUTANTS = [
    Mutant(
        name="generation-ignored",
        intent="The generation is ignored: a key is current when its account and slot match.",
        find="    if key != lease.key {\n",
        replace="    if key.account != lease.key.account || key.slot != lease.key.slot {\n",
        aimed={
            "p2_stale_key_mutates_nothing": ["P2b", "P2a"],
            "p7_old_key_leaves_the_reused_slot": ["P7c"],
        },
        tests=[
            "a_stale_key_mutates_nothing",
            "an_old_key_leaves_the_reservation_that_reused_the_slot",
        ],
    ),
    Mutant(
        name="stale-guard-dropped",
        intent="A guard is dropped: the key is not compared at all.",
        find=STALE_GUARD,
        replace="",
        aimed={"p2_stale_key_mutates_nothing": ["P2b", "P2a"]},
        tests=["a_stale_key_mutates_nothing"],
    ),
    Mutant(
        name="account-guard-dropped",
        intent="A guard is dropped: any account pays for the lease.",
        find=ACCOUNT_GUARD,
        replace="",
        aimed={"p4_wrong_account_is_refused_whole": ["P4b", "P4a"]},
        tests=["a_wrong_account_is_an_explicit_error_and_changes_nothing"],
    ),
    Mutant(
        name="release-repeated",
        intent="A release is repeated: a released lease is subtracted again.",
        find=RELEASED_GUARD,
        replace="",
        aimed={"p3_repeated_release_subtracts_once": ["P3a", "P3b"]},
        tests=["a_repeated_release_subtracts_nothing_more"],
    ),
    Mutant(
        name="state-not-released",
        intent="A release is repeatable: the usage is reduced and the lease stays held.",
        find=WRITE_STATE,
        replace="    Ok(ReleaseOutcome::Released)\n",
        aimed={
            "p6_current_key_releases_exactly": ["P6d"],
            "p3_repeated_release_subtracts_once": ["P3a", "P3c"],
        },
        tests=[
            "a_current_key_subtracts_both_dimensions_exactly_and_releases",
            "a_repeated_release_subtracts_nothing_more",
        ],
        untouched={"p6_current_key_releases_exactly": ["P6b", "P6c"]},
    ),
    Mutant(
        name="bytes-check-dropped",
        intent="A guard is dropped: the bytes are subtracted without a check and wrap.",
        find=BYTES_CHECK,
        replace=(
            "    let bytes = PayloadBytes::new(\n"
            "        account\n"
            "            .used\n"
            "            .bytes\n"
            "            .get()\n"
            "            .wrapping_sub(lease.cost.bytes.get()),\n"
            "    );\n"
        ),
        aimed={"p5_cost_above_usage_changes_neither_dimension": ["P5b"]},
        tests=["bytes_above_usage_change_neither_dimension"],
    ),
    Mutant(
        name="slots-check-dropped",
        intent="A guard is dropped: the slots are subtracted without a check and wrap.",
        find=SLOTS_CHECK,
        replace=(
            "    let slots = ExchangeSlots::new(\n"
            "        account\n"
            "            .used\n"
            "            .slots\n"
            "            .get()\n"
            "            .wrapping_sub(lease.cost.slots.get()),\n"
            "    );\n"
        ),
        aimed={"p5_cost_above_usage_changes_neither_dimension": ["P5c"]},
        tests=["slots_above_usage_change_neither_dimension"],
    ),
    Mutant(
        name="bytes-subtracted-before-slots-checked",
        intent="One dimension is subtracted before the other is checked.",
        find=BYTES_CHECK,
        replace=BYTES_CHECK + "    account.used.bytes = bytes;\n",
        aimed={"p5_cost_above_usage_changes_neither_dimension": ["P5b"]},
        tests=["slots_above_usage_change_neither_dimension"],
    ),
    Mutant(
        name="state-written-before-checks",
        intent="The lease is marked released before the subtractions are checked.",
        find=BYTES_CHECK,
        replace="    lease.state = LeaseState::Released;\n" + BYTES_CHECK,
        aimed={"p5_cost_above_usage_changes_neither_dimension": ["P5d"]},
        tests=["bytes_above_usage_change_neither_dimension"],
        untouched={"p5_cost_above_usage_changes_neither_dimension": ["P5b", "P5c"]},
    ),
    Mutant(
        name="slots-not-subtracted",
        intent="The subtraction is not exact: only the bytes are given back.",
        find=WRITE_USAGE,
        replace=(
            "    account.used = Credits {\n"
            "        bytes,\n"
            "        slots: account.used.slots,\n"
            "    };\n"
        ),
        aimed={"p6_current_key_releases_exactly": ["P6c"]},
        tests=["a_current_key_subtracts_both_dimensions_exactly_and_releases"],
        untouched={"p6_current_key_releases_exactly": ["P6b"]},
    ),
    Mutant(
        name="bytes-subtracted-from-the-limit",
        intent="The subtraction is not exact: the cost is taken from the limit, not the usage.",
        find="account.used.bytes.checked_sub(lease.cost.bytes)",
        replace="account.limit.bytes.checked_sub(lease.cost.bytes)",
        aimed={"p6_current_key_releases_exactly": ["P6b"]},
        tests=["a_current_key_subtracts_both_dimensions_exactly_and_releases"],
    ),
    Mutant(
        name="account-checked-before-the-key",
        intent="The order of the guards is swapped: a late release after reuse becomes a fault.",
        find=STALE_GUARD + ACCOUNT_GUARD,
        replace=ACCOUNT_GUARD + STALE_GUARD,
        aimed={"p2_stale_key_mutates_nothing": ["P2a"]},
        tests=["an_old_key_of_another_account_is_stale_not_a_fault"],
        untouched={"p2_stale_key_mutates_nothing": ["P2b"]},
    ),
]


def function_span(text: str) -> tuple[int, int]:
    start = text.index("pub fn release_if_current(")
    end = text.index("\n}\n", start) + 3
    return start, end


def run(command: list[str], cwd: Path) -> tuple[int | None, str, float]:
    """Exit code (None on timeout), combined output, seconds."""
    began = time.monotonic()
    try:
        done = subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=TIME_LIMIT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as expired:
        output = expired.stdout or ""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        return None, output, time.monotonic() - began
    return done.returncode, done.stdout, time.monotonic() - began


def harness_names() -> list[str]:
    lines = (HERE / "harnesses.txt").read_text().splitlines()
    return [line.split()[0] for line in lines if line and not line.startswith("#")]


def parse_kani(output: str, expected: list[str]) -> dict[str, dict] | None:
    """Verdict and failed checks of each harness, or None when the tool did
    not run every harness to a verdict."""
    parts = re.split(r"^Checking harness harnesses::(\w+)\.\.\.$", output, flags=re.M)
    results: dict[str, dict] = {}
    for name, body in zip(parts[1::2], parts[2::2]):
        if "VERIFICATION:- SUCCESSFUL" in body:
            verdict = "verified"
        elif "VERIFICATION:- FAILED" in body:
            verdict = "failed"
        else:
            return None
        failed = re.findall(r'^Failed Checks: "?(.*?)"?$', body, flags=re.M)
        named = sorted({m.group(0) for d in failed if (m := re.match(r"P\d+[a-z]", d))})
        other = sorted({d for d in failed if not re.match(r"P\d+[a-z]", d)})
        results[name] = {"verdict": verdict, "failed": named, "other": other}
    if sorted(results) != sorted(expected):
        return None
    total = re.search(
        r"^Complete - (\d+) successfully verified harnesses, (\d+) failures, (\d+) total\.$",
        output,
        flags=re.M,
    )
    if total is None or int(total.group(3)) != len(expected):
        return None
    return results


def parse_tests(output: str) -> tuple[list[str], list[str]] | None:
    """Passed and failed unit tests of the credits module, or None when the
    test binary did not report."""
    if not re.search(r"^test result: ", output, flags=re.M):
        return None
    passed = re.findall(r"^test credits::tests::(\w+) \.\.\. ok$", output, flags=re.M)
    failed = re.findall(r"^test credits::tests::(\w+) \.\.\. FAILED$", output, flags=re.M)
    return passed, failed


def copy_sources(destination: Path) -> None:
    skip = shutil.ignore_patterns("target", "runs", "__pycache__")
    for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
        shutil.copy2(PRODUCT / name, destination / name)
    shutil.copytree(PRODUCT / "crates", destination / "crates", ignore=skip)
    shutil.copytree(PRODUCT / "proofs", destination / "proofs", ignore=skip)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", help="run the control and this mutant only")
    parser.add_argument("--report", type=Path, help="write the Markdown table to this file")
    arguments = parser.parse_args()

    tool = shutil.which("cargo-kani")
    if tool is None:
        print("mutants: cargo-kani is not on PATH", file=sys.stderr)
        return 1
    kani = [tool, "kani", "-p", PACKAGE]
    build = ["cargo", "build", "--locked", "-p", "mqttiumd-core"]
    tests = ["cargo", "test", "--locked", "-p", "mqttiumd-core", "--lib", "credits::"]
    harnesses = harness_names()
    selected = [m for m in MUTANTS if arguments.only in (None, m.name)]
    if not selected:
        print(f"mutants: no mutant is named {arguments.only}", file=sys.stderr)
        return 1

    ok = True
    rows: list[str] = []
    with tempfile.TemporaryDirectory(prefix="mqttiumd-mutants-") as directory:
        copy = Path(directory)
        copy_sources(copy)
        target = copy / SOURCE
        original = target.read_text()
        start, end = function_span(original)

        # Control: the unmodified copy.
        code, output, seconds = run(kani, copy / "proofs")
        control = parse_kani(output, harnesses) if code == 0 else None
        if control is None or any(r["verdict"] != "verified" for r in control.values()):
            print("mutants: the control run of the proof did not pass", file=sys.stderr)
            print(output[-3000:], file=sys.stderr)
            return 1
        print(f"control  proof: {len(control)} harnesses verified ({seconds:.1f} s)")
        code, output, seconds = run(tests, copy)
        control_tests = parse_tests(output)
        if code != 0 or control_tests is None or control_tests[1] or not control_tests[0]:
            print("mutants: the control run of the unit tests did not pass", file=sys.stderr)
            print(output[-3000:], file=sys.stderr)
            return 1
        print(f"control  tests: {len(control_tests[0])} passed ({seconds:.1f} s)")

        for mutant in selected:
            body = original[start:end]
            if body.count(mutant.find) != 1:
                print(f"{mutant.name}: INCONCLUSIVE, the text to mutate occurs "
                      f"{body.count(mutant.find)} times in the function")
                ok = False
                continue
            target.write_text(
                original[:start] + body.replace(mutant.find, mutant.replace) + original[end:]
            )

            code, output, _ = run(build, copy)
            if code != 0:
                print(f"{mutant.name}: INCONCLUSIVE, the mutant does not build "
                      "(a compilation failure is not a detection)")
                print(output[-2000:])
                ok = False
                continue

            code, output, proof_seconds = run(kani, copy / "proofs")
            results = None if code is None else parse_kani(output, harnesses)
            if results is None:
                reason = "timed out" if code is None else "did not run every harness to a verdict"
                print(f"{mutant.name}: INCONCLUSIVE, the tool {reason}")
                print(output[-2000:])
                ok = False
                continue

            problems = []
            for harness, checks in mutant.aimed.items():
                missing = [c for c in checks if c not in results[harness]["failed"]]
                if missing:
                    problems.append(f"{harness}: {', '.join(missing)} did not fail")
            for harness, checks in mutant.untouched.items():
                broken = [c for c in checks if c in results[harness]["failed"]]
                if broken:
                    problems.append(f"{harness}: {', '.join(broken)} failed and should hold")
            others = {h: r["other"] for h, r in results.items() if r["other"]}
            if others:
                problems.append(f"checks that are not properties failed: {others}")

            code, output, test_seconds = run(tests, copy)
            outcome = None if code is None else parse_tests(output)
            if outcome is None:
                print(f"{mutant.name}: INCONCLUSIVE, the unit tests did not report")
                print(output[-2000:])
                ok = False
                continue
            failed_tests = outcome[1]
            for test in mutant.tests:
                if test not in failed_tests:
                    problems.append(f"unit test {test} did not fail")

            failing = {h: r["failed"] for h, r in results.items() if r["failed"]}
            verdict = "caught" if not problems else "NOT CAUGHT AS AIMED"
            ok = ok and not problems
            print(f"{mutant.name}: {verdict} ({proof_seconds:.1f} s proof, "
                  f"{test_seconds:.1f} s tests)")
            print(f"    aimed at      {mutant.aimed}")
            print(f"    proof failed  {failing}")
            print(f"    tests failed  {sorted(failed_tests)}")
            for problem in problems:
                print(f"    PROBLEM       {problem}")

            aimed = "; ".join(f"`{h.split('_')[0]}` {', '.join(c)}" for h, c in mutant.aimed.items())
            every = "; ".join(f"`{h.split('_')[0]}` {', '.join(c)}" for h, c in failing.items())
            rows.append(
                f"| `{mutant.name}` | {mutant.intent} | {aimed} | {every} | "
                f"{', '.join(f'`{t}`' for t in sorted(failed_tests))} | {verdict} |"
            )

        target.write_text(original)

    table = "\n".join(
        [
            "| Mutant | What it does | Checks aimed at | Every failed check | Unit tests that fail | Verdict |",
            "| - | - | - | - | - | - |",
            *rows,
        ]
    )
    if arguments.report:
        arguments.report.write_text(table + "\n")
    print()
    print(table)
    print()
    print("mutants: PASS" if ok else "mutants: FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
