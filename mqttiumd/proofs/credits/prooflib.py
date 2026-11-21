"""What the scripts of the pilot proof share (DEM-021).

One place says where the function is, what is recorded about the sources,
how the tool's output is read, how a command is bounded in time and what
the mutants are. The runner, the mutants, the Verus trial and the
self-test import it, so that no two of them can disagree.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROOFS = HERE.parent
PRODUCT = PROOFS.parent
PACKAGE = "mqttiumd-proof-credits"
CREDITS = Path("crates/mqttiumd-core/src/credits.rs")
RECORD = HERE / "BODY.sha256"

# Every source file the proof analyses. The digest of each is recorded;
# the coverage of the tool is used to check that no other file of the
# product is reached (see `run.py`). The first is the function's own
# file; the two of the contract crate hold what its body calls:
# `checked_sub` and the derived comparison of `LeaseKey`.
RECORDED_FILES = [
    CREDITS,
    Path("crates/mqttiumd-contract/src/capacity.rs"),
    Path("crates/mqttiumd-contract/src/ids.rs"),
    Path("proofs/credits/src/lib.rs"),
    Path("proofs/credits/harnesses.txt"),
    Path("proofs/credits/tests/no_allocation.rs"),
]
FUNCTION = "pub fn release_if_current("


def function_span(text: str) -> tuple[int, int]:
    """Where the text of the function starts and ends: from its `pub fn`
    line to the first closing brace in column one, newline included."""
    if text.count(FUNCTION) != 1:
        raise ValueError(f"`{FUNCTION}` occurs {text.count(FUNCTION)} times")
    start = text.index(FUNCTION)
    return start, text.index("\n}\n", start) + 3


def function_text(text: str) -> str:
    start, end = function_span(text)
    return text[start:end]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digests(product: Path = PRODUCT) -> str:
    """The record of the sources: one line per file, and one for the text
    of the function."""
    lines = []
    for file in RECORDED_FILES:
        lines.append(f"file {sha256((product / file).read_bytes())} {file.as_posix()}")
        if file == CREDITS:
            body = function_text((product / file).read_text())
            lines.append(f"body {sha256(body.encode())} release_if_current")
    return "\n".join(lines) + "\n"


def listed_harnesses(directory: Path = HERE) -> dict[str, int]:
    """Harness name to the number of witnesses it must satisfy."""
    listed = {}
    for line in (directory / "harnesses.txt").read_text().splitlines():
        if line and not line.startswith("#"):
            name, covers = line.split()
            listed[name] = int(covers)
    return listed


def run_bounded(
    command: list[str], cwd: Path, limit: float, env: dict[str, str] | None = None
) -> tuple[int | None, str, float]:
    """Exit code (None when the time limit was exceeded), combined output,
    seconds. The command runs in a session of its own and, on a timeout,
    the whole group is killed: the tool starts children that would
    otherwise keep running."""
    began = time.monotonic()
    process = subprocess.Popen(
        command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        start_new_session=True, env=env,
    )
    try:
        output, _ = process.communicate(timeout=limit)
        return process.returncode, output, time.monotonic() - began
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate()
        return None, output or "", time.monotonic() - began


def kani_sections(output: str) -> dict[str, str]:
    """The part of the tool's output that belongs to each harness it ran."""
    parts = re.split(r"^Checking harness harnesses::(\w+)\.\.\.$", output, flags=re.M)
    return dict(zip(parts[1::2], parts[2::2]))


def parse_kani(output: str) -> dict[str, dict] | None:
    """What the tool reports for each harness it ran, by its own account,
    or None when a harness has no verdict or the total is missing."""
    results: dict[str, dict] = {}
    for name, body in kani_sections(output).items():
        if "VERIFICATION:- SUCCESSFUL" in body:
            verdict = "verified"
        elif "VERIFICATION:- FAILED" in body:
            verdict = "failed"
        else:
            return None
        failed = re.findall(r'^Failed Checks: "?(.*?)"?$', body, flags=re.M)
        covers = re.search(r"^ \*\* (\d+) of (\d+) cover properties satisfied", body, flags=re.M)
        checks = re.search(r"^ \*\* (\d+) of (\d+) failed", body, flags=re.M)
        results[name] = {
            "verdict": verdict,
            "failed": sorted({m.group(0) for d in failed if (m := re.match(r"P\d+[a-z]", d))}),
            "other": sorted({d for d in failed if not re.match(r"P\d+[a-z]", d)}),
            "covers": (int(covers.group(1)), int(covers.group(2))) if covers else (0, 0),
            "checks": int(checks.group(2)) if checks else 0,
            "failures": len(re.findall(r"- Status: FAILURE$", body, flags=re.M)),
        }
    total = re.search(
        r"^Complete - (\d+) successfully verified harnesses, (\d+) failures, (\d+) total\.$",
        output, flags=re.M,
    )
    if total is None or int(total.group(3)) != len(results):
        return None
    return results


def copy_sources(destination: Path, product: Path = PRODUCT) -> None:
    """The product workspace, the proof workspace and the tools, without
    build output."""
    skip = shutil.ignore_patterns("target", "runs", "__pycache__")
    for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml"):
        shutil.copy2(product / name, destination / name)
    for name in ("crates", "proofs", "tools"):
        shutil.copytree(product / name, destination / name, ignore=skip)


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
