#!/usr/bin/env python3
"""The limited Verus trial on release_if_current (DEM-021, deliverable 4).

    python3 proofs/credits/verus-trial.py

It is a trial, not the proof of the lot: the reception rests on Kani.

Verus does not read an unannotated crate, and the pure crates may carry
neither its attributes nor its library (ADR 0007). So the trial annotates
a temporary copy of the two production crates, mechanically, and checks
that the text of the function is still exactly the production one: the
definition verified is the product's, with attributes around it, not a
rewritten equivalent. Nothing is written into the repository.

Steps, each with its expected verdict:

0. the unannotated contract crate is refused by Verus;
1. with the annotations, the contract crate and the core crate verify;
   the postcondition of release_if_current says that a call that does not
   return `Released` leaves the lease and the account exactly as they were;
2. the opposite postcondition (a release leaves them as they were) is
   refuted;
3. two mutants of the body are refuted by the postcondition.

Exit code: 0 when every step gives its expected verdict, 1 otherwise,
2 when the Verus that PATH selects is not the pinned one.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import prooflib
from prooflib import function_text

HERE = Path(__file__).resolve().parent
PRODUCT = HERE.parent.parent
PRELUDE = "use vstd::prelude::*;"
VERIFY = "#[verus_verify]"
SPEC = """#[verus_spec(result =>
    ensures
        !(result matches Ok(ReleaseOutcome::Released))
            ==> *final(account) == *old(account) && *final(lease) == *old(lease),
)]
"""
FALSE_SPEC = """#[verus_spec(result =>
    ensures
        (result matches Ok(ReleaseOutcome::Released))
            ==> *final(account) == *old(account) && *final(lease) == *old(lease),
)]
"""

# (file, text to find exactly once, replacement). Every replacement only
# adds lines; the count of added lines is reported.
ANNOTATIONS = [
    ("mqttiumd-contract/src/lib.rs", "extern crate alloc;\n",
     f"extern crate alloc;\n#[allow(unused_imports)]\n{PRELUDE}\n"),
    ("mqttiumd-contract/src/capacity.rs", "use alloc::vec::Vec;\n",
     f"use alloc::vec::Vec;\n{PRELUDE}\n"),
    ("mqttiumd-contract/src/capacity.rs", "        pub struct $name(u64);\n",
     f"        {VERIFY}\n        pub struct $name(u64);\n"),
    ("mqttiumd-contract/src/capacity.rs", "            pub const fn checked_sub(",
     f"            {VERIFY}\n            pub const fn checked_sub("),
    ("mqttiumd-contract/src/capacity.rs", "pub enum Resource {\n",
     f"{VERIFY}\npub enum Resource {{\n"),
    ("mqttiumd-contract/src/ids.rs", "use core::num::NonZeroU16;\n",
     f"use core::num::NonZeroU16;\n{PRELUDE}\n"),
    ("mqttiumd-contract/src/ids.rs",
     "        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]\n"
     "        pub struct $name(u64);\n",
     "        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]\n"
     f"        {VERIFY}\n        pub struct $name(u64);\n"),
    ("mqttiumd-contract/src/ids.rs", "pub struct LeaseKey {\n",
     f"{VERIFY}\npub struct LeaseKey {{\n"),
    ("mqttiumd-core/src/lib.rs", "extern crate alloc;\n",
     f"extern crate alloc;\n#[allow(unused_imports)]\n{PRELUDE}\n"),
    ("mqttiumd-core/src/credits.rs",
     "use crate::contract::ids::{AccountId, LeaseGeneration, LeaseKey};\n",
     f"use crate::contract::ids::{{AccountId, LeaseGeneration, LeaseKey}};\n{PRELUDE}\n"),
    *[
        ("mqttiumd-core/src/credits.rs", f"{item} {{\n", f"{VERIFY}\n{item} {{\n")
        for item in (
            "pub struct Credits", "pub enum LeaseState", "pub struct LeaseRecord",
            "pub struct Account", "pub enum ReleaseOutcome", "pub enum ReleaseError",
        )
    ],
    ("mqttiumd-core/src/credits.rs", "pub fn release_if_current(\n",
     f"{VERIFY}\n{SPEC}pub fn release_if_current(\n"),
]

# The same mutants as the proof's, by name, from the one place that
# defines them.
MUTANTS = {
    mutant.name: (mutant.find, mutant.replace)
    for mutant in prooflib.MUTANTS
    if mutant.name in ("state-written-before-checks", "bytes-subtracted-before-slots-checked")
}
assert len(MUTANTS) == 2, "a mutant of the Verus trial is no longer defined"


def once(text: str, find: str, replace: str, where: str) -> str:
    if text.count(find) != 1:
        sys.exit(f"verus trial: `{find.strip()}` occurs {text.count(find)} times in {where}")
    return text.replace(find, replace)


def verus(work: Path, crate: str, *extra: str) -> tuple[int, str, float]:
    began = time.monotonic()
    done = subprocess.run(
        ["verus", "--crate-type=lib", "--edition=2024", "--crate-name",
         crate.replace("-", "_"), f"crates/{crate}/src/lib.rs", *extra],
        cwd=work, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        timeout=600, check=False,
    )
    return done.returncode, done.stdout, time.monotonic() - began


def core(work: Path) -> tuple[int, str, float]:
    return verus(
        work, "mqttiumd-core",
        "--extern", "mqttiumd_contract=libmqttiumd_contract.rlib",
        "--import", "mqttiumd_contract=contract.vir",
    )


def results(output: str) -> str:
    found = re.search(r"verification results:: (\d+ verified, \d+ errors)", output)
    return found.group(1) if found else "no verification result"


def main() -> int:
    installer = (PRODUCT / "tools/install-tools.sh").read_text()
    release = re.search(r'^VERUS_RELEASE="(.*)"$', installer, flags=re.M).group(1)
    tool = shutil.which("verus")
    version = subprocess.run(
        ["verus", "--version"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        text=True, check=False,
    ) if tool else None
    if version is None or version.returncode != 0 or (
        f"  Version: {release}" not in version.stdout.splitlines()
    ):
        print(f"verus trial: PATH does not select Verus {release} ({tool})", file=sys.stderr)
        return 2
    toolchain = next(
        (line.split(":", 1)[1].split()[0] for line in version.stdout.splitlines()
         if line.strip().startswith("Toolchain:")), "unknown",
    )
    print(f"tool       {tool}")
    print(f"version    Verus {release}, toolchain {toolchain}, bundled Z3")

    ok = True

    def expect(step: str, condition: bool, detail: str) -> None:
        nonlocal ok
        ok = ok and condition
        print(f"{'ok  ' if condition else 'FAIL'}  {step}: {detail}")

    with tempfile.TemporaryDirectory(prefix="mqttiumd-verus-") as directory:
        work = Path(directory)
        shutil.copytree(PRODUCT / "crates", work / "crates",
                        ignore=shutil.ignore_patterns("target"))
        production = (work / "crates/mqttiumd-core/src/credits.rs").read_text()

        code, output, _ = verus(work, "mqttiumd-contract")
        expect("0 unannotated crate", code != 0 and "verus_builtin" in output,
               "refused: Verus needs its library imported by the crate")

        added = 0
        for file, find, replace in ANNOTATIONS:
            path = work / "crates" / file
            path.write_text(once(path.read_text(), find, replace, file))
            added += replace.count("\n") - find.count("\n")
        annotated = (work / "crates/mqttiumd-core/src/credits.rs").read_text()
        expect("  same body", function_text(annotated) == function_text(production),
               f"the text of the function is the production one; {added} lines added around it "
               f"in {len({a[0] for a in ANNOTATIONS})} files")

        code, output, seconds = verus(
            work, "mqttiumd-contract", "--export", "contract.vir",
            "--compile", "-o", "libmqttiumd_contract.rlib",
        )
        expect("1 contract crate", code == 0 and "0 errors" in results(output),
               f"{results(output)} ({seconds:.1f} s)")
        code, output, seconds = core(work)
        expect("1 core crate", code == 0 and "0 errors" in results(output),
               f"{results(output)} ({seconds:.1f} s): no mutation unless Released")

        path = work / "crates/mqttiumd-core/src/credits.rs"
        path.write_text(once(annotated, SPEC, FALSE_SPEC, "credits.rs"))
        code, output, _ = core(work)
        expect("2 false property",
               code != 0 and "postcondition not satisfied" in output and "1 errors" in results(output),
               f"{results(output)}: refuted, postcondition not satisfied")

        for name, (find, replace) in MUTANTS.items():
            path.write_text(once(annotated, find, replace, "credits.rs"))
            code, output, _ = core(work)
            expect(f"3 mutant {name}",
                   code != 0 and "postcondition not satisfied" in output
                   and "1 errors" in results(output),
                   f"{results(output)}: refuted, postcondition not satisfied")

    print("verus trial: PASS" if ok else "verus trial: FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
