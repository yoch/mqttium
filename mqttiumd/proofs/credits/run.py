#!/usr/bin/env python3
"""The pilot proof of release_if_current (lot L6, DEM-021), in one command.

    proofs/credits/run.sh            run the preflight, then the proof
    proofs/credits/run.sh --record   the same, then record the digests

Steps, in this order; the first that fails ends the run with its code.

1. The preflight (`proofs/preflight.sh`): the tool PATH selects, its
   version, a true property verified and a false one refuted. Its own
   exit code is passed on.
2. No allocation (exit 9): a test binary with a counting allocator calls
   the function once per outcome and requires zero allocations.
3. The harnesses, by the tool's own account (exit 6, 7). The tool is run
   on the whole proof crate, with no filter; the harnesses it reports
   must be exactly those of `harnesses.txt`, so a harness nobody listed
   cannot go unseen. Each must be verified, with no failed check, and
   satisfy exactly its listed number of witnesses.
4. Coverage (exit 8): every region of the function is covered under the
   harness that assumes nothing, and no harness reaches a source file of
   the product whose digest is not recorded.
5. The digests (exit 5): every file the proof analyses, and the text of
   the function, must be as `BODY.sha256` records them. With `--record`
   the record is written here, last, after everything else passed: a
   body that is not proved is never recorded.

Exit codes: 0 pass; 2 unknown argument; 5 a digest differs; 6 the
harnesses run are not the listed ones; 7 a harness is not verified with
its witnesses; 8 coverage; 9 the function allocated; otherwise the
preflight's code.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import prooflib
from prooflib import HERE, PACKAGE, PRODUCT, PROOFS, RECORD, RECORDED_FILES

TIME_LIMIT_SECONDS = 900


def fail(code: int, message: str) -> int:
    print(f"proof: FAIL: {message}", file=sys.stderr)
    return code


def main(arguments: list[str]) -> int:
    if arguments not in ([], ["--record"]):
        print(f"proof: unknown argument: {' '.join(arguments)}\n"
              "usage: proofs/credits/run.sh [--record]", file=sys.stderr)
        return 2
    record = arguments == ["--record"]
    out = Path(os.environ.get("PROOF_OUT", PROOFS / "target/proof-credits"))
    out.mkdir(parents=True, exist_ok=True)

    # --- 1. preflight ----------------------------------------------------
    preflight = subprocess.run(
        [str(PROOFS / "preflight.sh")], env={**os.environ, "PREFLIGHT_OUT": str(out / "preflight")},
        check=False,
    )
    if preflight.returncode != 0:
        return preflight.returncode
    tool = shutil.which("cargo-kani")
    kani = [tool, "kani", "-p", PACKAGE]

    # --- 2. no allocation ------------------------------------------------
    code, output, _ = prooflib.run_bounded(
        ["cargo", "test", "--locked", "-p", PACKAGE, "--test", "no_allocation"],
        PROOFS, TIME_LIMIT_SECONDS,
    )
    (out / "no_allocation.log").write_text(output)
    counted = re.search(r"^test result: ok\. (\d+) passed; 0 failed", output, flags=re.M)
    if code != 0 or counted is None or int(counted.group(1)) == 0:
        return fail(9, "the allocation test did not pass: the function allocates, or the "
                       f"test did not run (see {out / 'no_allocation.log'})")

    # --- 3. the harnesses, as the tool reports them ----------------------
    listed = prooflib.listed_harnesses()
    code, output, _ = prooflib.run_bounded(kani, PROOFS, TIME_LIMIT_SECONDS)
    (out / "harnesses.log").write_text(output)
    results = None if code is None else prooflib.parse_kani(output)
    if results is None:
        reason = "timed out" if code is None else f"exited with {code} without a full report"
        return fail(7, f"the tool {reason} (see {out / 'harnesses.log'})")
    if sorted(results) != sorted(listed):
        unlisted = sorted(set(results) - set(listed))
        missing = sorted(set(listed) - set(results))
        return fail(6, "the harnesses the tool ran are not those of harnesses.txt: "
                       f"not listed {unlisted}, not run {missing}")
    witnesses = 0
    for name, covers in listed.items():
        result = results[name]
        if result["verdict"] != "verified" or result["failures"] != 0:
            return fail(7, f"{name}: not verified, failed checks "
                           f"{result['failed'] + result['other']} (see {out / 'harnesses.log'})")
        if result["covers"] != (covers, covers):
            return fail(7, f"{name}: expected {covers} of {covers} witnesses satisfied, "
                           f"the tool reports {result['covers'][0]} of {result['covers'][1]}")
        print(f"  {name:<48} verified, {result['checks']} checks, "
              f"{covers} of {covers} witnesses")
        witnesses += covers
    if code != 0:
        return fail(7, f"the tool exited with {code} although every harness is verified")

    # --- 4. coverage -----------------------------------------------------
    code, output, _ = prooflib.run_bounded(
        [*kani, "--coverage", "-Z", "source-coverage"], PROOFS, TIME_LIMIT_SECONDS
    )
    (out / "coverage.log").write_text(output)
    sections = prooflib.kani_sections(output)
    if code != 0 or sorted(sections) != sorted(listed):
        return fail(8, f"the coverage run did not report every harness (see {out / 'coverage.log'})")
    function = re.search(
        r"^\S+ \(mqttiumd_core::credits::release_if_current\)\n((?: \* .*\n)+)",
        sections["p1_total_specification"], flags=re.M,
    )
    regions = function.group(1).splitlines() if function else []
    covered = [region for region in regions if region.endswith(" COVERED")]
    if not regions or len(covered) != len(regions):
        return fail(8, f"release_if_current: {len(covered)} of {len(regions)} regions covered "
                       f"under p1 (see {out / 'coverage.log'})")
    recorded = {(PRODUCT / file).resolve() for file in RECORDED_FILES}
    reached = set()
    for section in sections.values():
        for path in re.findall(r"^(\S+\.rs) \(", section, flags=re.M):
            file = (PROOFS / path).resolve()
            # Files of the product and of the proof; the standard library
            # belongs to the tool's compiler, which the preflight pins.
            if PRODUCT in file.parents:
                reached.add(file)
    unrecorded = sorted(str(file.relative_to(PRODUCT)) for file in reached - recorded)
    if unrecorded:
        return fail(8, f"the proof reaches source files whose digest is not recorded: {unrecorded}")

    # --- 5. the digests, last --------------------------------------------
    current = prooflib.digests()
    if record:
        RECORD.write_text(current)
    recorded_text = RECORD.read_text() if RECORD.exists() else ""
    if current != recorded_text:
        changed = sorted(set(current.splitlines()) ^ set(recorded_text.splitlines()))
        return fail(5, "the sources are not the recorded ones. Lines that differ:\n  "
                    + "\n  ".join(changed)
                    + "\nEverything else passed. Review the change, then run with --record "
                      "and update PROOF.md.")

    print("proof: PASS")
    print("  function        mqttiumd_core::credits::release_if_current")
    for line in current.splitlines():
        kind, digest, name = line.split(" ", 2)
        print(f"  {kind:<15} sha256:{digest} {name}")
    print(f"  no allocation   {counted.group(1)} tests passed: one call per outcome, 0 allocations,\n"
          "                  product toolchain, test profile")
    print(f"  harnesses       {len(listed)} run by the tool, {len(listed)} listed, all verified")
    print(f"  witnesses       {witnesses} of {witnesses} cover properties satisfied")
    print(f"  coverage        {len(covered)} of {len(regions)} regions of the function covered (p1, no\n"
          f"                  assumption); {len(reached)} source files reached, all recorded")
    print(f"  options         cargo kani -p {PACKAGE} (defaults, no filter);\n"
          "                  coverage: --coverage -Z source-coverage")
    print("  bounds          every field symbolic over its whole type (u64, u32, both\n"
          "                  states); no loop, no unwinding bound, no stub, no contract")
    print(f"  logs            {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
