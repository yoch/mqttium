# L6 — Pilot proof

Depends on: L0. Runs with: L1, L2, L3, L5. Its reception needs the call
site of L1.

## Goal

One function that the product really executes, proved to release exactly
and never to mutate partially, and a measured cost of keeping that proof.

## Requirements

DEM-021; V3-019; ARC-027; RES-001. Decision records 0006, 0007.

## The function

`release_if_current`, in `crates/mqttiumd-core/src/credits.rs`. It takes a
lease record, an account and a release key.

| Condition | Required result |
| - | - |
| Stale key | No mutation |
| Repeated release of a released lease | No second subtraction |
| Wrong account | Explicit error, no change |
| Cost above a usage | Explicit error, neither dimension changed |
| Current key and coherent account | Exact subtraction of bytes and slots, then the state `Released` |
| Old key after the slot was reused | The new reservation unchanged |

Both subtractions are checked before any mutation. The function does not
allocate and does no IO.

## Deliverables

In this order: the preflight, then the types and the real function with
its tests, delivered early to L1, then the proof.

0. **A preflight**, versioned in the repository and run by one command. On
   the real production dependency (the proof crate depending on
   `mqttiumd-core` as the workspace builds it) a true property passes and
   a deliberately false one fails. It checks the tool that `PATH` actually
   selects, and its version.
1. **The function, its types and its unit tests.** Early, as a contract
   change for the types, so that L1 can call it. L1 writes no second
   variant.
2. **A Kani harness** that calls the production body with symbolic values,
   integer limits included. Assumptions must not exclude the error paths;
   separate harnesses cover incoherent accounts and check that their branch
   mutates nothing. Witnesses of non-vacuity, with the coverage checks of
   the tool, show that each branch of the table is reached under the
   assumptions: an assumption that makes an error path unreachable does
   not verify it. If the encapsulation of the types prevents the harness
   from building a state the proof needs, the lot says so and treats it
   explicitly.
3. **Where the harness lives.** Prefer a separate crate under `proofs/`
   that depends on the core and calls the public function: the pure crates
   then need no proof attribute. If a proof tool needs an attribute, a
   macro or a dependency inside a pure crate, that is a decision that
   extends a list of the boundary (record 0007).
4. **A Verus trial**, limited: on the same executable definition, or its
   documented dismissal. A hand-made "equivalent" copy does not meet the
   requirement. The reception of the lot rests on the Kani proof.
5. **Mutants** of the function, each caught by the proof or by a test for
   its own reason: a guard dropped, one dimension subtracted before the
   other is checked, the generation ignored, a release repeated. The
   result shows the violation of the property aimed at; a compilation
   failure or a timeout is not a detection.
6. **A maintenance trial**: an ordinary change of cost or of structure,
   with the human time, the tool time and the adaptations it took.
7. **The file of the proof**, under `proofs/credits/`: properties, the
   exact content of the body by digest, preconditions, functions assumed or
   excluded, configuration, results, mutants.

## Tooling

`tools/install-tools.sh kani verus` installs both at pinned versions:
Kani 0.68.0 (CBMC 6.11.0) and Verus 0.2026.11.02.8bf68ee, whose release
archive is checked against a pinned digest. Both were installed and tried
in the cloud environment used so far, with no change to its network
settings: each verified a true property and refuted a deliberately false
one, and a Kani harness in a separate crate verified functions of
`mqttiumd-contract` as that crate is written.

Each tool brings its own compiler: Kani a pinned nightly, Verus the
toolchain 1.99.1. Neither builds the product, which stays on the toolchain
of `rust-toolchain.toml`: the ordinary build and the analysis under the
tool's compiler are two checks that complete each other. The file of the
proof records, for each run, the revision of the code, the tool and its
version, the compiler the tool brings, the target, the options and the
bounds covered. A proof that was not run is not reported as run.

The proof may be developed locally. It is received on a reproducible run
in CI.

Two defects of `tools/install-tools.sh` are corrected when this lot
starts, through the integration owner: `present()` ignores the exit code
of the command it runs and accepts the pinned version as a substring of
any output; and nothing checks that the tool found is the one `PATH`
selects for the proof.

No change of network settings is needed.

## Reception

- The proof is on `release_if_current` as the product executes it, with the
  call site of L1 named. A proof of a separate demonstration function does
  not receive the lot. The final reception comes after that call site
  exists.
- The proof runs in CI, reproducibly, on the production function: the
  same command gives the same verdict on the proposed revision.
- The error cases and the integer limits are among the proved cases, and
  the witnesses of non-vacuity are reached.
- Every row of the table above has a property that the harness checks.
- Every mutant is caught for its own reason.
- The report lists every assumption, bound and excluded function.
- The maintenance trial is reported with its measured cost.

This proof protects the calculation and its guards. It does not prove that
the call happens at the right moment of the protocol: MU-05 and the
integration traces still have to catch a premature release.

## Closes

In `../reception/open-conditions.md`: the condition of "Pilot proof".
