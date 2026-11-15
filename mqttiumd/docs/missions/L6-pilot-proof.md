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

1. **The function, its types and its unit tests.** First, as a contract
   change for the types, so that L1 can call it. L1 writes no second
   variant.
2. **A Kani harness** that calls the production body with symbolic values,
   integer limits included. Assumptions must not exclude the error paths;
   separate harnesses cover incoherent accounts and check that their branch
   mutates nothing.
3. **Where the harness lives.** Prefer a separate crate under `proofs/`
   that depends on the core and calls the public function: the pure crates
   then need no proof attribute. If a proof tool needs an attribute, a
   macro or a dependency inside a pure crate, that is a decision that
   extends a list of the boundary (record 0007).
4. **A Verus trial** on the same executable definition, or its documented
   dismissal. A hand-made "equivalent" copy does not meet the requirement.
5. **Mutants** of the function, each caught by the proof or by a test for
   its own reason: a guard dropped, one dimension subtracted before the
   other is checked, the generation ignored, a release repeated.
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
of `rust-toolchain.toml`. The file of the proof records the versions used.
A proof that was not run is not reported as run.

## Reception

- The proof is on `release_if_current` as the product executes it, with the
  call site of L1 named. A proof of a separate demonstration function does
  not receive the lot.
- Every row of the table above has a property that the harness checks.
- Every mutant is caught for its own reason.
- The report lists every assumption, bound and excluded function.
- The maintenance trial is reported with its measured cost.

This proof protects the calculation and its guards. It does not prove that
the call happens at the right moment of the protocol: MU-05 and the
integration traces still have to catch a premature release.

## Closes

In `../reception/open-conditions.md`: the condition of "Pilot proof".
