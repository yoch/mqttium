# The pilot proof of `release_if_current`

Lot L6. Requirements DEM-021, V3-019, ARC-027, RES-001; decision records
0006 and 0007.

This file is the record of the proof: what is proved, on which exact
content, under which assumptions, with which tool, and what is not proved.
The harnesses, the scripts and their output are the evidence; this file is
the guide to them.

**State: not received.** The proof runs locally. Two conditions of the
reception are not met yet: no call site of lot L1 exists, and nothing here
runs in CI. See "What is not done".

## The function

`mqttiumd_core::credits::release_if_current`, in
`crates/mqttiumd-core/src/credits.rs`. The proof crate depends on
`mqttiumd-core` by path and calls this function; there is no copy.

| | |
| - | - |
| Digest of `credits.rs` | `sha256:49669d713e09854672a29a7e180dd4ccfbc8caaa6cf1cce228cccd16fc9082f1` |
| Digest of the text of the function | `sha256:f1f4d430f508faa837db8c82d3ecb841bfc9c72f4a18c62f2a67969a83bd5cd1` |
| Recorded in | `BODY.sha256`; `run.sh` fails when the source differs |
| Call site in the product | None yet. Lot L1 calls it and writes no second variant (DEM-024) |

The text of the function is from its `pub fn` line to the first closing
brace in column one. After a change of the source, review it, run
`run.sh --record` and update this file.

## Commands

All from `mqttiumd/`.

| Command | What it does |
| - | - |
| `tools/install-tools.sh kani verus` | Installs the pinned tools |
| `proofs/preflight.sh` | The tool `PATH` selects, its version, a true property verified, a false one refuted |
| `proofs/preflight-selftest.sh` | Fake tools first on `PATH` are refused; the real preflight passes |
| `proofs/credits/run.sh` | The preflight, the digests, the eleven harnesses with their witnesses, the coverage |
| `python3 proofs/credits/mutants.py` | The control run and the twelve mutants |
| `python3 proofs/credits/verus-trial.py` | The limited Verus trial |

## Properties

Every row of the DEM-021 table has a harness of its own, and harness `p1`
states the whole table with no assumption. `Pna` names an assertion, `Wna`
a witness of non-vacuity (a cover property).

| DEM-021 condition | Required result | Harness | Checks |
| - | - | - | - |
| Stale key | No mutation | `p2_stale_key_mutates_nothing` | P2b nothing changes; P2a the result is `Stale` |
| Repeated release of a released lease | No second subtraction | `p3_repeated_release_subtracts_once` | P3a a second call changes nothing; P3c the lease is not held; P3b the result is `AlreadyReleased` |
| Wrong account | Explicit error, no change | `p4_wrong_account_is_refused_whole` | P4b nothing changes; P4a the result is `WrongAccount` |
| Cost above a usage | Explicit error, neither dimension changed | `p5_cost_above_usage_changes_neither_dimension` | P5b bytes unchanged; P5c slots unchanged; P5d still held; P5e nothing changes; P5a the result is `CostExceedsUsage` |
| Current key and coherent account | Exact subtraction of bytes and slots, then `Released` | `p6_current_key_releases_exactly` | P6b bytes reduced exactly; P6c slots reduced exactly; P6d released; P6e limits, identities and cost unchanged; P6f still coherent; P6a the result is `Released` |
| Old key after the slot was reused | The new reservation unchanged | `p7_old_key_leaves_the_reused_slot` | P7a the first lease is released; P7c the new reservation and its account are unchanged; P7d it is still held; P7b the result is `Stale` |
| The whole table, no assumption | Each outcome and final state, and no other outcome | `p1_total_specification` | P1a to P1l |
| Incoherent account, refusal | No mutation | `p8_incoherent_account_refusal_mutates_nothing` | P8a |
| Incoherent account, release | Exact, by the right key only | `p9_incoherent_account_release_is_exact` | P9a, P9b, P9c |
| Coherent account | `used <= limit` preserved by every outcome | `p10_coherence_is_preserved` | P10a |
| Reserve then release | The account as it was | `p11_reserve_then_release_restores_the_account` | P11a, P11b |

A failed assertion ends its path, as a panic does: a later assertion is
examined only where the earlier ones hold. So each harness asserts the
state first and the returned value last. The first run of the mutants
showed why: with the returned value first, a mutation of the state was
reported as a wrong return value only.

The tool also checks, in the production body and in everything it calls,
its default properties (arithmetic overflow, invalid values, panics). None
fails; they are among the checks counted below.

## Preconditions and assumptions

The function has no precondition: `p1`, `p3` and `p11` assume nothing about
the lease, the account or the key.

| Harness | `kani::assume` | Why it does not hide an error path |
| - | - | - |
| `p1` | None | |
| `p2` | The key differs from the lease's key | The row is about stale keys. W2a to W2c show each component differing alone; W2e shows a key that only its generation keeps from a real subtraction |
| `p3` | None | |
| `p4` | The account presented is not the lease's | W4a: that account could have paid; W4b: the lease was already released |
| `p5` | Right account, held lease, at least one cost above its usage | W5a to W5c: each dimension short alone, and both; W5d, W5e: one unit short at both ends of the range |
| `p6` | Coherent account, right account, held lease, both costs within the usage | W6a: both costs `u64::MAX`; W6b: zero cost; W6c: full account; W6d: every identity at its largest; W6e: something stays in use |
| `p7` | The new generation differs from the old one; both `reserve` calls succeed | W7b: the next generation; W7c: a generation that went backwards |
| `p8`, `p9` | The account is incoherent (`used > limit` in a dimension) | W8a to W8d: every refusal is reached; W9a, W9b: a release leaves the account incoherent, or makes it coherent; W9c, W9d: each dimension over its limit alone |
| `p10` | The account is coherent | W10a: a release; W10b: a refusal |
| `p11` | `reserve` succeeds | W11a: a cost in both dimensions |

`run.sh` requires, for each harness, exactly the number of witnesses of
`harnesses.txt`, all satisfied. An assumption that made a branch
unreachable would fail the run.

## Bounds

Every field is symbolic over its whole type: all of `u64` for the three
identities of the key, the account identity, both costs, both usages and
both limits; all of `u32` for the slot; both lease states. There is no
loop and no recursion, so no unwinding bound. No value is restricted to a
small range.

## Functions assumed or excluded

None. No stub, no function contract, no `kani::assume` inside production
code. The body is analysed with everything it calls: the derived
comparison of `LeaseKey` and `checked_sub` of the contract crate.

The states are built with the public `from_parts` constructors, which
check nothing. Without them the proof crate could not state an incoherent
account, or a held lease that its account never counted: the
encapsulation of the types would have excluded those states from the
proof. The constructors are also what recovery needs.

What the result rests on, and is not proved here:

- Kani and CBMC, and the compiler Kani brings. The product is built by
  another compiler (`rust-toolchain.toml`); the unit tests run under that
  one, the proof under Kani's. The two checks complete each other; neither
  shows that the two compilers agree.
- The harness reads the state through the public getters.
- "Does not allocate, does no IO" is a check of the source by `run.sh`
  (the module names no path into `alloc` or `std`, no collection, no
  formatting macro) in a `no_std` crate. It is not a Kani property.
- `Account::reserve` is exercised by `p7` and `p11` only. It is not the
  function of the pilot proof.
- The moment of the call. This proof protects the calculation and its
  guards. MU-05 and the integration traces still have to catch a release
  that comes too early.

## Runs

A result belongs to an exact content.

| | |
| - | - |
| Revision | `b0d6b940` on branch `claude/mqttiumd-l6-pilot-proof`: the lot merged with the contract chain `74ea7f4..9465451`, which sits on `642b376` of the integration branch. Later commits change this file and `proofs/proposals/` only |
| Contract digest | `sha256:7a869371e38d0bd344e49bf8928905ef0d59cebe932f7df3ce0cb2530bfb47cb`, unchanged by the lot |
| Where | A cloud container, local run. Not CI |
| Tool | `/root/.cargo/bin/cargo-kani`, as `PATH` selects it: Kani Rust Verifier 0.68.0 (cargo plugin), CBMC 6.11.0 |
| Compiler the tool brings | `rustc 1.100.0-nightly (8925ea358 2026-08-20)`, toolchain `nightly-2026-08-21-x86_64-unknown-linux-gnu` |
| Target | `x86_64-unknown-linux-gnu` |
| Options | `cargo kani -p mqttiumd-proof-credits --harness <name>`, defaults. Coverage: `--coverage -Z source-coverage` on `p1` |
| Product toolchain, for the unit tests | `1.97.0`, `rust-toolchain.toml` |

`proofs/credits/run.sh`: PASS.

| Harness | Verdict | Checks | Witnesses |
| - | - | - | - |
| `p1_total_specification` | verified | 256 | 6 of 6 |
| `p2_stale_key_mutates_nothing` | verified | 244 | 5 of 5 |
| `p3_repeated_release_subtracts_once` | verified | 245 | 2 of 2 |
| `p4_wrong_account_is_refused_whole` | verified | 244 | 2 of 2 |
| `p5_cost_above_usage_changes_neither_dimension` | verified | 135 | 5 of 5 |
| `p6_current_key_releases_exactly` | verified | 274 | 5 of 5 |
| `p7_old_key_leaves_the_reused_slot` | verified | 360 | 3 of 3 |
| `p8_incoherent_account_refusal_mutates_nothing` | verified | 270 | 4 of 4 |
| `p9_incoherent_account_release_is_exact` | verified | 271 | 4 of 4 |
| `p10_coherence_is_preserved` | verified | 237 | 2 of 2 |
| `p11_reserve_then_release_restores_the_account` | verified | 305 | 1 of 1 |

Eleven harnesses verified, none failed; 39 of 39 witnesses satisfied; 12
of 12 regions of the function covered under `p1`. The whole run, preflight
included, takes about twenty seconds, from an empty build directory as
well.

Also run on that revision: `cargo fmt --check`, `cargo clippy --workspace
--all-targets --locked -- -D warnings`, `cargo test --workspace --locked`
(151 passed, 17 of them in `credits`), `validate`, each on the product
workspace; `cargo fmt --check` and `cargo clippy` on the `proofs/`
workspace; `proofs/preflight-selftest.sh` (four fake tools refused, the
real preflight passes); `tools/test-install-tools.sh` and
`tools/install-tools.sh kani verus` with the corrected installer of the
integration branch (both tools present).

The proof crates are a workspace of their own, so that the product
workspace and its lock file carry nothing of the proof. The price:
`cargo test --workspace` and `cargo clippy --workspace` at the root of
`mqttiumd/` do not see `proofs/`. The scripts of the table of commands
are the only verification of this directory; they count only where they
run.

## Mutants

`python3 proofs/credits/mutants.py`: PASS, about two minutes. The sources
are copied to a temporary directory; the production tree is not touched.
The control run comes first: the unmodified copy gives eleven harnesses
verified and seventeen unit tests passed.

A mutant is caught only when its text was found exactly once in the
function, the mutated crate still builds with the product toolchain, every
harness ran to a verdict within the time limit, the checks it aims at
fail, the checks it must leave alone hold, and its unit test fails. A
compilation failure or a timeout is reported as inconclusive.

| Mutant | What it does | Checks aimed at, all failed | Other failed checks | A unit test that fails |
| - | - | - | - | - |
| `generation-ignored` | A key is current when its account and slot match | P2b, P2a, P7c | P1a, P1b, P9a | `a_stale_key_mutates_nothing` |
| `stale-guard-dropped` | The key is not compared at all | P2b, P2a | P1a, P1b, P7c, P9a | `a_stale_key_mutates_nothing` |
| `account-guard-dropped` | Any account pays for the lease | P4b, P4a | P1c, P1d, P9a | `a_wrong_account_is_an_explicit_error_and_changes_nothing` |
| `release-repeated` | A released lease is subtracted again | P3a, P3b | P1e, P1f, P9a | `a_repeated_release_subtracts_nothing_more` |
| `state-not-released` | The usage is reduced and the lease stays held | P6d, P3a, P3c | P1l, P9c | `a_current_key_subtracts_both_dimensions_exactly_and_releases` |
| `bytes-check-dropped` | The bytes are subtracted unchecked and wrap | P5b | P1g, P1h, P9b, P10a | `bytes_above_usage_change_neither_dimension` |
| `slots-check-dropped` | The slots are subtracted unchecked and wrap | P5c | P5b, P1j, P9b, P10a | `slots_above_usage_change_neither_dimension` |
| `bytes-subtracted-before-slots-checked` | One dimension is written before the other is checked | P5b | P1j, P3a, P8a | `slots_above_usage_change_neither_dimension` |
| `state-written-before-checks` | The lease is marked released before the checks. P5b and P5c still hold | P5d | P1h, P1j, P8a | `bytes_above_usage_change_neither_dimension` |
| `slots-not-subtracted` | Only the bytes are given back. P6b still holds | P6c | P1l, P9c, P11b | `a_current_key_subtracts_both_dimensions_exactly_and_releases` |
| `bytes-subtracted-from-the-limit` | The cost is taken from the limit | P6b | P1g, P1h, P1i, P1l, P5b, P5c, P5d, P9b, P9c, P11b | `a_current_key_subtracts_both_dimensions_exactly_and_releases` |
| `account-checked-before-the-key` | The first two guards are swapped. P2b still holds | P2a | P1a | `an_old_key_of_another_account_is_stale_not_a_fault` |

Twelve mutants, twelve caught by the check aimed at and by a unit test;
none survived, none inconclusive. The script prints every unit test that
fails for each mutant; the table names one.

The last mutant protects a decision of the function: a stale key is
examined before the account. After a slot is reused, the old key may name
another account than the new reservation; that is a late release, not a
fault.

## The order of the guards

The function examines four conditions in a fixed order. The order is a
decision; each rank has a case that another order would break.

| Rank | Guard | Why here | What another order breaks |
| - | - | - | - |
| 1 | The key is not the key of the record: `Stale` | A key says which reservation the caller means. Until it matches, nothing the record or the account holds concerns the caller | After a slot is reused, an old key may name another account than the new reservation. With the account examined first, that late release would be `WrongAccount`, a fault, where it is a release that came late. Mutant `account-checked-before-the-key`, check P2a, witness W2c |
| 2 | The account is not the lease's: `WrongAccount` | The key is current, so the caller means this lease, and presented an account it is not charged to. That is a fault of the caller whatever the state of the lease | With the state examined first, a released lease presented with a wrong account would answer `AlreadyReleased`, and the fault would go unseen. Unit test `a_wrong_account_is_reported_for_a_released_lease_too`, check P4a with witness W4b |
| 3 | The lease is released: `AlreadyReleased` | Before any arithmetic: a released lease owes nothing, so its cost is never compared with a usage | With the subtractions examined first, a repeated release from an account that no longer holds the cost would answer `CostExceedsUsage`, a fault, for a harmless repetition; with the guard dropped, the cost is subtracted twice. Mutant `release-repeated`, checks P3a and P3b |
| 4 | Both subtractions, then the writes | Both differences are computed and checked before either usage or the state is written | Any write placed before the last check is a partial mutation on a refusal. Mutants `bytes-subtracted-before-slots-checked` and `state-written-before-checks`, checks P5b and P5d |

Bytes are examined before slots. When both exceed their usage the error
names the bytes; nothing depends on which is named, and nothing is
written in either case.

An account whose usage exceeds a limit is not refused. A release only
lowers a usage, so refusing it would keep an incoherent account
incoherent for longer; the release is exact or refused whole (`p8`,
`p9`).

## The Verus trial

`python3 proofs/credits/verus-trial.py`: PASS, about four seconds. Verus
0.2026.11.02.8bf68ee, toolchain 1.99.1, as `PATH` selects it.

It is a trial. The reception rests on the Kani proof, and Verus is
dismissed for this lot, for the reasons below.

| Step | Result |
| - | - |
| The unannotated contract crate | Refused: Verus needs its library imported by the crate |
| A temporary copy, annotated mechanically | 24 lines added in 5 files; the text of the function is checked to be the production one |
| The annotated contract crate | 36 verified, 0 errors |
| The annotated core crate, with the postcondition "a call that does not return `Released` leaves the lease and the account as they were" | 5 verified, 0 errors, 0.8 s |
| The opposite postcondition | Refuted: postcondition not satisfied |
| Mutants `state-written-before-checks` and `bytes-subtracted-before-slots-checked` | Each refuted: postcondition not satisfied |

So Verus does verify the same executable definition, and refutes what is
false about it. What it costs:

- `use vstd::prelude::*;` in both pure crates and `#[verus_verify]` on
  every type the function touches: six in `credits.rs`, and the types and
  `checked_sub` of the contract crate, inside its macros. Those are files
  of lot L1.
- The attributes are procedural macros of Verus's library. The product
  would need that library as a dependency of the two pure crates to build
  at all. ADR 0007 allows no dependency and lists neither attribute;
  admitting them is a decision that names the dependency and its scope.
- The postcondition of a public function cannot name a private field
  ("field expression for an opaque datatype"). The exact subtraction
  could not be stated with attributes alone: it needs specification
  functions, which need the `verus!` macro, or public fields.
- The stale-key rows need a specification of the derived comparison of
  `LeaseKey`.

Not tried: `cargo verus`, the `verus!` macro, a specification of the
exact subtraction.

## The maintenance trial

An ordinary change of structure: a third dimension, `References`, added
to `Credits`, to `reserve` and to `release_if_current`. It was made in a
scratch copy and is not part of the lot: it would change the contract of
`credits.rs`.

| Step | Size | Clock, from the start |
| - | - | - |
| The product change, with three unit tests; 20 tests pass | `credits.rs` +80 −14 | 31 s |
| The unadapted proof on the changed product: `run.sh` refuses the unrecorded body (exit 5); Kani does not build the harnesses (`Credits::new` takes three arguments) | | 39 s |
| The proof adapted: three fields of the plain state, one branch of `p1`, one assumption and one assertion in `p5` and `p6`, two witnesses | `src/lib.rs` +55 −4, `harnesses.txt` 2 lines, `BODY.sha256` re-recorded | 66 s |
| `run.sh --record`: 11 harnesses verified, 41 of 41 witnesses, 14 of 14 regions | Tool time 21 s | 87 s |
| The mutants adapted, one added (`references-check-dropped`): 13 of 13 caught | `mutants.py` +21 −1; tool time 182 s | 279 s |

**No human time was measured.** The change and the adaptations were made
by the coding agent of this lot, and the clock above is its wall clock,
tool time included. V3-019 asks for the human time of review; that
measurement is not done.

What the trial shows: the unadapted proof did not pass silently, the
adaptation was mechanical and smaller than the product change, and the
tool time stayed in seconds for the proof and minutes for the mutants.
The mutants that replace a whole statement had to be re-pointed when the
statement changed; `mutants.py` reports a text it cannot find as
inconclusive, so a stale mutant cannot count as caught.

## What is not done

- **No call site.** Lot L1 has not called the function yet. The reception
  names that call site.
- **Not in CI.** The workflow is a shared file. Until a job installs the
  pinned tools and runs `proofs/credits/run.sh`, every result above is a
  local run.
- **The contract change is not reviewed yet.** The chain
  `74ea7f4..9465451` waits for the integration owner.
- **Human time** of the maintenance trial.
- The open condition "Pilot proof" of
  `docs/reception/open-conditions.md` stays open.
