# The pilot proof of `release_if_current`

Lot L6. Requirements DEM-021, V3-019, ARC-027, RES-001; decision records
0006 and 0007, and record 0010 (proposed), which holds the decisions of
this lot on the interface of `credits.rs`.

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
| Text of the function | `sha256:f1f4d430f508faa837db8c82d3ecb841bfc9c72f4a18c62f2a67969a83bd5cd1` |
| `crates/mqttiumd-core/src/credits.rs` | `sha256:cd9e17b4a221265bfab203232e57e54521adb40943ad812e586342e852ce7f22` |
| `crates/mqttiumd-contract/src/capacity.rs` | `sha256:7137872566aeb6892d00a8134b1ba1908c16c89dd75cf1c2790fa4ec2f9e235b` |
| `crates/mqttiumd-contract/src/ids.rs` | `sha256:c7ab95d573bee210f8f2668c4f13770e68ed93a95e9c704b33e63838a317679e` |
| `proofs/credits/src/lib.rs`, the harnesses | `sha256:cff057a4624567777a460fb728f6283b48272400e5197eb11633013aa859f605` |
| `proofs/credits/harnesses.txt` | `sha256:54f85cc0270a7956d07fceaa2914f5741982b6567503d3fd380cdeca64c17ca1` |
| `proofs/credits/tests/no_allocation.rs` | `sha256:f63b2706874092e4de47409d38d0efe883b9ed813d8c4dc7979d935f0d16e558` |
| Recorded in | `BODY.sha256`; `run.sh` fails with exit 5 when any of them differs |
| Call site in the product | None yet. Lot L1 calls it and writes no second variant (DEM-024) |

The text of the function is from its `pub fn` line to the first closing
brace in column one.

The record covers every source file the proof analyses, not the function
alone. The two files of the contract crate hold what the body calls:
`checked_sub` and the derived comparison of `LeaseKey`. `run.sh` uses the
coverage of the tool to check that no harness reaches another source file
of the product, so the list cannot fall short without failing the run.
The standard library is not recorded: it belongs to the compiler Kani
brings, which the preflight pins.

A consequence for lot L1, which owns `capacity.rs` and `ids.rs`: any
change of those two files makes `run.sh` fail until the change is
reviewed and recorded with `run.sh --record`, which writes the record
last, after the proof passed on the new content. That is the purpose:
a change of what the proof analyses asks for a look at the proof.

## Commands

All from `mqttiumd/`.

| Command | What it does |
| - | - |
| `tools/install-tools.sh kani verus` | Installs the pinned tools |
| `proofs/preflight.sh` | The tool `PATH` selects, its version, a true property verified, a false one refuted |
| `proofs/preflight-selftest.sh` | Fake tools first on `PATH` are refused; the real preflight passes |
| `proofs/credits/run.sh` | The preflight, the check that one file alone uses `unsafe`, the allocation test, the twelve harnesses with their witnesses, the coverage, the digests. `--record` records the digests after a pass |
| `python3 proofs/credits/mutants.py` | The control run and the twelve mutants |
| `python3 proofs/credits/selftest.py` | Breaks one thing the scripts check, per case, and expects the refusal |
| `python3 proofs/credits/verus-trial.py` | The limited Verus trial |

`run.sh` is a wrapper of `run.py`. `prooflib.py` is the one place that
locates the function, computes the digests, reads the tool's output and
defines the mutants; the four scripts import it.

The proof crates are a workspace of their own, so that the product
workspace and its lock file carry nothing of the proof. The price:
`cargo test --workspace` and `cargo clippy --workspace` at the root of
`mqttiumd/` do not see `proofs/`. The commands above are the only
verification of this directory; they count only where they run.

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
| The precondition, broken on purpose | What the function then does | `p12_a_second_record_of_a_held_lease_is_released_too` | P12a, P12b; see "Preconditions and assumptions" |

A failed assertion ends its path, as a panic does: a later assertion is
examined only where the earlier ones hold. So each harness asserts the
state first and the returned value last. The first run of the mutants
showed why: with the returned value first, a mutation of the state was
reported as a wrong return value only.

The tool also checks, in the production body and in everything it calls,
its default properties (arithmetic overflow, invalid values, panics). None
fails; they are among the checks counted below.

## Preconditions and assumptions

The function has one precondition, and the harnesses cannot establish
it: **each held lease has one record.** An account counts usage and does
not know its leases, so the function cannot tell a second record of a
held lease from the first. `LeaseRecord` is not `Clone`, but `from_parts`
and the getters can rebuild a record; recovery calls `from_parts` once
per durable row, and nothing else does outside this proof crate.

Harness `p12` builds the state the precondition forbids, a second record
of the same held lease, and shows what follows: each release is exact on
its own, both succeed while the usage covers the cost twice (W12a), and
the cost of one lease then has left the account twice (P12b). The usage
is undercounted and the account will admit more than its limit. `p3`
does not cover this: it repeats the release on the same record. The
owner of the lease table, lot L1, owes the guarantee that each record
exists once; it is proposed as an open condition.

Nothing else is required of the arguments: `p1`, `p3` and `p11` assume
nothing about the lease, the account or the key.

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
| `p12` | None; the second record copies the first | W12a: both releases succeed; W12b: the second is refused only when the usage no longer covers the cost |

`run.sh` runs the tool on the whole proof crate with no filter and takes
the harnesses from the tool's own report: they must be exactly those of
`harnesses.txt`, each with exactly its listed number of witnesses, all
satisfied. A harness nobody listed fails the run, and so does an
assumption that makes a branch unreachable.

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
- "Does not allocate" is a test, not a Kani property:
  `tests/no_allocation.rs` installs a counting global allocator, calls
  the function once per outcome and `reserve` once per outcome, and
  requires zero allocations; a control shows the counter sees one. It
  runs under the product toolchain, in the test profile, on the calls it
  makes. The proof shows these are all the outcomes; it does not show
  that no input allocates.
- That test is the only file of `proofs/` that lifts the ban on `unsafe`:
  a global allocator cannot be written without it. The proof workspace
  denies `unsafe` everywhere else, and `run.sh` fails (exit 10) when any
  other Rust file under `proofs/` names it. The product crates forbid
  `unsafe` and are not concerned.
- "Does no IO" has no check of its own. The crate is `no_std` and the
  boundary test of the product refuses what could reach a file or a
  socket (ADR 0007).
- `Account::reserve` is exercised by `p7` and `p11` only. It is not the
  function of the pilot proof.
- The moment of the call. This proof protects the calculation and its
  guards. MU-05 and the integration traces still have to catch a release
  that comes too early.

## Runs

A result belongs to an exact content.

| | |
| - | - |
| Revision | The commit that records this run, on branch `claude/mqttiumd-l6-pilot-proof`: its parent `23334904` merges the integration branch at `807ca26` (the contract chains of lots L1 and L2), and it changes only `BODY.sha256`, this file and the CI proposal |
| Contract digest | `sha256:7a869371e38d0bd344e49bf8928905ef0d59cebe932f7df3ce0cb2530bfb47cb`, unchanged by the lot |
| Where | A cloud container, local run. Not CI |
| Tool | `/root/.cargo/bin/cargo-kani`, as `PATH` selects it: Kani Rust Verifier 0.68.0 (cargo plugin), CBMC 6.11.0 |
| Compiler the tool brings | `rustc 1.100.0-nightly (8925ea358 2026-08-20)`, toolchain `nightly-2026-08-21-x86_64-unknown-linux-gnu` |
| Target | `x86_64-unknown-linux-gnu` |
| Options | `cargo kani -p mqttiumd-proof-credits`, defaults, no harness filter. Coverage: the same with `--coverage -Z source-coverage` |
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
| `p12_a_second_record_of_a_held_lease_is_released_too` | verified | 253 | 2 of 2 |

Twelve harnesses run by the tool, twelve listed, all verified; 41 of 41
witnesses satisfied; 12 of 12 regions of the function covered under `p1`;
4 source files reached, all recorded; 3 allocation tests passed. The
whole run, preflight included, takes 44 seconds from an empty build
directory, and 40 to 50 seconds here on a warm one.

Also run on that revision: `cargo fmt --check`, `cargo clippy --workspace
--all-targets --locked -- -D warnings`, `cargo test --workspace --locked`
(234 passed, 18 of them in `credits`), `validate`,
`python tools/build_corpus.py --check` and `tools/test-install-tools.sh`
on the product workspace; `cargo fmt --check` and `cargo clippy` on the
`proofs/` workspace; `proofs/preflight-selftest.sh` (four fake tools
refused, the real preflight passes).

## What a PASS of the scripts is worth

`python3 proofs/credits/selftest.py`: 16 of 16 cases as expected, about
five minutes. Each case copies the sources, breaks one thing and expects
the script of the copy to refuse by the exit code of that check.

| Case | Expected |
| - | - |
| The function allocates through `BoundedVec`, a type no search for `Vec` sees | `run.sh` exit 9 |
| The function allocates with `.to_vec()`, which names no type | exit 9 |
| An unlisted harness behind a second attribute | exit 6 |
| An unlisted harness declared `pub fn` | exit 6 |
| A listed harness that the source no longer holds | exit 6 |
| `checked_sub` changes in the contract crate | exit 5 |
| An assertion of a harness is weakened, and still passes | exit 5 |
| An unknown argument | exit 2 |
| `--record` on a body that is not proved | exit 7, `BODY.sha256` untouched |
| A mutant whose text is gone | Its row says inconclusive; the report says FAIL |
| The mutants exceed their time limit | Inconclusive rows, the report says FAIL |
| A command exceeds its limit after starting a child | The child is killed with it |
| A source file the proof reaches is taken out of the record | exit 8 |
| A second file of `proofs/` allows `unsafe` | exit 10 |
| A harness uses `unsafe` behind an `allow` of its own | exit 10 |
| The unmodified copy | `run.sh` exit 0 |

Most of these were found by the integration owner's review of the
scripts. Before the corrections, 10 of the 12 cases then written failed:
the scripts printed PASS. The self-test was committed in that state
(`cc7ada3`), then the corrections (`144a6d3`).

The two cases for `unsafe` were committed failing as well (`0904456`),
then their correction (`a5786183`). One thing the self-test does not
show: the first form of the timeout case passed whatever the script did,
so it was replaced; the present one was seen to fail when only the
direct child is killed.

## Mutants

`python3 proofs/credits/mutants.py`: PASS, about two and a half minutes. The sources
are copied to a temporary directory; the production tree is not touched.
The control run comes first: the unmodified copy gives twelve harnesses
verified and eighteen unit tests passed.

A mutant is caught only when its text was found exactly once in the
function, the mutated crate still builds with the product toolchain, every
harness ran to a verdict within the time limit, the checks it aims at
fail, the checks it must leave alone hold, and its unit test fails. A
compilation failure or a timeout is reported as inconclusive: the mutant
keeps its row and the run fails. On a timeout the whole process group of
the tool is killed.

| Mutant | What it does | Checks aimed at, all failed | Other failed checks | A unit test that fails |
| - | - | - | - | - |
| `generation-ignored` | A key is current when its account and slot match | P2b, P2a, P7c | P1a, P1b, P9a | `a_stale_key_mutates_nothing` |
| `stale-guard-dropped` | The key is not compared at all | P2b, P2a | P1a, P1b, P7c, P9a | `a_stale_key_mutates_nothing` |
| `account-guard-dropped` | Any account pays for the lease | P4b, P4a | P1c, P1d, P9a | `a_wrong_account_is_an_explicit_error_and_changes_nothing` |
| `release-repeated` | A released lease is subtracted again | P3a, P3b | P1e, P1f, P9a | `a_repeated_release_subtracts_nothing_more` |
| `state-not-released` | The usage is reduced and the lease stays held | P6d, P3a, P3c | P1l, P9c | `a_current_key_subtracts_both_dimensions_exactly_and_releases` |
| `bytes-check-dropped` | The bytes are subtracted unchecked and wrap | P5b | P1g, P1h, P9b, P10a, P12a | `bytes_above_usage_change_neither_dimension` |
| `slots-check-dropped` | The slots are subtracted unchecked and wrap | P5c | P5b, P1j, P9b, P10a, P12a | `slots_above_usage_change_neither_dimension` |
| `bytes-subtracted-before-slots-checked` | One dimension is written before the other is checked | P5b | P1j, P3a, P8a | `slots_above_usage_change_neither_dimension` |
| `state-written-before-checks` | The lease is marked released before the checks. P5b and P5c still hold | P5d | P1h, P1j, P8a | `bytes_above_usage_change_neither_dimension` |
| `slots-not-subtracted` | Only the bytes are given back. P6b still holds | P6c | P1l, P9c, P11b, P12a, P12b | `a_current_key_subtracts_both_dimensions_exactly_and_releases` |
| `bytes-subtracted-from-the-limit` | The cost is taken from the limit | P6b | P1g, P1h, P1i, P1l, P5b, P5c, P5d, P9b, P9c, P11b, P12a, P12b | `a_current_key_subtracts_both_dimensions_exactly_and_releases` |
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
- **Record 0010 is proposed, not accepted.** The interface is on the
  integration branch; the owner of the repository accepts the record.
- **The precondition is not enforced anywhere.** One record per held
  lease is owed by the lease table of lot L1, as an open condition; no
  test of L1 shows it yet.
- **The record goes stale with the contract.** It did once already:
  lot L1 added a variant of `WakeCondition` to `capacity.rs` and the
  identity `OutboxSerial` to `ids.rs`. On the merge `23334904` `run.sh`
  failed with exit 5, every harness still verified; the two additions
  were read (neither touches what the body calls), the proof was run on
  the new content and recorded. The same will happen at the next change
  of those files, and lot L6 records, not the lot that made the change.
- **Human time** of the maintenance trial.
- The open condition "Pilot proof" of
  `docs/reception/open-conditions.md` stays open.
