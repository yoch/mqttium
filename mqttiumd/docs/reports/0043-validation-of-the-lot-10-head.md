# Validation of the lot 10 head (`9dfd568`)

Branch `refactor/lot10-validated-base`, stacked on
`perf/lot10-delivery-owners-sharding`. This report covers the first push of
that branch: the baseline, the proofs made true of the code again, and the
defects of the default path (`--delivery-shards 0`). The sharded mode is the
subject of a later lot; what it lacks is listed, not repaired, here.

The owner set three conditions on 26 November 2026: the sharded mode stays
an option, marked experimental until its proofs exist; the work goes on a new
stacked branch; and no change may cost performance under the gate described
below, measured on the 4-core x86-64 VM of this session.

## Method

Three independent read-only reviews of `9dfd568` came first, one on the
proofs, one on the core and the stores, one on the runtime. Every finding
they reported was then either reproduced by a test, confirmed in the code
by reading, or set aside with its reason. A defect is fixed test first: the
test is committed while it fails, then the fix.

The baseline was replayed on `9dfd568` before any change. All of it passed:

| Check | Result on `9dfd568` |
| - | - |
| `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings` | pass |
| `cargo test --workspace` | 87 binaries, 1,439 tests pass |
| `cargo test --workspace --features mqttiumd-lab/kill-points` | 88 binaries, 1,461 tests pass |
| `cargo test -p mqttiumd --features test-hooks` | 17 binaries, 335 tests pass |
| `validate` of P1-02 | contract digest `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| Corpus, schemas, traces, oracle unit tests, conformance register | pass (637 corpus files, 661 schema documents, 405 traces) |
| Loop mutants, deterministic store and SQLite | every mutant rejected as aimed |
| Kani: preflight, `credits`, `lease-lighten`, mutants, selftest | pass; 12 of 12 mutants caught; 16 of 16 selftest cases |
| TLC, all 14 model directories | every configuration as declared |
| Compatibility matrix against Mosquitto 2.1.4, `K = 0` and `K = 2` | 137 scenarios each: 105 same, 15 refused by the profile, 7 where the reference departs from the standard, 10 unspecified, 0 defect; 34 of 35 expectations pass, 1 not applicable |

The tools that were missing on this machine were installed to run all of it:
the cJSON headers to build the Mosquitto 2.1.4 reference, and Kani 0.68.0
(`tools/install-tools.sh kani`).

## Proofs

**Two mutation scripts no longer mutated anything.** All four anchors of
`tools/run_wildcard_mutants.py` and the `early-window` anchor of
`tools/run_outgoing_qos2_mutants.py` matched no line: the code had moved.
Worse, three of the four wildcard tests they name exercised only the
reference selector compiled for tests, not the product's `RouteIndex`. The
tests now state the rules on the product's selector (the differential test
still compares it with the reference), the anchors aim at the code that
exists, and each mutant was checked to be detected for the reason it names.

**The oracle could not read the hot path of lots 6 to 9.** Recipes 24
(`ClaimedPublication`) and 25 (`MultiClaimedPublication`) were not decoded.
It now reads them, pinned to the bytes the contract crate produces, and
keeps them out of the scope of a verdict, which says so rather than judging
a run it cannot follow.

**Five model READMEs cited code that no longer exists**, removed by the
transport rewrite (`Lanes::*`, `Control::stop`, `group_responses`,
`plan_qos0_continuation`, `State::reserves_packet_id`). The checker only
verified that the cited tests exist. All seven model directories concerned
were read again against the code; the corrections, the statements found
false and the scope of each model (`K = 0`) are in their commits. The
checker now refuses a README that names a type or member no file declares,
and its tests run in CI, which they did not before.

**Invariants true by construction.** In `flow-pressure`, `NoPrematureRefusal`
and `IndependentOnly` were written on a ghost variable assigned inside the
branch whose guard they claimed to check: breaking the guard left them true.
They are replaced by invariants over the observable state, which the same
broken guard now violates. `CloseOnlyWhenPeerOwes` is kept and presented as
the regression guard of its mutant; `Bounded` and `WithinWindow` had no
mutant and have one.

**Replays that no longer replayed.** `writer_group_kill` formed its group by
filling a reply lane it believed held 64 answers; the lane has held 2,048
since `09632cd`, so the group formed only by timing. One replay of
`M1Answers` tested a fault the model does not contain; three citations of
the transport models named tests that do not drive the step. Seven
deterministic tests now drive a connection by hand for the transport models.

**What the models do not cover.** A delivery shard keeps the reactors' wake
hold through its wait, so "no frame waits for a timer" holds for `K = 0`
only; `reactor-wake` now states it and has a configuration for it. No model
describes the sharded mode.

**Text corrected.** The conformance note of MQTT-3.1.2-26 (Topic Alias) was
false and is now partial; the ordering rows of MQTT-4.6.0-5 and -6 say their
evidence is for one owner. Records 0055, 0056 and 0057 stated a wrong magic
(`MQDL` for `MQLG`), a wrong representation of `FrozenRecipe`, a wrong hash
and the wrong tables; record 0066 now names what it changes in 0044 (accepted)
and which of its statements the code does not meet. The register of open
conditions lost two false lines and gained a section.

## Defects of the default path

| Defect | Failing test | Fix |
| - | - | - |
| `LogStore` wrote and acknowledged a frame over the 64 MiB recovery reads, then refused to open (a group of 256 commands of one mebibyte is enough) | `b344b68`, `d4002da` | `6cc03d6`: an oversized group is committed as two; one command that cannot fit is not written |
| `LogStore` refused to open after a power loss that kept the later sectors of a frame and lost the first | `de8b300` | `f5e2340`: such a tail is cut, within the reach of one frame |
| `LogStore`: bytes a failed cut left were zero-filled only as far as the next preallocation, and frames were written at the cursor | `de8b300` | `f5e2340`: the preallocation fills to the end of the file; frames are written at the end of the log |
| `LogStore`: a checkpoint saved while its log could not be replaced left every write unknown until a forced checkpoint | `de8b300` | `f5e2340`: the next write replaces the log first |
| `LogStore`: a failed checkpoint was attempted again by every commit, each writing the whole state | `de8b300` | `f5e2340`: the next attempt waits for the threshold again |
| A scoped map gave its compact storage to the first key it received; after any restart that left a retained message or a queued message, every key of the new run went to the ordered fallback | `b148aff` | `f54c5a3`: a key of a later run takes the compact storage |
| `FlatMap::retain` showed an entry twice to its predicate when a run of entries goes round the end of the table | `112c7bd` | `8c0cdea` |
| The SQLite store handed its state over with commits still in its write-ahead log when a reader was open; the next image deletes that log before its rename | `fd02553` | `e5040cd`: a truncating checkpoint first, refused while a reader holds the log |
| `Deliveries::mutate` counted a packet identifier collision that does not exist | `f3019ca` | `62afa31` |
| A known option refused by an earlier one was reported as "unknown or unavailable option" | `db9f992` | `6b367b4` |

Examined and not changed:

- A log group decides which members need its frame on the state before the
  group. That holds while no member can turn a settled one into a write; a
  debug assertion now says so where it would break (`37b1d1d`).
- `LeaseTable::lighten` releases then restores. `leases.rs` is pinned by the
  `lease-lighten` Kani proof, which proves the accounting across it; an
  assertion would change a pinned file.

## Performance gate

The gate is the one of the layering branch, brought unchanged
(`tools/run_perf_gate.py`, `measure_qos0_delivery.py`, `measure_qos1_pairs.py`,
`count_broker_instructions.sh`; their tests now run in CI). It plays two
`dist` executables, A the reference and B the candidate, pass after pass,
alternating which goes first, with the broker on cores 0 and 1 and the clients
on cores 2 and 3, and compares the broker CPU time per delivered message.
The statistic of a shape is the median over the passes of B/A. Four shapes
decide; `blast` sheds QoS 0 by design and is only reported. Six passes; a
deciding median above 1.05 is played again over twelve passes, and one between
1.03 and 1.05 is followed by an instruction count, which does not decide.

A is `9dfd568` (sha256 `1f1219d7d13a54c5...`), B is `6b367b4`
(`e728c6dd4e9bfaf9...`), the last commit of this push that changes a crate.
4-core x86-64 VM, load average below 0.6 at the start, nothing else running.

| Shape | Store | Median of B/A, 6 passes | Broker CPU per delivery, median A / B (µs) | Verdict |
| - | - | - | - | - |
| QoS 0 bursts, 1 subscriber | SQLite | 0.927 | 5.00 / 4.63 | pass |
| QoS 0 bursts, 8 subscribers | SQLite | 1.000 | 1.94 / 1.97 | pass |
| QoS 1 pairs, 1 pair | volatile | 1.014 | 31.4 / 32.6 | pass |
| QoS 1 pairs, 8 pairs | volatile | 1.005 | 42.1 / 42.8 | pass |
| QoS 0 blast, 1 subscriber | SQLite | 0.820 | 3.50 / 2.82 | reported |
| QoS 0 blast, 8 subscribers | SQLite | 1.005 | 1.68 / 1.71 | reported |

No shape costs more. The two lower ratios on one subscriber are not claimed as
a gain: the gate resolves about 4 % on that shape and nothing in the change
touches the QoS 0 path.

The gate's QoS 1 shapes run on the volatile store, and the log store is where
most of the changes are. Six alternating passes of 8 QoS 1 pairs in `log`
mode, under the same pinning, are reported, not deciding: median B/A 0.962,
ratios 0.958, 0.945, 0.925, 1.011, 1.012, 0.967. A frame is now written after a
seek to the end of the log, one system call beside the `fdatasync` of each
commit.

Idle memory per connection, `tools/arm64_bench/idle_rss_bench.py`, one run per
executable, private dirty bytes per connection (A / B): plain TCP 3,232 / 3,264
at 128 connections, 3,041 / 3,041 at 400, 2,869 / 2,949 at 768; mutual TLS
12,832 / 12,832 at 128 and 11,960 / 11,971 at 400. No structure kept per
connection was changed.

## Reproducibility of reports 0039 to 0041

Their figures cannot be reproduced as stated, and this report does not
replace them; it says why.

- Every cell is one run, with no statistic over runs.
- Report 0041 measured `d07e231`, 47 commits that change the crates before
  `9dfd568`. Reports 0040 and 0041 used a load generator (`arm64_matrix_bench.c`)
  that could write past its output buffer, fixed by `675128d`.
- The reference was Mosquitto 2.1.2, not the pinned 2.1.4, holding its state
  in memory, while mqttiumd in `log` mode synchronised a log per commit: the
  two columns compare different contracts.
- Report 0041 shows the volatile `fanin-32x1-q1` row at 0.04 ms p50 for
  `K = 0` and 365 ms for `K = 2`; record 0066 reports only the gain in `log`
  mode.
- Report 0042, on an x86 VM with Mosquitto 2.1.4 and three repetitions, is
  the best of the set and concludes that the throughput has no monotonic
  order in `K`.

## Records applied while proposed

Records are accepted by the owner, not by this validation. What it found
about each:

| Record | What the validation found | Suggestion |
| - | - | - |
| 0051 Core scaling, indexed routing, credits | Nothing against it; the selector it introduced is now what the routing tests check | Accept |
| 0052 Volatile store and snapshots | Nothing against its decision; the scoped-map defect was in a table it uses | Accept |
| 0053 PUBLISH properties | Nothing against it; the register line that said the properties were refused was false | Accept |
| 0054 Configuration subset | Nothing against it | Accept |
| 0055 Append-only log store | Four recovery and write defects fixed; one image left that recovery cannot tell from a torn tail; magic and default threshold corrected | Accept as amended |
| 0056 Claimed publication | Its description of `FrozenRecipe` was wrong; the oracle did not read its recipes | Accept as corrected |
| 0057 FlatMap and direct volatile store | Wrong hash and tables named; the hot scope defect cancelled the optimisation after a restart | Accept as amended |
| 0064 Lot 8 | Its fail-closed compaction never recovered; amended | Accept as amended; not its figures (report 0039) |
| 0065 Lot 9 | Nothing against its default path | Accept; not its figures (report 0040) |
| 0066 Lot 10 sharding | Departs from 0044 without amending it; defects and suspects listed in the register; figures not reproducible | Keep proposed until the sharded lot |
| 0067 Bounded event details | Nothing against it | Accept |

## What stays open

The section "Validation of the lot 10 head" of
`docs/reception/open-conditions.md`. In short: the sharded mode (no model,
defects found by reading, one intermittent ordering failure), the records
above, the figures of lots 8 to 10, one log image, one takeover path the
model does not contain, two scans of the volatile store, and the oracle's
scope.

## Commands

From `mqttiumd/`, on `6b367b4` for the crates and on the head of this push
for the rest, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `cargo test --workspace --locked --no-fail-fast` | 87 binaries, 1,459 tests pass |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked --no-fail-fast` | pass |
| `cargo test -p mqttiumd --features test-hooks --locked --no-fail-fast` | pass |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | pass; digest unchanged, `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| `python3 tools/build_corpus.py --check`; `python3 tools/check_schemas.py` | pass |
| `python3 tests/oracle/build_traces.py --check`; `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | pass |
| `python3 conformance/check.py --check` | pass |
| `python3 models/check_models.py`; `python3 models/test_check_models.py` | 14 directories consistent; 18 tests pass |
| `python3 ../tools/formal/run_tlc.py --models-dir models/<dir>` for the seven directories changed | 201 configurations as declared: flow-pressure 21, reactor-wake 49, group-commit 20, pi-reuse 12, transport-credit 23, takeover 24, m1 52 |
| `python3 tools/test_perf_gate.py`; `python3 -m unittest discover -s tests/compat -p "test_*.py"` | pass |
| `python3 tools/run_wildcard_mutants.py <out>` | 4 of 4 detected, each for its reason |
| `python3 tools/run_outgoing_qos2_mutants.py --target <dir> <out>` | 2 of 2 detected, each for its reason |
| `tools/run_loop_mutants.sh`; `tools/run_loop_mutants.sh --backend sqlite` | every mutant rejected for its reason on both stores |
| `python3 tests/compat/run.py --reference <mosquitto-2.1.4> --broker <dist> --delivery-shards 0`, then `2` | as at the baseline: 137 scenarios, 105 same, 0 defect; 34 of 35 expectations pass, 1 not applicable |

Not run on this push: Kani (no file it analyses changed; CI runs it), the
frozen receptions, the TLS and MQTT 3.1.1 suites at `K = 2`, and the ARM64
suite, which the owner may run at reception.
