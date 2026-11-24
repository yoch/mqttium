# The delivery table and the end of `pubsub::State`: deslop lot 5, steps 4 and 5

Report 0044 covers the first three steps of lot 5: the index check of
the core's tests, the connection table and the volatile queue. This
report covers the last two: the delivery table, and what was left of
`pubsub::State`, which is gone. It changes no behaviour. It also gives
every reading of the performance gate since report 0044, which overstated
the resolution of the gate; the correction is below.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, `422c941` (the delivery
  table) and `10a87fc` (the end of the state), on top of `c0bf13e`
  (report 0044).
- **Host**: Linux x86-64 container, 4 cores, `rustc 1.97.0`.

## Step 4: the delivery table

`pubsub::State` held the outgoing QoS 1 and 2 deliveries, nine indices
and five counts beside them, and the rank of the next PUBREC. The owner's
methods read and indexed them, and rewrote rows between calls to the
index helpers.

`deliveries::Deliveries` (`owner/deliveries.rs`, 1205 lines with its
tests) now holds the rows, every index and count derived from them, and
the rank, with private fields; `Owner` holds the table.

- **What moved with it.** `Phase` and `Delivery`. `Delivery` is the
  record the owner's transitions edit; its fields are visible to the
  owner. The functions of the state that read only deliveries moved
  unchanged: the index maintenance, `next_send`, `next_pipelined_claim`,
  `qos0_writing`, `known`, `has_control_work`, `close_windows`,
  `accepts_message_time`, `evict_expired_cache` and the others of that
  kind.
- **Transitions.** `push_delivery`, `remove_delivery_at`,
  `mutate_delivery` (the partial reindexing of record 0057, unchanged)
  and `rewrite`, which unindexes a row, changes it and reindexes it. The
  owner did that by hand in three places; they call `rewrite`. In one of
  them the change could give up half way (a phase with no command in
  flight): the closure returns whether it changed the row, and the owner
  returns the stale-plan fault after the row is reindexed, as before.
- **Questions.** Lookups by operation, deadline, frame, claim, and packet
  identifier of a target, which return a row index; the rows of a
  session; the rows, read only; whether a delivery may need a control
  command and which; whether a delivery expires; whether a payload fits
  the loaded bytes; the rank a first positive PUBREC takes, and
  `advance_rank`.
- **What stayed in `pubsub.rs`.** `free_packet_id` and
  `reserves_packet_id` read the mutant constants `MUTANT_*`, which stay in
  `pubsub.rs` for `tools/run_pi_core_mutants.py`. They are methods of
  `Owner` there; `models/pi-reuse/README.md` names them so. The raw scan
  of the mutant `MUTANT_STALE_OUTGOING_FRAME_RESULT` reads the rows the
  table hands out.
- **Split methods.** `retired_flights`, `retire_session` and
  `holds_deadline` also read the retained or control operation. The
  table answers for the deliveries, and the owner composes the two parts
  in the order they had (`Owner::retired_flights`,
  `Owner::retire_outgoing`).
- **Citations.** `conformance/server-statements.json` cites
  `expiry_allows_send` in `owner/deliveries.rs`, where it is defined.
  `conformance/check.py --check` and `models/check_models.py` pass.

It was done in two stages, both before the commit. The first moved the
code with the table's fields still visible, and was checked neutral (the
core's tests, the 33 index mutations below). The second made the fields
private and turned every place the compiler named into a question or a
transition.

The index check, the two tests of the table alone and their fixtures
(copied, since a pure crate may put `cfg` only on a whole module, and a
`pub(super) mod tests` is not one of the allowed forms) moved to the test
module of `deliveries.rs`. Tests elsewhere read the rows through `rows`,
`len` and `is_empty`, and rewrite them through `rows_mut`, a helper of
that test module. One test pushed a recovered row without indexing it
(`try_push`) and relied on the rebuild before the next step; it pushes
it through `push_delivery` now, which indexes it. The core keeps 392
tests.

### Where `Delivery` lives

Before the move, a canary in a scratch copy of `9b7c05d` put `Delivery`
in a sibling module with every field visible: `deslop-please` did not
report it. With one private field added, it reported its 20 fields as
leaks. `Delivery` is not judged only because it keeps no field to
itself, as it was not in `pubsub.rs` either; the baseline never had an
entry for it. The move hides nothing.

### What the linter stopped seeing

After this step, `deslop-please` reported 13 entries of the baseline as
fixed. Ten were: the fields the table took. Three were not:
`State::control`, `State::retained` and `State::non_inline_by_session`,
which the owner still read. Every field of `State` was then known outside
it, so the linter no longer judged `State`. A canary (a private field of
`State` read only by `State`) brought the three back. They stayed in the
baseline, with a note, until step 5.

## Step 5: the end of `pubsub::State`

What was left was the subscription change in flight, the retained QoS 0
admission in flight, the limit of loaded payload bytes, and the count,
per session, of admissions committed without an inline claim. The owner
already held the other half of that count, the targets per operation.

- `control`, `retained` and `payload_limit` are fields of `Owner`. The
  four methods of the state that read them are methods of the owner:
  `pubsub_work_bound`, `control_or_retained_in_flight`,
  `control_or_retained_holds` (the part of `deadline_is_live` the state
  answered) and `control_pending`.
- The two maps of the admissions without an inline claim are one table,
  `pubsub::NonInlineAdmissions`: `admit`, `resolve`, `holds` and
  `is_empty` keep the count with the targets. The owner marks the targets
  dirty outside the table. Marking a session dirty reads no count, so
  where it happens in the loop changes nothing.

A rewrite of this step dropped the negation of one condition: the
connection whose work page ends is caught up only when no admission
without an inline claim is pending for its session
(`no_pending_non_inline`). Compiled so, before the negation was put
back, it failed
`inline_claim_commits_emits_rejects_and_handles_toolarge_and_retire`.

## Count

| | Before | After step 4 | After step 5 |
| - | -: | -: | -: |
| Entries of `.deslop-please.allow` | 56 | 46 | 43 |

Removed in step 4: `State::{deliveries, control_candidates,
deliveries_by_session, delivery_by_claim_op, delivery_by_deadline,
delivery_by_frame, delivery_by_op, delivery_by_target_pi,
expiring_deliveries, next_pubrec_rank}`. In step 5: `State::{control,
retained, non_inline_by_session}`. Lot 5 began at 61. The leak left in
the core is `Owner::outbox`, for lot 7.

## The index check on the new file

The 33 throwaway mutations of the deliveries in report 0044 were played
again on `owner/deliveries.rs` after each stage of step 4. Each failed as
before, and `close-active` survived, as before: it is equivalent. The
mutations of the connections were played on `owner/connections.rs` in
step 2 (report 0044); steps 4 and 5 do not change that file.

## Performance

### The correction to report 0044

Report 0044 says that, played A against A, the four medians that decide
"were 0.994 to 1.000". That was one campaign. Played again, the same
pairs of binaries gave medians several per cent apart:

| Pair, QoS 0 `bursts` with 1 subscriber | Readings of the median |
| - | - |
| `9b7c05d` against a binary with its production code | 0.997 (6 passes), 0.992 (12) |
| `9b7c05d` against `f3e11a7` | 0.992 (6), 1.038 (12) |
| `9b7c05d` against `422c941` | 1.044 (6), 1.005 (12) |

On that shape the gate resolves about 4 %, not 0.6 %. It can catch a
regression clearly above 5 %. It cannot show that there is none just
below. Pooled over the 18 passes of each pair, the medians are 0.995 (A
against A), 1.019 (`f3e11a7`) and 1.015 (`422c941`). With 8
subscribers, 0.988 (A against A) and 1.006 (`f3e11a7`).

The statistic the plan first named, the median throughput, read 0.949
for `422c941` on that shape over six passes, and 1.008 over twelve.

### The readings of steps 4 and 5

The rule of report 0044 applies: the broker CPU time per delivery,
pinned, as the median over passes of B/A; a shape above 1.05 over six
passes is played again alone over twelve, which decides.

`422c941` against `9b7c05d`, `release` builds:

| Shape | Six passes | Twelve passes |
| - | -: | -: |
| QoS 0 `bursts`, 1 subscriber | 1.044 | 1.005 |
| QoS 0 `bursts`, 8 subscribers | 1.012 | |
| QoS 1, 1 pair | 0.991 | |
| QoS 1, 8 pairs | 0.986 | |
| QoS 0 `blast`, 1 (reported) | 1.016 | |
| QoS 0 `blast`, 8 (reported) | 0.978 | |

`Cargo.toml` names the `dist` profile (fat LTO, one codegen unit) as the
one a load measurement uses, and the gate had run on `release`. The end
of the lot was measured on `dist` builds of both sides. `10a87fc`
against `9b7c05d`:

| Shape | Six passes |
| - | -: |
| QoS 0 `bursts`, 1 subscriber | 1.037 |
| QoS 0 `bursts`, 8 subscribers | 0.985 |
| QoS 1, 1 pair | 1.006 |
| QoS 1, 8 pairs | 0.991 |
| QoS 0 `blast`, 1 (reported) | 0.961 |
| QoS 0 `blast`, 8 (reported) | 1.015 |

Both pass. 1.037 lies where the gate cannot tell, so no pass was added:
the instructions the broker retires were counted instead, with
`valgrind --tool=callgrind`, which does not depend on the clock. Twenty
thousand QoS 0 messages in bursts, one subscriber, three plays per side,
`dist` builds:

| | Instructions retired by the broker | Median |
| - | - | -: |
| `9b7c05d` | 432.9 M, 434.2 M, 436.3 M | 434.2 M |
| `10a87fc` | 433.0 M, 433.9 M, 435.7 M | 433.9 M |

The count includes the start and the end of the broker. The two medians
are 0.06 % apart, within the spread of each side. Lot 5 costs no
instruction there that this count can see.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`,
one after another, on `422c941` and on `10a87fc` each:

| Command | Result on each |
| - | - |
| `deslop-please --allow .deslop-please.allow` | `ok` |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo test --workspace --locked` | 1381 tests, 0 failed |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | 1403 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | 288 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked` |
| `python3 tools/build_corpus.py --check`; `python3 tests/oracle/build_traces.py --check`; the tests of the oracle | up to date; OK |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root); `python3 models/check_models.py` | pass; every model consistent |
| `git apply --check` of every `tests/mutants/*/*.mutant`; the digests of `proofs/credits/BODY.sha256` | all apply; the digests match |
| `tools/run_loop_mutants.sh` and `--backend sqlite` | every mutation expected to be rejected was rejected for its reason, on both stores |
| `python3 tools/run_pi_core_mutants.py`, `tools/run_expiry_mutants.py`, `tools/check_will_core_mutants.py` (each with a fresh output directory) | 3, 4 and 4 mutations detected, each for its expected reason; none by the index check |
| The trace digests of report 0044 | the same as at `9b7c05d` |
| `python3 mqttiumd/tests/compat/run.py` against Mosquitto 2.1.4 (from the repository root) | 135 scenarios: 105 `same`, 30 classified, as before; 21 expectations of 21 pass |
| `cargo test -p mqttiumd-lab --test boundary --locked` | 17 tests, OK |

The performance figures above, and the callgrind count, come from
scratch scripts of the session, not committed.

## Not run, not shown

- The proofs (Kani): the files `BODY.sha256` hashes are unchanged.
- The mutations of `run_outgoing_qos2_mutants.py` and
  `run_wildcard_mutants.py`, whose anchors were already broken before
  this work (report 0042).
- The QoS 1 shapes under callgrind.
- No qualifying campaign (`run --rebuild`).
