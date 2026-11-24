# The nets of lot 5, the connection table and the volatile queue: deslop lot 5, steps 1 to 3

Report 0039 explains `deslop-please` and the baseline
`.deslop-please.allow`. Lot 5 dissolves `pubsub::State`
(`crates/mqttiumd-core/src/owner/pubsub.rs`) into tables that keep their
own indices and that the owner holds. This report covers its first three
steps: the nets the tables are moved under, the connection table and the
volatile queue. It changes no behaviour. The delivery table, and what is
left of the state, are the next steps.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, on top of `9b7c05d`
  (report 0043): `a7882ec` and `264e578` (the nets), `e28fb4e` (the
  connection table), `f3e11a7` (the volatile queue).
- **Host**: Linux x86-64 container, 4 cores, `rustc 1.97.0`.

## Step 1: the nets

### The indices are checked after every step of the tests

`pubsub::State` kept a row per delivery and per connection and, beside
them, fourteen indices and four counts, each maintained by hand at every
change of a row (`index_delivery`, `unindex_delivery`,
`reindex_moved_delivery`, `mutate_delivery`, `close`, and the same for
connections). `rebuild_indices` derives them all from the rows. Until
now only the helper `apply()` of the tests of `pubsub.rs` called it,
before each step, which repairs a drift rather than detects it.

The tests of the core now step the owner through `Owner::step_checked`.
It calls `step`, then, on every outcome, takes a snapshot of every index
and count, rebuilds them from the rows and compares; the first drift
fails the test. It is written in the test module of `owner/pubsub.rs`,
and production code is unchanged.

The comparison is strict: an empty set or a zero count left in a map is
a drift as well. Run that way over the 390 tests of the core, it found
none: the maps hold no dead entries, and the tables keep that property.

The first version (`a7882ec`) put the check at the end of `Owner::step`
under `#[cfg(test)]`, beside two `cfg(test)` methods of the state, and
wrote the snapshot with `format!`. The `contracts` job failed in
`pure_crates_keep_their_source_rules` (`crates/mqttiumd-lab/tests/boundary.rs`):
a pure crate may put `cfg` only on a whole module, and `format!` is not
one of the macros it may invoke; the list is ADR 0007's decision and was
not extended. `264e578` moved the check into the test module, wrote the
snapshot with `write!`, and routed the tests through `step_checked`.

### What the check catches

Throwaway mutations each dropped one statement of the incremental
maintenance and ran the core's tests (a scratch script, not committed).
Of 35, 27 failed through the check at once. Of the eight that survived:

- `close-active`, which drops `active_by_gen.remove` in `close`, is
  equivalent: the `unindex_delivery` that follows for every delivery of
  the window takes the count to zero and removes it.
- Seven were paths no step reaches in the tests: the swap removal of a
  delivery that moves a row in flight, expiring or retrying its start
  into its slot; the removal of a delivery sent on its connection and
  candidate for control; and a mutation that changes the packet
  identifier of a claim. Two tests of the state alone now drive them,
  `a_swap_removal_reindexes_the_row_it_moves_under_every_index` and
  `a_mutation_that_changes_a_claim_moves_its_packet_identifier_key`. The
  seven mutations fail them.

After `264e578`, a sample of fourteen of these mutations failed as
before.

The oracle of the check is `index_delivery`, which the rebuild shares: a
mistake in it would be on both sides. The behaviour tests and the trace
digests cover that part. `non_inline_by_session` and `next_pubrec_rank`
are not derived from rows, and the check does not cover them.

The core has 392 tests instead of 390.

### Traces

The lab played each scenario of `tests/scenarios/` twice on each store
(`sim-store` and `sqlite`, in process, seed 1). The 16 scenarios that run
so gave one digest of `trace.jsonl` per scenario and store, the same in
both plays. The lab of `9b7c05d`, built in a clean worktree, and that of
each later step give the same digests. K1 and K2b need the `kill-points`
feature and a child process; they are not in this list, and the suite
with `kill-points` plays them.

### Performance

The plan measured performance at the delivery table; steps 2 and 3 were
measured here, after the fact, so that a change at the next step can be
told apart from theirs.

The plan's gate was: no median throughput more than 5 % lower on a shape
of `tools/measure_qos0_delivery.py` (`bursts`, `blast`, 1 and 8
subscribers) or of a QoS 1 measure, over six alternated passes. Measured
first with the same code on both sides (the release broker of `9b7c05d`
against that of `a7882ec`, whose production code is the same), that
throughput moved by up to 7.0 % between the two (QoS 0 `bursts` with 8
subscribers, −7.0 %; `blast` with 1, −5.9 %): it cannot tell 5 %. The
broker CPU time per delivered message, measured the same way, still
moved by 4.7 % on QoS 1 with 8 pairs and by 8.3 % on `blast` with 1
subscriber.

The gate is therefore taken as follows. The threshold stays 5 %; what it
is applied to changes.

- **What is measured.** The CPU time of the broker (user and system,
  read in `/proc/<pid>/stat` before it stops) divided by the deliveries
  received. Throughput is reported beside it.
- **How.** The broker runs on cores 0 and 1, the clients on cores 2 and
  3. Each pass plays A and B one after the other, alternating which goes
  first. The statistic of a shape is the median over the passes of B/A,
  the ratio of the two plays of one pass.
- **Shapes that decide.** QoS 0 `bursts` with 1 subscriber (300 000
  messages) and with 8 (150 000), and QoS 1 with 1 pair (300 000) and 8
  pairs (60 000 each), eight publications in flight per publisher, on
  the volatile store. `blast` sheds messages by design and is only
  reported. The QoS 1 measure and the CPU reading are scratch scripts,
  not committed.
- **Rule.** Six passes. A shape whose median exceeds 1.05 is played
  again alone over twelve passes, and that median decides.

Played A against A with this method (`9b7c05d` against `a7882ec`), the
four medians that decide were 0.994 to 1.000. Within one pass, a ratio
still moved by up to about 5 %.

`f3e11a7`, the end of step 3, against `9b7c05d`:

| Shape | Six passes | Twelve passes |
| - | -: | -: |
| QoS 0 `bursts`, 1 subscriber | 0.992 | |
| QoS 0 `bursts`, 8 subscribers | 1.058 | 1.003 |
| QoS 1, 1 pair | 1.012 | |
| QoS 1, 8 pairs | 1.019 | |
| QoS 0 `blast`, 1 (reported) | 1.021 | |
| QoS 0 `blast`, 8 (reported) | 0.954 | |

The six plays of `f3e11a7` for 8 QoS 0 subscribers were bimodal (four
near 3.5 µs, two near 3.1 µs, against 3.2 to 3.4 µs for `9b7c05d`). Over
twelve passes the median was 1.003, against 1.001 for `e28fb4e` (step 2
alone) and 0.988 for A against A. Steps 2 and 3 cost nothing measurable
here.

## Step 2: the connection table

`pubsub::State` held the online connections as a public row vector with
two indices, and the owner's methods read, indexed and rewrote the rows
and called the index helpers directly.

The rows, the index by generation and the index of the read in flight
are now `connections::Connections` (`owner/connections.rs`), held by
`Owner` itself. The table answers questions: the row of a generation, a
copy of a connection, the row that reads an effect for a session,
whether it is full. It makes named transitions that keep its indices:
`push`, `update`, `remove`, `record_qos0_ingress`, `invalidate_reads`,
`mark_dirty`, `mark_all_dirty`. Closing a connection is
`Owner::close_outgoing`, which removes the row and then releases the
windows the deliveries held, in the order `State::close` did. The work
bounds take the table as an argument.

Before fixing the form, it was tried in two places.

- As a child module of `pubsub`, the table gave `pubsub` submodules, and
  `deslop-please` named `owner::pubsub` a shallow module (66 of its 91
  functions shown). A child sees the private fields of its parent, which
  was the reason to try it.
- As a sibling under `owner/`, no entry appears. `Connection` and `Read`
  go with it; their fields are visible to the owner, and as plain data
  with no method that works they are not judged.

A deliberate access to a private field of the table from `owner::pubsub`
is reported as a field leak (`Connections::rows`), so the linter does see
through the owner's field. Four mutations of the table fail the tests:
dropping the fix-up of the moved row in `remove` (5 tests, by the
check), the removal of the read in `unindex` (28), the reindexing in
`update` (38); and the insertion of the read in `index`, which the
rebuild shares, fails 29 behaviour tests.

Records 0056 and 0057 still name `pubsub::Connection` and `conn_by_gen`;
they describe the code when they were decided and are left as they are.

## Step 3: the volatile queue

`pubsub::State` held the queue of volatile QoS 0 deliveries and, beside
it, the continuation that serves the queue and whether one was ever
started. Every QoS 0 change wrote the queue and then the continuation,
from the owner, field by field.

`Owner` now holds the queue, which stays in `pubsub.rs` and keeps its
continuation. Each change is one transition of the queue that does its
operation and then names the continuation that serves it next, in the
order the owner did: `admit`, `send_front`, `drop_front`, `park_front`,
`serve`, `finish` and `idle`. The owner asks `serving`, `serves`,
`continuations_started` and `has_expiring` instead of reading fields. The
work bound takes the queue as an argument, and a message clock is
accepted when both the pub/sub state and the queue accept it.

## Count

| | Before | After step 2 | After step 3 |
| - | -: | -: | -: |
| Entries of `.deslop-please.allow` | 61 | 59 | 56 |

Removed: `State::connections`, `State::conn_by_loading_effect`;
`State::volatile`, `State::continuation`, `State::continuations_started`.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`,
one after another, on `264e578`, `e28fb4e` and `f3e11a7` each:

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
| The trace digests (above), on `e28fb4e` and `f3e11a7` (the production code of `264e578` is that of `9b7c05d`) | the same as at `9b7c05d` |
| `python3 mqttiumd/tests/compat/run.py` against Mosquitto 2.1.4 (from the repository root), on `e28fb4e` and `f3e11a7` | 135 scenarios: 105 `same`, 30 classified, as before; 21 expectations of 21 pass |

On `a7882ec`, `cargo test --workspace` failed in
`pure_crates_keep_their_source_rules`, as the CI did.

## Not run, not shown

- The proofs (Kani): the files `BODY.sha256` hashes are unchanged.
- The mutations of `run_outgoing_qos2_mutants.py` and
  `run_wildcard_mutants.py`: their anchors were already broken before
  this work (report 0042).
- No qualifying campaign (`run --rebuild`).
