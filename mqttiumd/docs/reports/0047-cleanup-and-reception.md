# Cleanup of the validated base, and its reception

Branch `refactor/lot10-validated-base`, after report 0046. This report
covers lot 4 of the validation of `9dfd568`, a cleanup that changes no
behaviour, and lot 5, the reception of the branch's code at `9884c3d`;
`2c8f584` and the commit of this report change documentation only.
Reports 0043 to 0046 cover the proofs, the defects of the default path and
the three parts of the sharded mode; their findings are not repeated here.

## The rule of the cleanup

A group of changes changes no behaviour, no byte on the wire and no
durable format. It is committed alone, and it is measured: either its
dist build has the `.text` of the commit before it, byte for byte, or its
instructions are counted under callgrind against that commit, alternating
the two executables, for QoS 1 pairs (1 and 8 pairs, volatile storage) and
QoS 0 bursts (1 and 8 subscribers). A group whose cost showed was to be
withdrawn, not adjusted; one first version was, below.

## What changed

| Commit | Change | Evidence |
| - | - | - |
| `bbe5b6d` | Seven methods only tests call move into their test modules; `PeerTable::iter_mut`, never called, goes | tests |
| `6145626` | `Owner::bound_sessions`, `Owner::delegated_deliveries_enabled` (never called), the `Option` of `State::loaded_bytes` (always `Some`), the unread parameter of `State::work_bound` go | tests, counts with `1df375b` |
| `1df375b` | `Owner::outbox` returns the box, not an `Option` the callers all unwrapped | counts, two runs: QoS 0 bursts x8 702.39M and 703.29M, x1 280.36M and 281.06M; report 0046 gives 703.27M and 280.58M for `22627b3` |
| `efd618c` | The two shard steps share one reservation and one dispatch of their effects (`shard_reserve`, `shard_dispatch`) | tests; code of `K >= 1` only |
| `3f97f28` | The test modules of `owner.rs` (7,600 lines) and `owner/pubsub.rs` (5,300) move to `owner/tests.rs` and `owner/pubsub/tests.rs`; 75 citations of the conformance register and the replays of four model sidecars name the new files | `.text` and `.rodata` identical |
| `c81f58b` | `plan_admission` chooses its inline claims in `delegated_claims` or `local_claims`, builds the recipe in place, and rolls reservations back in one function | counts, below |
| `88f9e99` | 108 `json!` literals over 100 columns, up to 1,602, laid out one field per line | `.text` identical |
| `4534bd7` | The kind of a frame awaiting its receipt is an `enum`, `FrameKind`, where it was a string compared at about twenty places | counts, below |
| `51ca2a5` | `runtime.rs` (9,300 lines) gives the start of a run to `runtime/start.rs` and the impl blocks of `Sink` to `runtime/sink.rs`, unchanged | counts, below |
| `5db8ab4` | Three module headers named a work lot, two said what inspired them, two named code that does not exist | comments only, `.text` identical |
| `63ac1c5` | 61 SQL statements on one line, up to 621 columns, break before their clauses with the `\` continuation, which keeps every literal's value | `.text` identical |
| `43fbb28` | The owner keeps its limits as `ShardLimits`, the public struct of the same three fields it converted to and from | tests |
| `9884c3d` | The journal of `LogStore` calls the SQLite store's `sync_parent_dir` instead of a copy of it | tests |

The token checks of `88f9e99` and `63ac1c5` compare each file before and
after with whitespace and comments removed, string literals evaluated and
trailing commas dropped. The only other differences are those rustfmt
made once it could lay out the statements around the literals: braces
around the bodies of closures of one expression, and one semicolon after
a `return`.

## Counts

Instructions retired by the broker under callgrind, start and end
included, medians of runs alternating the commit before (A) and the
commit (B):

| Commit | QoS 1 pairs x1, 20,000 messages | QoS 1 pairs x8, 4,000 per pair | QoS 0 bursts x1, 20,000 | QoS 0 bursts x8, 10,000 |
| - | - | - | - | - |
| `c81f58b` | A 788.05M, B 787.83M | A 1,244.06M, B 1,243.51M | | |
| `4534bd7` | A 787.52M, B 788.97M | A 1,242.63M, B 1,243.94M | A 280.58M, B 280.27M | A 703.32M, B 701.73M |
| `51ca2a5` | A 790.78M, B 790.11M | A 1,243.11M, B 1,243.81M | A 280.04M, B 280.15M | A 702.02M, B 702.11M |

A first version of `c81f58b` moved the admission into a function that
built the recipe; it cost 0.7 % on QoS 1 x1 and 0.95 % on x8, the copy of
the admission in and of the recipe out, and was not kept. The version kept
builds the recipe in place; `local_claims`, now a call, costs about 15
instructions per publication. `4534bd7` differs by +0.18 % and +0.1 % on
QoS 1 and −0.1 % and −0.25 % on QoS 0, within the spread of the runs.
`51ca2a5` moves code only, but the dist build's `.text` grows by 6.5 KB:
the order of the functions in the single codegen unit changes what the
inliner does. Its counts do not move.

The lot as a whole, `22627b3` (the end of lot 3) against `9884c3d`, the
same way:

| Shape | `22627b3` | `9884c3d` | Change |
| - | - | - | - |
| QoS 1 pairs x1 | 788.28M | 788.32M | +0.01 % |
| QoS 1 pairs x8 | 1,243.58M | 1,243.50M | −0.01 % |
| QoS 0 bursts x1 | 280.16M | 280.26M | +0.04 % |
| QoS 0 bursts x8 | 703.22M | 701.90M | −0.19 % |

Medians of three runs each for x1, means of two for x8.

## Not done, and why

- `drain_inputs` (about 1,100 lines) and `Sink::try_submit` (about 1,170)
  were not split arm by arm. Their large arms are the per-publication paths
  (QoS 0 emission, admission, frame and PUBLISH emission); moving them into
  functions relies on the inliner to cost nothing, and the plan excludes a
  split that adds an indirection on a hot path. They are where a later
  optimisation lot can split them, measuring each arm.
- The `MUTANT_*` switches of the runtime already sit together at the top of
  `runtime.rs`; the mutation scripts find them by their text, and each
  use is at the place it alters. Moving them would break the scripts.
- The other helpers the two stores share in name differ in what they do:
  the temporary path of a snapshot and that of a rotated journal; a
  snapshot keeps the permissions of the file it replaces, a journal takes
  those of its mode. The validations of `MultiClaimedPublication` dispatch
  to each store's own functions.
- `net.rs` (5,200 lines) and `writer.rs` (4,300) were not split: this lot
  did not study their seams.
- 133 lines of the product crate still exceed 100 columns (252 at
  `9dfd568`), 32 of them 160 (109): string literals (messages of the
  configuration parser, usage texts) and `matches!` with a guard, which
  rustfmt leaves as written.

## Found by the CI

`51ca2a5` moved `Sink::try_submit` to `runtime/sink.rs` and left the
conformance register citing it in `runtime.rs` (MQTT-3.3.1-9); the local
verification did not run the register's check, the CI of `9884c3d` did.
`2c8f584` corrects that citation and five others that named the file in
prose: the model `MTransportCredit`, the tables of `transport-credit` and
`takeover`, and record 0065.

At `88f9e99`, `sharded_slow_subscriber_isolation_and_fifo_drain` failed
once in the instrumented build on the CI runner: the publisher and the
three fast subscribers took 1.41 s where the test allows 550 ms. It
passed in the CI of `efd618c` and again in that of `9884c3d`. Played here with the instrumented
build, ten times alone and ten times with the other tests of its binary,
at the head and at `efd618c`, it took 75 to 266 ms alone and 115 to
198 ms with the others, the two commits alike. No cause was found; the
runner's load and the head-of-line blocking of the store worker (D8, open)
are the candidates. The test is unchanged.

## Verification

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`.
The full list ran on `63ac1c5`; `43fbb28` and `9884c3d` change the core's
limits type and one store helper, and were checked by the rows that name
them and by the CI of `9884c3d`.

| Command | Result |
| - | - |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`, also with `mqttiumd-lab/kill-points` and with `-p mqttiumd --features test-hooks` | pass on `63ac1c5`; on `9884c3d` the first two here, the third by the CI |
| `cargo test --workspace --locked` | 87 binaries, 1,477 tests pass |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | 88 binaries, 1,499 tests pass |
| `cargo test -p mqttiumd --features test-hooks --locked` | 17 binaries, 364 tests pass |
| `cargo test -p mqttiumd-core`, `-p mqttiumd-store-sqlite -p mqttiumd-store-volatile`, the boundary test of the lab, on `9884c3d` | 401, 202 and 17 tests pass |
| `tools/check_kill_points.sh` | pass |
| `validate` of P1-02 | pass; digest unchanged, `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| Corpus, schemas, traces, oracle (279 tests) | pass |
| `conformance/check.py --check` | pass on `3f97f28`, which moved the tests it cites, and on `2c8f584` |
| `python3 models/check_models.py`; `python3 -B models/test_check_models.py` | pass; TLC on every model directory in the CI of `9884c3d` |
| `tools/run_loop_mutants.sh`, sim-store and SQLite, on `9884c3d` | every mutation rejected for its reason |
| `tools/run_pi_core_mutants.py` on `9884c3d` | the three mutants detected, each by its test, which `3f97f28` moved to `owner/pubsub/tests.rs` |
| `tests/compat/run.py` against Mosquitto 2.1.4, `--delivery-shards 0` and `2`, on `9884c3d` | 137 scenarios, 105 same, 15 refused by the profile, 7 where the reference departs from the specification, 0 defect, 10 unspecified; 34 of 35 expectations pass, 1 not applicable |
| `tests/compat/listener_profiles.py`, `--delivery-shards 0` and `2`, on `9884c3d` | pass |

Kani was not run: no file it analyses changed; the CI runs it.

## Performance

The gate of report 0043, A `9dfd568` (the base of the branch), B
`9884c3d`, six passes, broker on cores 0 and 1, clients on 2 and 3:

| Shape | Median of B/A | Verdict |
| - | - | - |
| QoS 0 bursts, 1 subscriber | 0.990 | pass |
| QoS 0 bursts, 8 subscribers | 1.037 | counts decide: pass |
| QoS 1 pairs, 1 pair | 0.984 | pass |
| QoS 1 pairs, 8 pairs | 1.045 | counts decide: pass |
| QoS 0 blast, 1 / 8 subscribers | 1.030 / 0.978 | reported |

The two shapes on 8 clients spread widely from pass to pass (0.91 to 1.14
for QoS 0 bursts, 0.87 to 1.17 for QoS 1 pairs). Their medians fall
between 1.03 and 1.05, where the gate takes the instruction count. Counts
under callgrind, alternating A and B in this session:

| Shape | `9dfd568` | `9884c3d` | Change |
| - | - | - | - |
| QoS 0 bursts, 8 subscribers, 10,000 messages (mean of two) | 716.47M | 701.95M | −2.0 % |
| QoS 1 pairs, 8 pairs, 4,000 per pair (median of three) | 1,247.66M | 1,243.42M | −0.3 % |
| QoS 1 pairs, 1 pair, 20,000 messages (median of three) | 794.07M | 790.24M | −0.5 % |

Report 0046 counted 285.63M at `9dfd568` for QoS 0 bursts with one
subscriber; `9884c3d` counts 280.26M (median of three, above).

Idle private memory per connection (`idle_rss_bench.py`, one run each, A
`9dfd568` / B `9884c3d`): plain TCP 3,041 / 2,929 bytes at 400
connections and 2,869 / 2,763 at 768; mutual TLS 11,981 / 11,878 at 400.

## Contracts

No type of the contract crate, durable encoding, schema or injection
point changed; the digest is unchanged. Two signatures of the core's own
API, which only the workspace uses, did: `Owner::outbox` returns `&Outbox`
instead of `Option<&Outbox>` (`1df375b`), and two unused public methods of
`Owner` are gone (`6145626`). The SQLite store exports `sync_parent_dir`
(`9884c3d`).

## What stays open

In `docs/reception/open-conditions.md`, section "Validation of the lot 10
head": the sharded mode stays experimental, with the rest of D7, D8, D9
and the other items of its row; the records applied while proposed; the
figures of lots 8 to 10; and, new, what the cleanup left. The decisions
the owner holds:

- whether records 0051 to 0057 and 0064 to 0067 are accepted (report 0043
  has a table), and record 0066 against record 0044;
- whether the sharded mode leaves "experimental";
- whether `e081f9c`, which removed the foreign identifiers of report 0045
  once the read fence made them unnecessary (report 0046), stays.
