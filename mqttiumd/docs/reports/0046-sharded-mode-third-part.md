# The sharded mode, third part

Branch `refactor/lot10-validated-base`, after report 0045. This part gives
the inline-claim lease and its dirty barrier a model, repairs what the
model and the tests written from it found, makes a shard connection read
what needs no acknowledgement credit, and aligns three parts of the shard's
effect sink with the coordinator's. With `--delivery-shards K >= 1` a
subscriber session is owned by a delivery shard; the coordinator may
carry an inline claim for it in an admission, with a packet identifier it
reserves from the lease the two share. Its verification found a cost on
the default path, which the gate flagged, and a fault of the new fence;
both are repaired below.

## The model

`models/inline-lease` (`MInlineLease`) models one subscriber session:
the coordinator's reservations, dirty marks and barriers, the store's
answers in order, an unknown answer learnt later by a resolution, and the
shard's lane, committed claims, reads of the store, sends and lease
publication. It checks that no two deliveries the shard holds share an
identifier, that the subscriber receives a publisher's publications in
the order of their admission, that nothing is sent twice on a
connection, and that once nothing more happens every committed
publication was delivered and the lease is eligible again. Fifteen
configurations: the behaviour of `9dfd568`, that of `1ccddfb` (after
report 0045), the repair, and a mutant for each part of the repair. Its
scope is in its README: one connection, a read of the store as one step,
answers stepped in their order.

Three defects beside suspect B1 were found while the model was written,
by reading the code and by exploratory tests; the model reproduces each,
and each was played against a sharded runtime before it was repaired.

## Defects fixed

Each by a test committed while it failed, then the fix. The four tests
of the lease are in `runtime_tests/sharded_lease.rs`; three use the
`hold-admission-result` hook and wait for the resolution of the held
answer.

| Defect | Failing test | Fix |
| - | - | - |
| After one publication admitted without an inline claim, the coordinator never claimed inline for that subscriber again: the step of the commit marked the lease dirty after the notification sent on the store's answer, and recorded a barrier that nothing sent | `e341fcf` | `87b198f`: a stored target is notified after the step of the commit |
| A stored publication whose admission answer was unknown was never delivered once a resolution committed it: a resolution names no target, and the runtime notified none with shards | `e341fcf` | `87b198f`, the same change: the step names the targets on a resolution too |
| The order of a subscriber's publications was inverted when the answer to the first admission was unknown and the second, claimed inline, committed (suspect B1) | `e341fcf` | `ab3f0cb`: the coordinator holds a session's committed claims while one of its inline admissions has an unknown outcome, and hands them over in order before the session's barrier |
| A delivery was sent twice on one connection. A read of the store made while the answer to an inline claim was unknown took the claim's committed row and sent it marked as a repeat; the subscriber acknowledged it; at the resolution the claim arrived, was inserted again and sent again, and the subscriber was disconnected when it acknowledged that one | `e341fcf` | `1280511`: a shard does not read a session the coordinator may still owe a claim |
| The fence of `1280511` held a read in `Owner::plan_work` only: the core's test of ready work still took a fenced session that waits for a read for ready work, and the shard spent its passes queuing that session's work again, delaying its other sessions. `sharded_slow_subscriber_isolation_and_fifo_drain` failed on the CI runner at `e9e4fb3` (809 ms where it allows 550) | `5e46228` | `f28a32d`, `22627b3`: one predicate, `read_held`, for both |
| A shard connection without acknowledgement credit read nothing, PINGREQ and DISCONNECT included (D6) | `e72fef3` | `358d046`: without credit a read turn reads one packet; an acknowledgement waits in the task for credit, and nothing behind it is read meanwhile |
| The shard's QoS 0 drop entry named the kind of the origin (`origin_kind`) where the coordinator's names the origin (`origin`) (part of D7) | `79d4d26` | `7141558`: the shard writes the coordinator's entry, with the same function |
| A PUBREL whose identifier was already bound replaced the binding silently, where the coordinator stops; the shard's table of bindings had no bound (part of D7) | `79d4d26` | `7141558`: the same checks as the coordinator's sink |

A journal entry the shard could not write was ignored; the coordinator
stops the broker (`events_unavailable`). The shard now does the same
(`7141558`). No test makes the journal fail, at the shard or at the
coordinator.

## A fix of report 0045 removed

Report 0045 made a shard's claims avoid the identifiers the lease held at
the coordinator's last dirty message. Since `1280511` a shard reads a
session, and so claims for it, only after a message that comes once no
inline claim of the session is pending, and once every committed one is a
row of its core: no reserved identifier is then missing from its rows.
The model passes the repair with or without those identifiers, and
without the read fence a shard claim takes a reserved one. `e081f9c`
removes them; `Deliveries::free_packet_id` is again what it was at
`9dfd568`. The rule that no lease is published while a committed claim
waits stays: it is one look at the waiting claims, and its test plays the
state it guards.

## A cost found by the gate, and removed

At `e9e4fb3` the gate read 1.032 for QoS 0 bursts to eight subscribers,
which asks for an instruction count: 734.8 million instructions against
714.6 million at `9dfd568` (median of three plays). The counts of
intermediate commits placed the step at `87b198f`, and a variant of it
with only its new accessor of `Owner` had it too: the default runtime
never calls that accessor, but with it the optimiser no longer elided the
move of a step's `Draft`, about a kilobyte, into `Owner::apply_draft`, and
copied it at each step (callgrind: one more `memcpy` of 0x3e0 bytes per
step in `Owner::step_with_sink_and_oracle`). `fced594` takes the draft by
reference: no copy is left whatever the optimiser decides, and the shape
retires 703 million instructions, fewer than at `9dfd568`.

## A test made robust

`reserved_collection_and_lifecycle_progress_while_ordinary_replies_are_unread`
waited 3 seconds for 65 durable commits, reading the store every
millisecond, and failed once on the CI runner in the kill-points build of
`c9e64e0`, a commit that changed only models. It now waits at most 30
seconds (`9f35bfa`).

## Found, not fixed here

- The rest of the divergence of the shard's effect sink (D7): a PUBLISH
  too large for the subscriber is answered by a failed frame and a close
  notice, where the coordinator refuses it as closed; a transport that is
  gone gives no close notice; the shard writes no `frame_queued`,
  `frame_failed` or `effect_retained` entry; it does not know that the
  coordinator is closing a connection and may still write to it. None
  loses a delivery or breaks an order; the missing journal entries make
  the sharded mode less observable.
- D8: the store worker takes no request while a reply lane is full, so
  one owner whose lane is full stops every owner. Repairing it changes
  the worker's scheduling, on which group commit and its model rest.
- D9: the detaches a shard completes are found by a scan of every pending
  detach at each pass, quadratic in their number.
- The model steps the store's answers in order. In the runtime an answer
  the core cannot apply for want of capacity goes back to the end of its
  lane, and a later answer may be stepped first; the hold of `ab3f0cb`
  waits only behind unknown outcomes. That case is not modelled.
- TLS has no unit harness for the read turn of D6; it uses the same plan
  as TCP, and the TLS listener profiles run with `--delivery-shards 2`.

## Consequence for earlier measurements

Until `87b198f` a subscriber that received one publication without an
inline claim (the first after its subscription, or one beyond the lease's
room) had every later publication stored and claimed by the shard. The
sharded measurements of reports 0039 to 0042 were made with that defect,
and say nothing of the inline path after it.

## Verification

From `mqttiumd/`, on `22627b3`, with
`env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings`, also `-p mqttiumd --features test-hooks` | pass |
| `cargo test --workspace --locked --no-fail-fast` | 87 binaries, 1,476 tests pass on `f28a32d`; the boundary test of the pure crates failed there and passes on `22627b3` |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked --no-fail-fast` | 88 binaries, 1,498 tests pass on `f28a32d`; the same boundary test as above |
| `cargo test -p mqttiumd --features test-hooks --locked --no-fail-fast` | 17 binaries, 364 tests pass |
| `validate` of P1-02 | pass; digest unchanged, `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| Corpus, schemas, traces, oracle, conformance register | pass |
| `python3 models/check_models.py`; `python3 -B models/test_check_models.py` | pass |
| `python3 ../tools/formal/run_tlc.py --models-dir models/inline-lease` | 15 configurations as declared, in about 3 minutes |
| The tests of `sharded_lease.rs`, of `fence_tests.rs` and the test of D6, ten times | ten of ten |
| `sharded_slow_subscriber_isolation_and_fifo_drain`, ten times after `f28a32d` | ten of ten, 0.68 to 0.73 s each |
| `tools/run_loop_mutants.sh`, sim-store and SQLite | every mutation rejected for its reason |
| `tests/compat/run.py` against Mosquitto 2.1.4, `--delivery-shards 0` and `2` | 137 scenarios, 105 same, 0 defect; 34 of 35 expectations pass, 1 not applicable |
| `tests/compat/listener_profiles.py`, `--delivery-shards 0` and `2` | pass |

One test failed once in a full run of the product tests under load and
passed alone, five times, and in three full runs of its binary:
`secure::takeover_with_a_partially_written_tls_record`, a TLS test at
`K = 0` that drives a Python client with deadlines; nothing this part
changed runs there. Kani was not run: no file it analyses changed.

## Performance

The gate of report 0043, A `9dfd568`, B `fced594`, six passes, broker on
cores 0 and 1, clients on 2 and 3:

| Shape | Median of B/A | Verdict |
| - | - | - |
| QoS 0 bursts, 1 subscriber | 0.967 | pass |
| QoS 0 bursts, 8 subscribers | 1.170 over 6 passes, 0.946 over 12 | pass |
| QoS 1 pairs, 1 pair | 0.982 | pass |
| QoS 1 pairs, 8 pairs | 1.005 | pass |
| QoS 0 blast, 1 / 8 subscribers | 0.916 / 0.911 | reported |

Instructions retired under the bursts shape (callgrind, broker start and
end included); the median of three plays for the first four rows, the
mean of two for the others:

| Commit | 8 subscribers, 10,000 messages | 1 subscriber, 20,000 messages |
| - | - | - |
| `9dfd568` | 714,574,114 | 285,627,256 |
| `b9b4638` (first part) | 717,266,395 | |
| `5e1f935` (second part) | 717,858,848 | |
| `e9e4fb3` | 734,812,764 | |
| `fced594` | 703,380,871 | |
| `22627b3` | 703,266,419 | 280,584,250 |

`22627b3` differs from `fced594` by the predicate of `f28a32d`; its counts
are those of `fced594`, and the gate was not played again.

Idle private memory per connection (`idle_rss_bench.py`, one run each,
A `9dfd568` / B `22627b3`): plain TCP 3,011 / 2,980 bytes at 400
connections and 2,917 / 2,773 at 768; mutual TLS 11,971 / 11,899 at 400.

## What stays open

In `docs/reception/open-conditions.md`, row "The sharded mode is
experimental": the rest of D7, D8, D9, an answer of the store stepped
after a later one, and the TLS read turn without credit, which has no unit
test. The routing of acknowledgements, the end of a generation and the
stop have no model. Whether the mode leaves "experimental" is the
owner's decision.
