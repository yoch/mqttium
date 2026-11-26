# The sharded mode, first part

Branch `refactor/lot10-validated-base`, after report 0043. This report covers
the first part of lot 3 of the validation of `9dfd568`: the delivery shards
(`--delivery-shards K >= 1`). The owner decided on 26 November 2026 to keep
the mode, repair it and prove it, as a runtime option marked experimental
until its proofs exist. This part repairs what could be repaired without
changing the core; what it leaves is listed at the end.

## Defects fixed

Each by a test committed while it failed, then the fix.

| Defect | Failing test | Fix |
| - | - | - |
| A shard that panicked ended its thread silently. Release builds keep overflow checks and several `expect`s on the shard's paths; its sessions then got no delivery, and the coordinator learned of it only if it later sent to that shard | `d5d586d` | `742a077`: the loop runs under `catch_unwind`, and a panic is the fault `delivery_shard_panicked`, which stops the broker |
| The stop of a shard was a message offered with `try_send`: on a full lane it was lost, and the shutdown reported a deadline with the thread still running | `376b05d` | `014e618`: a stop flag, read when the lane is empty, so that every message sent before the stop is read first |
| A shard kept the reactors' wake-up hold through its wait. The hold is one flag per reactor thread: a producer that holds nothing (a reactor thread giving back acknowledgement credit) did not kick, and its reader waited up to 20 ms | `8896298`: test builds assert where a shard waits that it holds nothing; both shards failed | `33c2410`: the guard covers the work of a pass and drops before the wait. Model reactor-wake updated: the fairness it assumes now holds for every holder |
| A connection whose session revision was unknown was skipped at attach, and a second attach of one generation kept its first route and still sent a second `Attach`: nothing showed either | `778b873` | `30436de`: the connection is closed with `shard_attach_without_revision` or `shard_attached_twice` |
| Delegated QoS 0 waiting for a jammed core was unbounded: the shard keeps emptying its lane, so the lane gave no backpressure | `c4a0559` | `6cca159`: at most `PENDING_QOS0_LIMIT` (4,096), then shed with the journal entry of the core (`qos0_dropped`, `capacity_exceeded`) |
| Releasing an identifier above the lease's bitmap decremented its count unconditionally; from zero it wrapped and the lease read as full | `f156128` | `f07a801`: a checked decrement |
| Every connection carried a whole shard route in place, 112 bytes, also without shards | `05ff067` | `ed833c0`: the route is boxed |

The readiness line now ends with `delivery_shards=K experimental`
(`b9b4638`), and the product documentation describes the option. The
`ready` event already carries the count; its schema is closed, and a status
field there would be a contract change.

## A blocking send kept on purpose

The coordinator's send to a shard blocks when the shard's lane is full. It
is kept. A live shard waits only for a bounded time and empties its lane at
each pass, and its queues are now bounded by shedding, not by leaving the
lane full; a dead shard is reported, and the send to it fails. Bounding the
shard by refusing its lane would close a cycle: the coordinator waiting for
the shard, the shard for the store worker, and the worker, which takes no
request while a reply lane is full, for the coordinator. Record 0066 states
it.

## Found by reading, not fixed here

**Packet identifiers of a shard's own claims.** A shard picks the identifier
of a claim it makes from its own rows (`Deliveries::free_packet_id`). The
identifiers the coordinator reserves for inline claims live in the lease the
two share, and the shard does not read them. A dirty notification sent while
the coordinator has inline claims pending (`dirty_seq = 0`) still makes the
shard read the session's work and claim what it finds: it can take an
identifier the coordinator reserved, and the second row then fails the
shard on its outgoing binding. Separately, `publish_eligible` overwrites the
lease's bitmap with the shard's view; an inline claim committed but not yet
applied by the shard is then missing from it, and the coordinator may
reserve that identifier again. These were the suspects S1 and B2. The repair
is one allocator per session for both owners, which changes the core; it is
the subject of the second part, with a model of the lease.

**Reads stopped by the acknowledgement budget.** A connection's read turn
reserves acknowledgement credit before it parses, since it cannot know a
packet's kind before; with no credit it reads nothing, PINGREQ and
DISCONNECT included. Reading one packet anyway needs a place for an
acknowledgement read without credit, delivered to the shard when credit
returns; the held packets of a transport task go to the coordinator only.
Not done in this part.

## Verification

From `mqttiumd/`, on `65434da` (crates as at `b9b4638`), with
`env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `cargo test --workspace --locked --no-fail-fast` | 87 binaries, 1,469 tests pass |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked --no-fail-fast` | pass |
| `cargo test -p mqttiumd --features test-hooks --locked --no-fail-fast` | pass |
| `validate` of P1-02 | pass; digest unchanged, `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| Corpus, schemas, traces, oracle, conformance register | pass |
| `python3 models/check_models.py`; `python3 -B models/test_check_models.py` | pass |
| `python3 ../tools/formal/run_tlc.py --models-dir models/reactor-wake` | 49 configurations as declared |
| `tests/delivery_shards.rs` and the sharded unit tests, ten times each | ten of ten |
| `tests/compat/run.py` against Mosquitto 2.1.4, `--delivery-shards 0` and `2` | 137 scenarios, 105 same, 0 defect; 34 of 35 expectations pass, 1 not applicable |
| `tests/compat/listener_profiles.py`, `--delivery-shards 0` and `2` (the option is new) | pass |

The mutation campaigns of the core and the stores were not run again: no file
they mutate or build has changed since report 0043. Kani was not run: no file
it analyses has changed.

## Performance

The gate of report 0043, A `9dfd568`, B `b9b4638` (`eafebfbfbfd02a6e...`), six
passes, broker on cores 0 and 1, clients on 2 and 3:

| Shape | Median of B/A | Verdict |
| - | - | - |
| QoS 0 bursts, 1 subscriber | 1.036 | pass; instruction count required |
| QoS 0 bursts, 8 subscribers | 0.949 | pass |
| QoS 1 pairs, 1 pair | 0.968 | pass |
| QoS 1 pairs, 8 pairs | 1.011 | pass |
| QoS 0 blast, 1 / 8 subscribers | 1.014 / 1.126 | reported |

The instruction count of the first shape (`tools/count_broker_instructions.sh`,
three plays each, broker start and end included): A 285,427,956,
285,339,811, 285,367,674; B 286,307,468, 286,189,239, 286,128,582. Medians
differ by 0.29 %.

Idle private memory per connection (`idle_rss_bench.py`, one run each, A / B):
plain TCP 3,041 / 2,949 bytes at 400 connections and 2,869 / 2,741 at 768;
mutual TLS 11,960 / 11,858 at 400. About the 112 bytes the boxed route
removed.

## What stays open

In `docs/reception/open-conditions.md`, row "The sharded mode has no model
and open defects": the packet identifier allocation of a shard, the reads
stopped by the acknowledgement budget, the effect handling that differs from
the coordinator's, writer head-of-line blocking across owners, quadratic
detach scans, and two suspects without a test (PUBLISH order after an
unknown admission outcome, a detach completed before a queued inline claim).
No model of the coordinator and shard protocol exists yet.
