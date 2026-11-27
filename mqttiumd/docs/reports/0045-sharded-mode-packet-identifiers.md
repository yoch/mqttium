# The sharded mode, packet identifiers

Branch `refactor/lot10-validated-base`, after report 0044. This part settles
the suspects of the review that concern the packet identifiers of a sharded
session, and one suspect about a detach. With `--delivery-shards K >= 1` a
session has two allocators of identifiers: the shard that owns it, which
claims from its own rows, and the coordinator, which reserves identifiers
for the inline claims it carries in an admission from a lease the two share.

## Defects fixed

Each by a test committed while it failed, then the fix. Both tests drive a
hand-built shard worker and play the interleaving step by step; neither
depends on timing.

| Defect | Failing test | Fix |
| - | - | - |
| A shard claimed with an identifier the coordinator had reserved. A dirty notification sent while an inline claim was pending (`dirty_seq` 0) still made the shard read and claim the session's work, from its rows only; when the coordinator's claim committed, the shard held two rows with one identifier and failed on the outgoing binding (suspect S1) | `8c27f0c` | `70c3450`: the delivery table keeps, per session, the identifiers another owner may hold, and picks beside them; the shard gives its core the lease's bits at each dirty message of the coordinator |
| A shard published its lease while an inline claim committed for the session waited to be applied. The bitmap it published is read from its rows, the waiting claim was not one yet, and the coordinator could reserve its identifier again (suspect B2) | `e7b7046` | `2fea662`: no publication while such a claim waits; the publication is tried again once it is applied |

The snapshot of the lease is complete when it is read. The coordinator marks
the lease dirty before it sends a dirty message, and a reservation needs a
lease the shard has acknowledged since its last mark: no reservation can
follow the mark, so the shard, which reads the bits after the message, sees
every identifier the coordinator may still hand over. The shard forgets them
when it publishes the lease again, which then holds its rows' view.

Without shards the table holds no foreign identifier: the search is the one
it was, after one test of an empty map. A test of the core pins the rule,
with and without rows and above the bitmap.

## A witness

A shard reads the committed claim of a generation and the detach of that
generation in one pass, and completes the detach before it applies the claim
(suspect S2). The test plays that order: the detach completes, no frame is
queued for the closed generation, and nothing of the claim is left in the
shard. The claim is durable and is delivered again on the next connection.
It is not a defect; the test stays (`5e1f935`).

## Left for the model of the lease

The order of a subscriber's PUBLISH packets after an admission whose outcome
is unknown (suspect B1). In delegated mode the coordinator asks only the
lease before it carries an inline claim, and the lease knows nothing of an
earlier claim for the same session whose commit returned an unknown outcome:
a later publication can be claimed, committed and emitted first. The repair
has two parts: the coordinator refuses a new inline claim while an earlier
one is unresolved, and the shard holds its scan of the session behind the
barrier of the pending claim instead of reading at once. The second part
makes the session's deliveries wait for the barrier to be flushed, on every
path by which a pending claim ends. That liveness is what the model of the
lease and the dirty barrier must establish first; no test of this suspect
was written.

## Verification

From `mqttiumd/`, on `946d5a1` (crates as at `5e1f935`), with
`env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`. The core changed on a
path the single owner takes too, so the mutation campaigns were run again.

| Command | Result |
| - | - |
| `cargo fmt --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `cargo test --workspace --locked --no-fail-fast` | 87 binaries, 1,473 tests pass |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked --no-fail-fast` | pass |
| `cargo test -p mqttiumd --features test-hooks --locked --no-fail-fast` | pass |
| `validate` of P1-02 | pass; digest unchanged, `sha256:cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300` |
| Corpus, schemas, traces, oracle, conformance register | pass |
| `python3 models/check_models.py`; `python3 -B models/test_check_models.py` | pass |
| `python3 ../tools/formal/run_tlc.py --models-dir models/pi-reuse` | 12 configurations as declared |
| `tools/run_wildcard_mutants.py`; `tools/run_outgoing_qos2_mutants.py` | 4 of 4 and 2 of 2 detected, each for its reason |
| `tools/run_loop_mutants.sh`; `tools/run_loop_mutants.sh --backend sqlite` | every mutant rejected for its reason on both stores |
| `tests/delivery_shards.rs`, ten times | ten of ten |
| `tests/compat/run.py` against Mosquitto 2.1.4, `--delivery-shards 0` and `2` | 137 scenarios, 105 same, 0 defect; 34 of 35 expectations pass, 1 not applicable |
| `tests/compat/listener_profiles.py`, `--delivery-shards 0` and `2` | pass |

Kani was not run: no file it analyses has changed.

## Performance

The gate of report 0043, A `9dfd568`, B `5e1f935` (`9ceb440daca8c3d4...`), six
passes, broker on cores 0 and 1, clients on 2 and 3:

| Shape | Median of B/A | Verdict |
| - | - | - |
| QoS 0 bursts, 1 subscriber | 0.988 | pass |
| QoS 0 bursts, 8 subscribers | 0.976 | pass |
| QoS 1 pairs, 1 pair | 1.002 | pass |
| QoS 1 pairs, 8 pairs | 0.975 | pass |
| QoS 0 blast, 1 / 8 subscribers | 0.976 / 0.941 | reported |

No shape reads above 1.03.
