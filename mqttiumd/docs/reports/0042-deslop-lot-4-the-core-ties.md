# The core's sub-states, one layer each: deslop lot 4

Report 0039 explains `deslop-please` and the baseline
`.deslop-please.allow`. Lot 4 covers the ties between the sub-states of
the owner (`crates/mqttiumd-core/src/owner/`). It changes no behaviour,
adds no file and moves no test. The tables inside `pubsub::State` are
lot 5.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, `9fb64a8` (the lot), on
  top of `e89f5ee` (report 0041).
- **Host**: Linux x86-64 container, `rustc 1.97.0`.

## The circle

`deslop-please` named five modules in one circle: `pubsub`, `receipts`,
`routing`, `sessions` and `will`. Each is the module of one sub-state of
the owner, and each holds `impl Owner` blocks that work on it. Read tie
by tie, almost every use goes down if the modules are layered this way,
from the bottom up:

- `receipts` and `routing`, which the others ask (`control_available`,
  `select`);
- `pubsub`;
- `sessions` and `will`, which drive pub/sub.

Three ties went up.

- **`receipts → pubsub, will, sessions`.** `Owner::deadline_is_live` asks
  whether a token is the live deadline of anything the owner holds. It
  was written in `receipts.rs` and read the four sub-states. It is the
  owner's question: it is now in `owner.rs`, and each sub-state answers
  for itself. `receipts::State`, `pubsub::State` and `will::State` each
  have `holds_deadline` (the last two renamed from `deadline_is_live`).
  The order of the alternatives is unchanged. `sessions::Flight` is
  plain data with no method, and the owner reads its deadline as before.
- **`routing → pubsub`.** `Owner::routing_work_bound` recognised the
  historical S0 owner, which keeps a one-transition work abstraction, by
  reading `self.pubsub.connections`. Whether the owner registers sessions
  or serves connections is the owner's fact: `Owner::serves_connections`,
  in `owner.rs`, answers it with `pubsub::State::has_connections`.
- **`pubsub → will`.** The gate of an inline claim ("no control or
  retained operation in flight, no will admission pending") was written
  twice in `pubsub.rs`: in `plan_inline_claim` and where a work page is
  applied. It combines two sub-states, so it is the owner's question
  `Owner::inline_claims_blocked`, in `owner.rs`. `pubsub::State` answers
  its part with `control_or_retained_in_flight`.

The first attempt moved only the will check of `plan_inline_claim` to
its caller. `deslop-please` then still named a circle `pubsub ⇄ will`,
through the second copy of the gate. That attempt was replaced by the
single owner question above.

## Fields

- `receipts::State::{pending, entries}`: the owner's receipt methods read
  them directly. The state now answers `collecting`, `holds_deadline`,
  `pending_at` (a copy of the collection in flight at a deadline),
  `answering` (the collection in flight, when an effect is one of its
  attempts) and `status`. In `plan_receipt_result`, the check that the
  reply belongs to the collection in flight was two steps that both
  returned `Nothing`; it is one call to `answering`.
- `LeaseTable::account`: recovery built the key of a restored lease with
  the identifier of the table's account. `LeaseTable::restore_at` builds
  the key of its own account and returns it. `restore` is unchanged:
  `proofs/credits/PROOF.md` names it, and recovery still goes through it.

## Count

| | Before | After lot 4 |
| - | -: | -: |
| Entries of `.deslop-please.allow` | 65 | 61 |

Removed: the circle of the core, `LeaseTable::account` and
`receipts::State::{pending, entries}`. The leaks of `pubsub::State` and
`Owner::outbox` remain for lots 5 and 7, and the three long files of the
core remain.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`,
one after another:

| Command | Result |
| - | - |
| `deslop-please --allow .deslop-please.allow` | `ok` |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo test --workspace --locked` | pass: 85 result lines, 1379 tests, 0 failed |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | pass: 86 result lines, 1401 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 15 result lines, 288 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`, digest unchanged |
| `python3 tools/build_corpus.py --check`; `python3 tests/oracle/build_traces.py --check`; the tests of the oracle | 624 and 405 files up to date; OK |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root); `python3 models/check_models.py` | pass; every model consistent |
| `git apply --check` of every `tests/mutants/*/*.mutant` | all apply |
| `sha256sum` of the three files `proofs/credits/BODY.sha256` names in the crates | the three digests match |
| `tools/run_loop_mutants.sh` and `--backend sqlite` (m3, m4 and m5 patch `owner.rs`) | every mutation expected to be rejected was rejected for its reason, on both stores |
| `python3 tools/run_pi_core_mutants.py` | both mutations detected |
| `python3 tools/run_expiry_mutants.py` | the four mutations detected |
| `python3 tools/check_will_core_mutants.py` | the four mutations detected |
| `python3 tools/run_outgoing_qos2_mutants.py` | exit 1 at its first case: the anchor of `early-window` is not in `pubsub.rs` |
| `python3 tools/run_wildcard_mutants.py` | exit 1 at its first case: the anchor of `no-aggregation` is not in `routing.rs` |

The last two scripts stop at an anchor that is not in the file it
patches. Counted in the files at `290db2b`, before the first lot, at
`e89f5ee` and at this lot, the counts are the same: `early-window` 0
(`wrong-replay-leg` 1); `no-aggregation` 0, `maximum-qos-lost` 2,
`fanout-truncated` 0, `no-local-global-self-suppression` 2. The anchors
were broken before this work, and the mutations of these two scripts
were not played. They are left for their own correction.

## Not run, not shown

- The proofs (Kani): the files `BODY.sha256` hashes are unchanged (above).
- The compatibility bench: the CI runs it.
- The mutations of `run_outgoing_qos2_mutants.py` and
  `run_wildcard_mutants.py` (see above).
- No qualifying campaign (`run --rebuild`).
