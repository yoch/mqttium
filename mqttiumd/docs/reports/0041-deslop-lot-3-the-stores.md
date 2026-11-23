# The stores' layers: deslop lot 3

Report 0039 explains `deslop-please` and the baseline
`.deslop-please.allow`. Lot 3 covers the SQLite store
(`crates/mqttiumd-store-sqlite`) and the volatile store
(`crates/mqttiumd-store-volatile`). It changes no behaviour and no file
outside these two crates, and it adds no file: `src/` of the SQLite
store holds 15 entries, the most `deslop-please` accepts, and a new
module there would have been a new diagnostic.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, `a8e9cb9` (the lot), on
  top of `feb3370` (report 0040).
- **Host**: Linux x86-64 container, `rustc 1.97.0`.

## SQLite: two circles and a tangle

### `reader → will → sessions → will`, and `sessions::stage ⇄ will::{open, end}`

`sessions::stage` sent an opening or an end with a will to `will::open`
or `will::end`. Each built a plain session action and called
`sessions::stage` back. `sessions` also needs `will::has_client`, the
lookup that refuses an opening while a will of the client is pending; a
mutant (`allow-reconnect-before-will`) is anchored in it, in `will.rs`.

So the will memory is the lower layer, and the session layer composes
it. `will::open` is now two steps, `before_open` (the checks, and the
plain opening they allow) and `arm` (the insertion once the session is
opened). `will::end` is `settle` (the will published later or forgotten,
and the plain detach). `sessions::stage` runs them around the plain
change, in the order they ran before. `will` no longer names `sessions`.

`reader::snapshot`, the budget preflight of a snapshot, began by calling
`will::receipt_capacity`. Its only caller, `read_snapshot`, now makes
that check itself, at the same place in the order.

### `message_expiry → retained → receipts → retained`, `pubsub → message_expiry`

- The first 330 lines of `retained.rs` were not about retained messages.
  They were about the identities a receipt reserved: `IdentityBounds`,
  `OriginWitness`, `bounds`, `receipt_bounds`, and `identity_was_used`,
  which also reads the receipt frontier. They are now in `receipts`.
  `retained` keeps retained values.
- `message_expiry` held the lifetime metadata of publications, used by
  `pubsub`, `retained` and the store, and also the bounded maintenance
  that deletes expired messages and calls `retained::release`. `release`
  stays in `retained.rs`, where the mutant `release-owed-previous-payload`
  is anchored. The maintenance is a store operation with one caller,
  `SqliteStore::expire_messages`. It is now beside that caller in
  `lib.rs`, as `sweep_expired_messages` with its two helpers. This makes
  `lib.rs`, a long file already in the baseline, 341 lines longer
  (3726 to 4067).

### Fields of `StoreCaches`

`StoreCaches` held three caches as public fields. `pubsub`, `will`,
`receipts`, `TxDelta` and `open_epoch` read and wrote them directly. Each
cache now has a question and its transitions:

- `work_verified`, `mark_work_verified`, `forget_verified_work`;
- `witnessed`, which reads the witness of an origin once;
- `open_receipts`, counted once.

`TxDelta` stages the other side. `stage_receipt` replaces the lines that
staged a receipt, written twice (after a commit and after a rejection), and
`witnessed` answers for what is staged. `TxDelta::commit_into` and
`StoreCaches::clear_receipt_caches` keep their names and signatures,
which loop mutant g1 names.

## Volatile: a tangle and three leaks

- `read.rs` was a set of free functions taking a `&VolatileStore` and
  reading its private fields, called by the store's own methods. They
  are now methods of `VolatileStore`, still in `read.rs`, with their
  bodies unchanged. The conversion of an inventory fact (`FactRef::build`,
  which took the store) is the store's `recovered_fact`.
- `LogStore` wrote the durability of the store it wraps around six
  dry-run probes, and read its ledger and its clock anchor field by
  field. A probe now runs through `VolatileStore::dry_run`, which runs an
  operation with the store refusing to change and then gives back the
  durability the store had. The ledger is read through `ledger()`, and
  the anchor through `session_clock()`, a copy.

The plan suspected the old probe of a latent defect: it set `Available`
again instead of the saved state. Read in the code, nothing else changes
the durability of the wrapped store. The field is private to `LogStore`
and has no getter; `LogStore::set_durability` sets `LogStore`'s own
field, and the replay of the journal does not touch it. Outside a probe
the wrapped store is therefore always `Available`, and `dry_run` behaves
as the old code did. There is no defect to prove, and the plan's claim
is withdrawn.

## Not done, and why

- **The leaks of the effective settings** (`SqliteStore::settings`,
  `LogStore::sqlite_settings`, `VolatileHandle::sqlite_settings`). Report
  0039 put the first in this lot. They are one family: a record of what
  the engine read back, which every reader only copies or reads into
  JSON. Their main reader is the writer of the runtime. They are decided
  with the runtime lot, and the header of the baseline says so.
- **Planned shapes not built.** The plan named an `expiry.rs` and an
  `identity.rs` module, moving the dispatch of the lifecycle to the
  command level, and a `Journal` or `write_ahead` interface for
  `LogStore`. The width of `src/` excluded the new files. The dispatch
  stays in `sessions`, the layer above `will`. `dry_run` was enough for
  the journal.
- **Long files.** `lib.rs` of both stores stays long.
- `docs/pilot/reviews/open-corrections.md` names `will::open`, now
  `before_open` and `arm`. `docs/pilot/` is left as it is.
- `tools/bounded-batch.mutant`, the study patch of report 0025, no
  longer applies. It already did not apply at `290db2b`; it patches the
  runtime, not the stores.

## Count

| | Before | After lot 3 |
| - | -: | -: |
| Entries of `.deslop-please.allow` | 75 | 65 |

Removed: the two module cycles and the tangle of the SQLite store, the
tangle of the volatile store, three leaks of `StoreCaches` and three of
`VolatileStore`.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`,
one after another:

| Command | Result |
| - | - |
| `deslop-please --allow .deslop-please.allow` | `ok` |
| `cargo fmt --check` | clean |
| `git apply --check` of every `tests/mutants/*/*.mutant` | all apply |
| Text anchors of `tools/run_retained_will_mutants.py` and `tools/run_session_store_controls.py` | each occurs once in its file |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo test --workspace --locked` | pass: 85 result lines, 1379 tests, 0 failed |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | pass: 86 result lines, 1401 tests, 0 failed |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 15 result lines, 288 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`, digest unchanged |
| `python3 tools/build_corpus.py --check`; `python3 tests/oracle/build_traces.py --check` | 624 derived files and 405 files, 0 not up to date, 0 stale |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root); `python3 models/check_models.py` | pass; every model consistent |
| `tools/run_loop_mutants.sh --backend sqlite` and `tools/run_loop_mutants.sh` | every mutation expected to be rejected was rejected for its reason, on both stores |
| `python3 tools/run_will_store_mutants.py` | `forget-on-startup` and `publish-normal` detected |
| `python3 tools/run_session_store_controls.py` | both controls detected; the original checkout unchanged |
| `python3 tools/run_retained_will_mutants.py` | exit 1 at its first case, `discard-retain-bit`: expected `retained_will_flag_lost_at_open`, observed `product_event_details`. The same at `290db2b`, before the first lot |

`run_retained_will_mutants.py` stops at its first case. That case patches
`crates/mqttiumd/src/codec.rs`, which no lot changed, and it fails the
same way at `290db2b`, so the failure predates this work. Its four cases
on the SQLite store were not played; their text anchors are each found
once in their files (above). The failure is left for its own analysis.

## Not run, not shown

- The proofs, the compatibility bench: no file they read changed.
- The four store cases of `run_retained_will_mutants.py` (see above).
- The campaigns: the loop mutants above play P1-02 and QABS-B on both
  stores; no campaign was run on its own for this lot.
- No qualifying campaign (`run --rebuild`).
