# The contract's layers: deslop lot 2

Report 0039 explains `deslop-please` and the baseline
`.deslop-please.allow`. Lot 2 is the contract (`crates/mqttiumd-contract`).
It changes no behaviour and no byte of any encoding. No other crate
changes either: `command` and `packet` re-export what moved, and every
crate of the workspace builds with its imports as they were.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, `ce98778` (the lot), on
  top of `610d5c7` (report 0039).
- **Host**: Linux x86-64 container, `rustc 1.97.0`.

## What the circle was

`deslop-please` named nine modules in one circle (`command`,
`message_expiry`, `outcome`, `packet`, `properties`, `pubsub`, `retained`,
`session`, `will`) and one tangle (`Recipe`, `AnnotatedPublication`,
`ExpiringPublication`, `TimedReplaySubscriptions` and
`retained::{check, decode, encode}`). It had two causes.

- **The codec recursed through the lower modules.** A retained, expiring
  or timed recipe nests a legacy recipe. `retained::decode` and the
  `decode` of `ExpiringPublication` and `TimedReplaySubscriptions` called
  `Recipe::decode_canonical` on the nested bytes, then matched on the
  `Recipe` they got back. `retained::check` and `retained::encode` took a
  whole `Recipe` and dispatched on it. The values these modules define
  contain no `Recipe`; only their codec did.
- **Low modules took their primitives from high ones.** `properties`,
  `session`, `will`, `retained` and `message_expiry` read their fields with
  `command::Reader` and refused with `command::RecipeError`. Nearly every
  module took `SharedBytes` and `Qos` from `packet`, which itself needs
  `outcome`, `pubsub` and `properties`. `retained`, `will` and
  `message_expiry` embed recipe bodies (`AdmitPublication`,
  `UpdateSubscriptions`, `FinishDelivery`, `Target`, `Costs`) that were
  defined in `command`, at the top.

## The layers now

From the bottom up; each module uses only modules on the lines above, or
before it on its own line (read from the code, test modules left out).

| Layer | Modules |
| - | - |
| What everything is made of | `decimal`, `ids`, `capacity`, `topic`, `message` (new: `SharedBytes`, `TooLarge`, `Qos`), `scoped_map`, `flat_map` |
| Reading canonical bytes | `canonical` (new: `Reader`, `RecipeError`, the magic, the format, the named kinds) |
| Values a recipe carries | `message_time` (new: the lifetime of a message), `properties`, `pubsub` (now also the delivery cause and the recipes of subscriptions and outgoing deliveries) |
| The admitted publication | `publication` (new: `Source`, `PayloadRef`, `Target`, `Costs`, `AdmitPublication` and its codec, recipe kind 1) |
| Publications built on it | `retained`, `will`, `message_expiry`, `session` (it carries a will), `opening` |
| The recipe | `command`: `Recipe`, its whole codec, `AnnotatedPublication`, `ClaimedPublication`, `FrozenRecipe`, `Command` |
| What a step produces | `receipts`, `outcome`, `packet`, `effects`, `step` |

- Each step of the codec that recurses into a `Recipe` is now a step of
  `Recipe`'s codec, moved into `command` as it was written:
  `decode_expiring` (kind 13), `decode_timed_replay` (kind 14),
  `decode_retained_nested` (kinds 10, 12 and 20), and the check and encode
  arms of the three retained kinds. `RetainedQos0::decode` reads its own
  kinds, 11 and 21, which nest nothing. No decoding logic was rewritten,
  so the error a malformed nested recipe gets is the one it got before.
- The `_ =>` arms of `retained::check` and `retained::encode` refused any
  other recipe; their only caller called them for the three retained
  kinds, so the arms could not be reached and are gone with the dispatch.
- In the crate, users import from the new home; `command::{RecipeError,
  RECIPE_FORMAT, AdmitPublication, …}`, `packet::{Qos, SharedBytes,
  TooLarge}` and `message_expiry::{MessageExpiry, MessageTime, …}` are
  re-exports for the other crates.
- `ids.rs` and `capacity.rs`, whose digests `proofs/credits/BODY.sha256`
  holds, are not changed; the digests were compared.

## Not done, and why

- `ReceiptClass::of`, which takes a `Recipe`, stays in `receipts`. The plan
  had it move to `command`; `receipts` is in neither the circle nor the
  tangle, and `receipts → command` is a downward use.
- `mqttiumd_contract` stays a shallow crate in the baseline, for the
  reason its header gives.
- `src/` of the contract holds 24 entries instead of 20: four modules
  were added. The entry `wide-directory crates/mqttiumd-contract/src/` was
  already in the baseline; the width is decided with the long files.
- A link of `docs/pilot/reviews/transport.md` points at line 58 of
  `message_expiry.rs`, which is now in `message_time.rs`. `docs/pilot/` is
  left as it is, as report 0037 left it.

## Count

| | Before | After lot 2 |
| - | -: | -: |
| Entries of `.deslop-please.allow` | 78 | 75 |

Removed: the module cycle and the tangle of the contract, and the long
file `command.rs` (1901 lines now). Left for the contract: the shallow
crate, the wide directory, and the long files `effects.rs` and
`opening.rs`.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `deslop-please --allow .deslop-please.allow` | `ok` |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo build --workspace --all-targets --locked` with no file outside the contract changed | builds, no warning |
| `sha256sum` of `capacity.rs` and `ids.rs` | the digests of `proofs/credits/BODY.sha256` |
| `cargo test --workspace --locked` | pass: 85 result lines, 1379 tests, 0 failed |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | pass: 86 result lines, 1401 tests, 0 failed (second run; see below) |
| `cargo test -p mqttiumd --features test-hooks --locked` | pass: 15 result lines, 288 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`; `contracts sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5`, as in reports 0038 and 0039 |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 tests/oracle/build_traces.py --check` | 405 files, 0 not up to date, 0 stale |
| `python3 -m unittest discover -t tests/oracle -s tests/oracle/tests` | 274 tests, OK |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root) | pass |
| `python3 models/check_models.py` | every model consistent |
| `git apply --check` of each `tests/mutants/loop/*.mutant` | the thirteen apply |
| `tools/run_loop_mutants.sh` and `--backend sqlite` | every mutation expected to be rejected was rejected for its reason, on both stores |
| `tools/run_campaign.sh`: P1-02 (seed 1) on `sim-store`, `sqlite` and `sqlite` in a child; QABS-B (seed 7) on `sim-store` and `sqlite` | five verdicts PASS, provisional: none is a rebuilt run |

The first run of the suite with `kill-points` failed in
`tests/bench_replay.rs`, two tests: the binary it replays had been built
without the feature. The campaigns above were running at the same time,
in the same build directory, through `cargo run -p mqttiumd-lab` without
the feature, and rebuilt that binary under the suite. Run alone, the suite
passed, as the line above says.

## Not run, not shown

- The proofs (Kani) on this host: the two files they hash are unchanged,
  and the CI runs them.
- The compatibility bench against Mosquitto: no byte on the wire can
  change; the CI runs it.
- The mutant scripts of the core and the stores (PI, QoS 2, will, expiry,
  wildcard, session): none of them patches the contract, and no file they
  patch changed.
- No qualifying campaign (`run --rebuild`).
