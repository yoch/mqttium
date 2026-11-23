# The lab's layers: what `deslop-please` found and what lot 1 changed

`deslop-please` 0.1.3 reads the workspace through rust-analyzer and names
modules that use each other in a circle (`module-cycle`), functions that
know each other in a circle across modules (`tangle`), fields that code
outside their type works on (`field-leak`), modules that show more than
they hide (`shallow-module`), long files and wide directories. On
`290db2b` it named 87 diagnostics in `mqttiumd/`. `.deslop-please.allow`
records them (`741ce98`); each lot of branch `refactor/deslop-layers`
removes the entries it fixes, and an entry still in the file is not fixed.

This report is lot 1, the lab (`crates/mqttiumd-lab`). It changes no
behaviour: the same records, the same verdicts, the same messages. No
file is renamed and none of the code the loop mutants patch moves.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`: `e093808` (the lot), on
  top of `741ce98` (the baseline).
- **Host**: Linux x86-64 container, `rustc 1.97.0`, rust-analyzer of the
  same toolchain.

## What each circle was, and the layer each module belongs to

### `campaign → runner → broker → child → campaign, runner`

The supervisor's modules are layered: a campaign plays runs through the
runner, the runner drives a broker, a broker is in-process or a child. The
child process, at the bottom, reached back up: it took `settings_of` from
the campaign and `Population`, `bound_exchanges` and `specification` from
the runner, and wrote its own copy of the loop that arms the faults not
reached yet.

What the child needs is how a fixture sets a process up, and what a
scenario says about its faults. Both are below the runner:

- `bench/setup.rs` (new) holds what a fixture makes of a process, before
  the process exists; the plan had put it in `process`, which is the
  process running:
  `settings_of`, `Setup` and `Population`, with the two constants they
  use. The campaign, the runner and the child read it.
- `Scenario` answers about its own steps and faults, and its fields are
  private: `step`, `steps`, `step_count`, `assertions`, `fault_count`,
  `fault_index`, `fault_name`, `is_barrier`, `tallies` (the hits of each
  fault against what it expects, which the runner, the campaign and
  `failed_before_start` each formatted), `bound_exchanges` and `arm`, the
  loop written twice. `Fault` turns itself into the selection of the
  store adapter. The scenario does not know `Stop`; each caller wraps the
  message, which is unchanged.

### `semantic ⇄ workspace`

Checking a document follows its references, and a referenced document is
checked in turn: the recursion is real. It went through two modules that
each knew the other: the workspace called `semantic::check`, and the rules
took `Workspace`, its catalogues, its four reference constants and four
of its methods.

The rules are the lower layer. `semantic` now states what it reads beyond
the document, as the trait `Documents` (catalogues, contract digest, a
referenced document checked before it is handed back, the digest of cited
evidence), and owns the vocabulary the rules are written in: `Catalogs`,
`FaultPoint`, `Reference`, the four references and the table of all of
them. `Workspace` is where the documents are, and implements `Documents`.
The workspace uses the rules; the rules no longer name the workspace.

### `stage_session ⇄ will_memory::{open, end}`

`stage_session` sent the two will actions to the will memory, which built
a plain session action and called `stage_session` again. The will memory
is a layer above the session lifecycle: opening with a will is an opening
then an armed intent, ending with a will is a settled intent then a
detach. The plain change is now its own step, `Staging::change_session`,
which the will memory calls; `stage_session` only chooses between the two.
`stage_release`, which mutant m1 patches, is not touched.

## Fields

- `Scenario::{steps, faults, assertions}`: see above. A test that reads the
  faults of a parsed scenario uses `faults()`.
- `SimStore::{events, grouped}`: after each step the process checked that
  no evidence was lost, took the group counts, then took the events, in
  that order. `SimStore::drain_evidence` is that transition: it hands out
  both, or nothing once evidence was lost, so no caller can read events
  after a loss. That is why it counts as a fix and the closure of
  `Collector::lines` below does not: it moves an order the caller had to
  keep into the adapter, where the closure only changed the shape of the
  same hand-off. The adapter's own tests still take the events alone.
- The opening read the ledger through the wrapped store; `SimStore::ledger`
  answers it.

## Kept, with their reason

These stay in `.deslop-please.allow`, and its header says why:

- `Collector::lines`: the child relays the records its collector wrote,
  byte for byte, and reads each one again for its origin and its kind;
  mutant n1 patches that reading. A version that handed the buffer to a
  closure was tried: the linter still saw the field, and it is the same
  lending. It was reverted.
- `SimStore::store`: the inspector of the backend reads the durable state
  beside the adapter, unrecorded and unfaulted, by design. It is the only
  use left.
- `SqliteStore::settings` is a leak of the SQLite store that the lab is
  one reader of; it belongs to the lot of the stores.
- The long files and the wide directories, as for every crate.
  `src/bench/` holds one file more, `setup.rs`: 17 entries instead of 16.

## Count

| | Before | After lot 1 |
| - | -: | -: |
| Entries of `.deslop-please.allow` | 87 | 78 |
| Of which named in the lab | 16 | 7 |

Removed: two module cycles, two tangles, five field leaks. The seven left
are the two kept leaks, two wide directories and three long files.

## Commands run

From `mqttiumd/`, with `env -u CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS`:

| Command | Result |
| - | - |
| `deslop-please --allow .deslop-please.allow` | `ok` |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `tools/check_kill_points.sh` | no manifest turns them on |
| `cargo test --workspace --locked` | pass: 85 result lines, 1379 tests, 0 failed |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | pass: 86 result lines, 1401 tests, 0 failed |
| `cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | `accepted: 17 documents checked`; `contracts sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5`, as in report 0038 |
| `python3 tools/build_corpus.py --check` | 624 derived files, 0 not up to date, 0 stale |
| `python3 mqttiumd/conformance/check.py --check` (from the repository root) | pass |
| `python3 models/check_models.py` | every model consistent |
| `git apply --check` of each `tests/mutants/loop/*.mutant` | the thirteen apply |
| `tools/run_loop_mutants.sh` and `--backend sqlite` | every mutation expected to be rejected was rejected for its reason, on both stores |
| `tools/run_campaign.sh`: P1-02 (seed 1) on `sim-store`, `sqlite` and `sqlite` in a child; QABS-B (seed 7) on `sim-store` and `sqlite` | five verdicts PASS, provisional: none is a rebuilt run |

## Not run, not shown

- The instrumented suite of the broker (`-p mqttiumd --features
  test-hooks`): no file of that crate changed; clippy ran on it.
- The proofs, the tests of the oracle, the compatibility bench: nothing
  they read changed.
- No qualifying campaign (`run --rebuild`).
