# AGENTS.md — mqttiumd

Guide for coding agents working under `mqttiumd/`. The repository guide at
the root covers the Python client and does not apply to this subtree, with
one exception: maintained content is English. `docs/spec/fr/` is the only
French content; it is an immutable archive.

**Current work starts at `docs/missions/L12-herdr-handoff.md`** (hand-off
after Lot 11 and report `0050`, branch `perf/lot11-production-grade-and-cpu`,
for the three-agent Herdr workflow); see `docs/missions/L11-optimisation.md`
for the prior Lot 10 baseline and reserve definitions (R1 to R10).

## Checks

Run from `mqttiumd/`. All four must pass before a change is proposed.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json
```

The oracle under `tests/oracle/` is a second reader of the semantic level
and the judge of a run. A change to a contract, to the corpus or to the
oracle also runs it, with its pinned versions; CI runs it in the job
`contracts`:

```bash
python -m pip install -r tests/oracle/requirements.txt
python tests/oracle/build_traces.py          # rewrite the derived traces
python tests/oracle/build_traces.py --check
python -m unittest discover -t tests/oracle -s tests/oracle/tests
```

A campaign runs one scenario through the real core, the deterministic
store and their adapters (`crates/mqttiumd-lab/src/bench/`), then has the
oracle judge the directory the runner wrote. The runner computes no
verdict, and its `run-result` is provisional. The exit status is the
oracle's: 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 3 OUT_OF_SCOPE. Campaign
directories are generated and never committed.

```bash
tools/run_campaign.sh tests/scenarios/p1/P1-02.json /tmp/p1-02        # seed 1
tools/run_campaign.sh tests/scenarios/qabs/QABS-B.json /tmp/qabs-b 7  # another seed
tools/run_campaign.sh tests/scenarios/p1/P1-02.json /tmp/p1-02-sqlite 1 sqlite
```

The fourth argument names the store: `sim-store` (the default) or
`sqlite`, the store of `crates/mqttiumd-store-sqlite` in one file of the
campaign. On both, a crash of a scenario is simulated inside one
process: the adapter is dropped and the store recovered or reopened. No
process is killed and nothing is shown about `fsync` or power loss.

A third value, `sqlite-group`, is the SQLite store behind a writer of the
bench that commits the commands waiting together in one transaction
(record 0045). It runs in-process only, and only the scenarios of
`tests/scenarios/group/` reach a group of more than one command; their
README states what such a run shows and what it does not.

A result of the oracle is qualifying only for a run whose binary the
runner rebuilt from an archive of the recorded content, kept in the
campaign (`run --rebuild`, record 0017); any other result is provisional
and never PASS. The script below plays such a run on a clean checkout,
has the oracle produce the result, requires it to be qualifying, accepted
by `validate` and standing at `recompute`, and rebuilds the binary a
second time from the archive to compare it with the one the manifest
names. The campaign directory must be outside the work tree:

```bash
tools/run_qualifying_campaign.sh tests/scenarios/p1/P1-02.json /tmp/p1-02-q 1 sqlite
```

By default the process under test runs inside the runner and a crash is
simulated. On the SQLite store it can run in a child process
(`--process child`, the fifth argument of the two scripts above): the
supervisor then kills that child with SIGKILL where the scenario crashes
the broker, waits for its end by that signal, and starts another child on
the same database file. The script below plays such a run rebuilt from
its source, which must qualify, and plays it again under `strace` to keep
the kernel's account of the kill; `tools/check_campaign_kill.py` checks
both. It shows the loss of the process after a commit, outside any
transaction, and the recovery; nothing about a transaction cut by a kill,
`fsync` or a power loss:

```bash
tools/run_killed_campaign.sh tests/scenarios/p1/P1-02.json /tmp/p1-02-killed
```

Only `recompute` says that a result is evidence: it recomputes the
assertions and the faults from the trace and checks again every condition
of a qualifying run, so a qualifying result whose source archive, oracle
or kept modification no longer matches is refused. `validate` checks the
document only. The reception below produces a qualifying campaign from
the clean tree and one from a modified worktree, degrades independent
copies, and requires each refusal for its own reason:

```bash
tools/run_recompute_reception.sh /tmp/recompute-reception sqlite
```

The command a campaign records in `campaign.json` (`commands.replay`)
names every parameter of the run, its store included, and
`commands.replay_scope` says what that replay is: an ordinary replay
plays with the lab binary of the checkout it is run in and gives a
provisional result; only a rebuilt run can qualify.

Build directories are per agent: run the suites with `env -u
CARGO_TARGET_DIR -u CARGO_ENCODED_RUSTFLAGS` or an explicit
`--target-dir`, and campaigns in directories of their own. The binary
check unpacks and builds in a directory of its own; `run --rebuild` uses
the directory its archive names under `TMPDIR` and takes its turn on a
lock, so two rebuilds of the same source wait for each other.

Run normal and instrumented full suites sequentially in one checkout, or use
separate worktrees. Separate Cargo targets alone do not isolate the legacy
source-relative `target/workspace-tests/one-snapshot` fixture.

The loop is shown to reject a defect by mutations kept as diffs under
`tests/mutants/loop/` (`*.mutant`: the root `.gitignore` ignores `*.patch`
and `*.diff`). The script applies each one in a temporary worktree, never
in the current tree, plays the campaign with the mutated binary, has the
oracle of the unmodified tree judge it, and compares the verdict, the
failing assertion and its reason with `expected.json`:

```bash
tools/run_loop_mutants.sh
tools/run_loop_mutants.sh --backend sqlite
```

`proofs/` is a workspace of its own, so the four commands above do not see
it. Its scripts are its only verification, and CI runs them in the job
`pilot proof`; they need Kani (`tools/install-tools.sh kani`):

```bash
proofs/credits/run.sh                    # preflight, harnesses, witnesses, coverage, digests
python3 proofs/credits/mutants.py        # each mutant caught as aimed
python3 proofs/credits/selftest.py       # the scripts refuse what they claim to check
```

`run.sh` fails when `crates/mqttiumd-core/src/credits.rs`,
`crates/mqttiumd-contract/src/capacity.rs` or `ids.rs`, or a file of the
proof, differs from `proofs/credits/BODY.sha256`. After such a change the
owner of the proof reviews it, runs `run.sh --record` and says so.

The requirement register and the example corpus are derived files. After
changing a decision record, a chapter digest, scenario P1-02 or the fixture,
rebuild them; CI fails when they are not up to date:

```bash
python tools/build_corpus.py          # rewrite the derived files
python tools/build_corpus.py --check  # what CI runs
```

A change to a schema or to the example corpus also runs the second schema
reader, with its pinned versions:

```bash
python -m pip install -r tools/requirements.txt
python tools/check_schemas.py
```

## Load-bearing rules

1. **The pure crates stay pure.** `mqttiumd-contract` and `mqttiumd-core`
   are `no_std + alloc`, forbid `unsafe`, have no unlisted dependency, use
   conditional compilation only as `cfg(test)` or `cfg(kani)` on a module,
   and contain no async. They invoke only the macros and use only the
   attributes listed in `crates/mqttiumd-lab/tests/boundary.rs`, and a
   local macro captures only identifiers and literals: a macro or
   attribute name is always written, never passed in. Extending a list is
   a decision (ADR 0007). The same core code runs in simulation
   and against SQLite. That test enforces all of this and states what it
   does not cover.
2. **An input is applied or handed back intact.** A transition computes a
   single-use plan against `&State`, then applies it (ADR 0006). Reserve
   before mutating; a plan carries the owner and state revision it was
   computed against.
3. **No effect is dropped.** Effects live in the owner's bounded `Outbox`
   until an adapter accepts them; a refused effect returns to its place with
   the same identity. A reservation is a key the box checks before it
   changes anything.
4. **Absence is not rejection.** An observed absence proves nothing. Both
   `RejectedCertain` and `RetiredOutcomeUnavailable` forbid execution, and a
   committed operation is never executed again; only `RejectedCertain` says
   that nothing happened. Terminal knowledge never regresses; contradictory
   terminal results are a fault.
5. **An `OperationId` designates one recipe.** Recipes are compared by their
   exact canonical bytes.
6. **Counters never wrap.** Use `checked_next`; exhaustion suspends.
7. **64-bit values are decimal strings in JSON and 8 big-endian bytes in
   SQLite.** No arithmetic on the encoded form.
8. **The bench gives the broker nothing the product could not know.**
   Scenario bindings such as `op_A` never reach the broker. Every broker
   input is a page of the durable reader, an identity the core created, a
   result correlated to an effect, or client bytes (ADR 0003).
9. **Every reader is bounded.** Sizes are computed with checked arithmetic
   and compared to a budget before anything is allocated or enumerated.

## Evidence tiers (ADR 0044)

| Tier | Paths | Regime |
| - | - | - |
| Core | `contracts/`, `crates/mqttiumd-contract/`, `crates/mqttiumd-core/`, `crates/mqttiumd-store-api/`, `crates/mqttiumd-store-sqlite/`, the deterministic store in `crates/mqttiumd-lab/`, `tests/oracle/`, `tests/scenarios/`, `proofs/` | Everything in this guide |
| Runtime | `crates/mqttiumd/`, `tests/compat/`, the runtime models under `models/`, load tools | Test first; differential scenarios against the pinned Mosquitto; TLA+ models of the thread and queue protocol with mutant configurations; property tests of the codec; load measurements with their commands. No sealed reception per increment |

A runtime change that alters a public type, a durable encoding or an
injection point of the core tier follows the core regime. The product
tests run in both configurations:

```bash
cargo test -p mqttiumd --locked
cargo test -p mqttiumd --features test-hooks --locked
```

The system receptions under `tests/system/`, the `tools/run_*_reception.sh`
scripts, `preview/` and the pilot kits are frozen evidence of the
executable built from `51be4079` (report 0026). They run on demand
(`mqttiumd-frozen-receptions.yml`) and are not repaired when the runtime or
a limit changes; cover the behaviour with a runtime test or a compatibility
scenario first. The distributed executable is built with
`cargo build -p mqttiumd --profile dist --locked`; `[profile.release]` is
the profile the run manifests record and stays unchanged.

## Roles and file ownership

One lot per branch. A lot touches only its own paths. The lots of the
first wave, the paths each owns and the way contract changes are
coordinated are in `docs/missions/README.md`; read your lot's mission
before you start.

| Role | Owns | May not change alone |
| - | - | - |
| Contracts and oracle | `contracts/`, `tests/oracle/`, `tests/scenarios/` | Core behaviour to match an expectation |
| Core | `crates/mqttiumd-core/` | `contracts/`, `tests/oracle/`, assertions |
| Store | `crates/mqttiumd-store-api/` (the interface every store realises), `crates/mqttiumd-store-sqlite/`, the deterministic store in `crates/mqttiumd-lab/` | Admission guarantees |
| Bench and adversarial | `crates/mqttiumd-lab/`, mutations, injectors | An assertion, to turn a run green |

A change of public type, durable encoding, injection point or schema is a
contract change: it needs a decision record in `docs/decisions/` and updated
examples. It changes the contract digest that `validate` prints; say so in
the report. Lots that run in parallel start from the same revision, and a
contract change made by one of them is merged before the others rebase on
it: two lots never change the same contract file independently. A behaviour change never ships together with the weakening of the
test that covers it.

## Evidence

A result belongs to an exact content. Report the commands actually run and
their output, the revision, and what was not run. A compilation failure or
an unexplained timeout is not a detection. An injection that was not reached
makes the run inconclusive. Never rewrite `docs/spec/fr/`.

A defect is fixed test first: the discriminating test is committed while it
still fails, then the fix. `docs/reception/open-conditions.md` lists what is
deliberately not done yet; do not report one of its rows as done without
removing it.
