# AGENTS.md — mqttiumd

Guide for coding agents working under `mqttiumd/`. The repository guide at
the root covers the Python client and does not apply to this subtree, with
one exception: maintained content is English. `docs/spec/fr/` is the only
French content; it is an immutable archive.

## Checks

Run from `mqttiumd/`. All four must pass before a change is proposed.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo run -q -p mqttiumd-lab -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json
```

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
   attributes listed in `crates/mqttiumd-lab/tests/boundary.rs`; extending
   a list is a decision (ADR 0007). The same core code runs in simulation
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

## Roles and file ownership

One lot per branch. A lot touches only its own paths. The lots of the
first wave, the paths each owns and the way contract changes are
coordinated are in `docs/missions/README.md`; read your lot's mission
before you start.

| Role | Owns | May not change alone |
| - | - | - |
| Contracts and oracle | `contracts/`, `tests/oracle/`, `tests/scenarios/` | Core behaviour to match an expectation |
| Core | `crates/mqttiumd-core/` | `contracts/`, `tests/oracle/`, assertions |
| Store | `crates/mqttiumd-store-sqlite/`, the deterministic store in `crates/mqttiumd-lab/` | Admission guarantees |
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
