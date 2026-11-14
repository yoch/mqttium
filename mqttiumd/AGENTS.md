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

## Load-bearing rules

1. **The pure crates stay pure.** `mqttiumd-contract` and `mqttiumd-core`
   are `no_std + alloc`, forbid `unsafe`, have no external dependency, use no
   `cfg` other than `cfg(test)` and `cfg(kani)`, and contain no async. The
   same core code runs in simulation and against SQLite.
   `crates/mqttiumd-lab/tests/boundary.rs` enforces this.
2. **An input is applied or handed back intact.** Transitions plan against
   `&State` and then apply infallibly (ADR 0006). Reserve before mutating.
3. **No effect is dropped.** Effects live in the owner's bounded `Outbox`
   until an adapter accepts them; a refused effect returns to its place with
   the same identity.
4. **Absence is not rejection.** Only `RejectedCertain` forbids execution.
   Terminal knowledge never regresses; contradictory terminal results are a
   fault.
5. **An `OperationId` designates one recipe.** Recipes are compared by their
   exact canonical bytes.
6. **Counters never wrap.** Use `checked_next`; exhaustion suspends.
7. **64-bit values are decimal strings in JSON and 8 big-endian bytes in
   SQLite.** No arithmetic on the encoded form.
8. **The bench gives the broker nothing the product could not know.**
   Scenario bindings such as `op_A` never reach the broker (ADR 0003).

## Roles and file ownership

One lot per branch. A lot touches only its own paths.

| Role | Owns | May not change alone |
| - | - | - |
| Contracts and oracle | `contracts/`, `tests/oracle/`, `tests/scenarios/` | Core behaviour to match an expectation |
| Core | `crates/mqttiumd-core/` | `contracts/`, `tests/oracle/`, assertions |
| Store | `crates/mqttiumd-store-sqlite/`, the deterministic store in `crates/mqttiumd-lab/` | Admission guarantees |
| Bench and adversarial | `crates/mqttiumd-lab/`, mutations, injectors | An assertion, to turn a run green |

A change of public type, durable encoding, injection point or schema is a
contract change: it needs a decision record in `docs/decisions/` and updated
examples. A behaviour change never ships together with the weakening of the
test that covers it.

## Evidence

A result belongs to an exact content. Report the commands actually run and
their output, the revision, and what was not run. A compilation failure or
an unexplained timeout is not a detection. An injection that was not reached
makes the run inconclusive. Never rewrite `docs/spec/fr/`.
