# Open conditions

What lot L0 leaves open on purpose. Each condition names the lot that closes
it and the gate or reception it blocks. A lot that closes one removes its
row in the same change; a lot that finds a new one adds it.

Nothing here is satisfied by the present tree.

## Before a result may be used as evidence (L3, gate G0)

`mqttiumd-lab validate` checks a qualifying result against the files it
names: digests, contract digest, assertions and faults of the scenario,
verdict equal to the one its assertions determine. It does not check that
the assertions are true.

| Condition | Closed by | Blocks |
| - | - | - |
| The collector assigns the origin of a record from its channel and refuses a record that claims another (ADR 0003). The rule and the refusal of a trace that breaks it are done by lot L3; the collector that assigns the origin is owed | L4 | G0 |
| **Core, limits of what it remembers** (lot L1, corrections of the review of its core). An operation that ended is kept in a bounded window (`OwnerConfig::retained_operations`); beyond it a contradicting result is absorbed. A connection the owner closed is kept in a bounded window (`remembered_connections`); beyond it what the connection still carries is no longer known as stale. Lot L4, which assigns connection generations, decides whether "older than the current one" closes the second limit | L1, L4 | S0 |
| **Store, limits of the deterministic store** (lot L2, head `35bec750`). It clones its whole durable state per command; the records of terminal sessions stay, bounded by the session counter and not by `max_sessions`; the receipts and fences of epoch 0 are never collected; a recovered limit that does not fit its field, and a route to a session that is not active, are reported as `AllocationFailed`. Stated limits of the deterministic store; the SQLite store states its own | L7 | S0 on SQLite |
| **Store and formats, what the two lots left apart.** (1) The example `store-snapshot-after-commit.json` gives the route table of the fixture revision 1; the store advances the revision at each `AddRoute`, so the fixture has revision 2: one of the two is decided and the other follows. (2) No trace record exists for the opening of an epoch, for a `RecipeMismatch` violation, or for a result handed to the core by the store; `TraceWriter` names what it does not write. (3) A fault at `store.fence.transit` cannot select on the kind of the resolved operation, which the store does not know for an absent operation: the fault catalogue says so, or the runner maps the selection | L3 with L2; L4 for the runner | G0 |
| **Store, the read `DurableStore::established`.** The fence point needs a read that writes nothing; the SQLite store realises it and passes the same contract tests | L7 | S0 on SQLite |
| **Oracle, what is left of the review of lot L3** (review of `3c023bf`, corrections merged at `b4e5952`; the integrator read the corrections of findings 1 and 4 only). The list of contract files is still computed twice, by `manifest.rs` and by `Workspace::open`: both now refuse a file of the schema directory that is not a schema, and one list read once needs a change of the shared `manifest.rs` that lot L3 proposed and that is not applied. The Python reader keeps its own copy of the order of recovered facts, on purpose, tied to the Rust reader by twelve shared cases. Not written yet: the two inventory rules of the core in the oracle (a route twice, a route to a session absent from the inventory); the dimension `reason` of `release_pending.rejected.prepare_again` (line 4 of record 0013); the guard on exhausted identities | L3; integrator for `manifest.rs` | G0 |
| The registry rules that decide a behaviour of the broker which the chapters leave open (record 0013, proposed) are accepted or replaced | Owner of the repository | G0 |

## Before a campaign is run (L4, L7)

The manifest states what is certain about the running binary and what was
observed in the checkout. It does not connect the two.

| Condition | Closed by | Blocks |
| - | - | - |
| The binary of a campaign is built from the recorded content inside the campaign, or its correspondence to that content is otherwise established | L4 | Any qualifying run |
| Modified content is kept in the run directory, so that a run on a modified tree can be reproduced from its own directory | L4 | Any qualifying run on a modified tree |
| The manifest records the effective parameters: scenario, fixture, backend, failure model, scheduler version and seed, commands | L4 | Any qualifying run |
| For SQLite: the exact engine version and compile options, and the effective `journal_mode` and `synchronous` read back after they are set (DEM-011) | L7 | S0 on SQLite |
| The lab binary, the oracle and the generators are identified separately in the manifest (V3-002) | L4 | G0 |
| **One snapshot of the contracts.** The contract digest is computed from one read of the contract files and the schemas and catalogues are then read again; a file that changes in between leaves a digest that names other rules than the ones loaded. Either the bytes that are hashed are the bytes that are interpreted, or the snapshot of a campaign is established as immutable. L3: the readers interpret the bytes they hashed. L4: the snapshot of a campaign. The row stays until both parts are done | L3, L4 | Any qualifying result |

## Scenario P1-02b (ADR 0001)

| Condition | Closed by | Blocks |
| - | - | - |
| `tests/scenarios/p1/P1-02b.json` and its run on the deterministic store | L4 | S0 |

## Effect boxes and transitions (ADR 0006): reception of L1

Lot L1 is not received while a condition it closes is open. The creation
authority is decided by record 0009 and split by lot: no part is closed by
another lot's part.

| Condition | Closed by | Blocks |
| - | - | - |
| **Creation authority, the real path** (ADR 0009). The trusted entry point that constructs an authority is stated; start-up is wired through it, and composition tests go through that path, not through a helper that builds an authority directly | L4 | S0 |
| **Creation authority on SQLite** (ADR 0009). The opening of an epoch and its results realised on the SQLite store, with the same contract tests | L7 | S0 on SQLite |

## Pilot proof (DEM-021): reception of L6

| Condition | Closed by | Blocks |
| - | - | - |
| The proof that is received is on `release_if_current` as the product really executes it. Tooling may be prepared in parallel; a proof of a separate demonstration function does not receive the lot | L6, with L1 | Reception of L6 |

## Model M1: reception of L5, then L4

| Condition | Closed by | Blocks |
| - | - | - |
| The sidecar of a model lists as `tests` only files that exist and replay one of its counterexamples. Until a deterministic test replays a counterexample of M1, the list stays empty and the correspondence table names, row by row, the lot that owes the replay | L5 for the table, L4 for the replays | G1 |

## Process topology (ADR 0002)

| Condition | Closed by | Blocks |
| - | - | - |
| The control protocol: bounded frames, one outstanding action, a bounded evidence window, decisive records acknowledged before the child proceeds | L4 for the simulated store, L9 for SQLite | S0 |
| The choice log records elementary choices with stable identities; `max_steps` counts them on both backends | L4 | Exact replay |

## Decisions that stay open in the specification

| Decision | State |
| - | - |
| D1, expiry against uncertain emission | Open; S0 excludes expiry |
| T1, temporal guarantees of the Will | Open; S0 excludes the Will |
| Service targets of the real profiles | Not supplied; needed before G4, useful for the measurement of ADR 0008 |
