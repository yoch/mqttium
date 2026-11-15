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
| The oracle recomputes every assertion from the trace, the snapshots and the decoded packets, and a result whose statuses differ is refused | L3 | G0 |
| The obligations of a run are derived from its exact scenario and perimeter; a run on a sub-perimeter (core without bytes) has its own declared obligations and never passes the full scenario | L3 | G0 |
| A trace is checked as a whole: header first, gapless sequence per origin, causal parents present, footer count and completeness | L3 | G0 |
| The collector assigns the origin of a record from its channel and refuses a record that claims another (ADR 0003). L3: the rule, and the refusal of a trace that breaks it. L4: the collector that assigns the origin. The row stays until both parts are done | L3, L4 | G0 |
| A PASS requires a registry whose status is `complete`; the 39 uncovered cells of `p1.transitions.json` are written by the oracle role | L3 | G0 |
| The assertion `broker_inputs_have_provenance` and the trace records it needs join the contracts (ADR 0003) | L3 | S0, DEM-MU-03 |
| A permanent second reader of the semantic rules exists; today only the schema level has one (`tools/check_schemas.py`) | L3 | G0 |

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
| The scenario format gains the steps P1-02b needs: firing a deadline, releasing a held command, delivering a held result; as a contract change with examples | L3 | P1-02b |
| The registry rows for a replayed PUBLISH while the outcome is unknown, after a reconnect and on a live connection | L3 | P1-02b, G0 |
| `tests/scenarios/p1/P1-02b.json` and its run on the deterministic store | L4 | S0 |

## Effect boxes and transitions (ADR 0006): reception of L1

Lot L1 is not received while a condition it closes is open. The creation
authority is decided by record 0009 and split by lot: no part is closed by
another lot's part.

| Condition | Closed by | Blocks |
| - | - | - |
| **Owner counters** (ADR 0009, record 0012 proposed). Replacing a box resets no counter of its owner; the constructor of an owner refuses a box smaller than the largest reservation a transition makes; an exhausted identity counter is reported as `Exhaustion::IdentityCounter`. All three come with the state of an owner in `mqttiumd-core` | L1 | Reception of L1 |
| **Creation authority, its issuance** (ADR 0009). A durable fact is not an authorisation to construct. The contract of opening an epoch and the hand-over give at most one authority per live opening: none for a duplicated reply, a replayed fact or a further recovery page, none usable for an unknown outcome | L2, with L1 | Reception of L2 |
| **Epoch opening, after a fault** (record 0011, proposed; fifth review of the contract of L2 at `7941f334`). (1) A faulted opening drops every later reply, so a `Superseded` that arrives after the fault never marks a handed-over grant closed, and `check_recovered` answers by the order of the replies. (2) `AttemptReused` is taken for a command whose condition is epoch 0, which no ledger reports; the exhaustive test does not use that condition. (3) `observe` reports `Unchanged` when a reply raises the lowest value a later read may return without moving the stage. (4) The documentation of `OpeningStage::Rejected` says the first reason is kept, while a definitive reason replaces it | L2 | Reception of L2 |
| **Creation authority, the real path** (ADR 0009). The trusted entry point that constructs an authority is stated; start-up is wired through it, and composition tests go through that path, not through a helper that builds an authority directly | L4 | S0 |
| **Creation authority on SQLite** (ADR 0009). The opening of an epoch and its results realised on the SQLite store, with the same contract tests | L7 | S0 on SQLite |
| **Transition contract.** `PlanStamp` is a support for validation, not yet the private single-use plan of ADR 0006. L1 establishes: no mutation while planning; a plan consumed once; owner and revision checked before application; resources reserved before mutation; the exact behaviour of `NeedCapacity`. Reserving then releasing an entry advances its generation: L1 orders its acquisitions so that a refused transition restores what the contract requires. Any provisional metadata allowed to differ needs an explicit amendment of the contract, by a decision record; a report cannot grant it | L1 | Reception of L1 |

## Pilot proof (DEM-021): reception of L6

| Condition | Closed by | Blocks |
| - | - | - |
| The proof that is received is on `release_if_current` as the product really executes it. Tooling may be prepared in parallel; a proof of a separate demonstration function does not receive the lot | L6, with L1 | Reception of L6 |
| The lease table holds each `LeaseRecord` exactly once: one record per held lease. Recovery calls `LeaseRecord::from_parts` once per durable row and nothing else calls it outside the proof crate. A test of the table shows that no path builds a second record of a held lease (record 0010, proposed) | L1 | Reception of L6 |

## Model M1: reception of L5, then L4

| Condition | Closed by | Blocks |
| - | - | - |
| The sidecar of a model lists as `tests` only files that exist and replay one of its counterexamples. Until a deterministic test replays a counterexample of M1, the list stays empty and the correspondence table names, row by row, the lot that owes the replay | L5 for the table, L4 for the replays | G1 |
| The `mqttiumd` workflow runs the models of `mqttiumd/models/` with the client's TLC runner and its `--models-dir` option; a runner change runs them again | L5, through the integration owner | Reception of L5 |

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
