# Open conditions

What lot L0 leaves open on purpose. Each condition names the lot that closes
it and the gate or reception it blocks. A lot that closes one removes its
row in the same change; a lot that finds a new one adds it.

Nothing here is satisfied by the present tree.

## Audit of the first wave (head `0daf0eb`)

Defects found by the audit, each with a counterexample that the tests of
the lots did not contain. A row is removed by the change that closes it,
with its failing test first.

| Finding | Reception | Closed by | Blocks |
| - | - | - | - |
| **F3, progress of resolutions: what is left.** Corrected in the core (lot L1, `2255bf7`): an operation retains its command, its two latest resolutions and its four latest landmark resolutions, those whose rank is a power of two, so that an answer delayed by any finite number of resolutions meets a landmark still retained; at most seven effect identifiers per operation; the invariants are in the documentation of `Answers`. Played by the runner as a judged scenario, `tests/scenarios/audit/F3-steady-lag.json`: the result of the Persist is lost, the answer of the first resolution is delivered after six newer ones, and the exchange completes; with the landmarks removed by mutation the run stops before the PUBREC and the oracle answers INCONCLUSIVE. Left: the scenario delays one answer by a bounded number of resolutions, because the vocabulary cannot express a steady delay without fixing in advance the delivery at which the core progresses (a `DriveUntil` on a set of events, or a fault expected at most once); a run that stops with nothing eligible and its awaited event not reached is INCONCLUSIVE, not FAIL; the correspondence of the retention rule with the model M1 | L3 for the vocabulary and the verdict of a blocked run; L1 with L5 for the model | G0 for the verdict; G1 for the model |

## Before a result may be used as evidence (L3, gate G0)

`mqttiumd-lab validate` checks a qualifying result against the files it
names: digests, contract digest, assertions and faults of the scenario,
verdict equal to the one its assertions determine. It does not check that
the assertions are true.

| Condition | Closed by | Blocks |
| - | - | - |
| **Core, limits of what it remembers** (lot L1, corrections of the review of its core). An operation that ended is kept in a bounded window (`OwnerConfig::retained_operations`); beyond it a contradicting result is absorbed. A connection the owner closed is kept in a bounded window (`remembered_connections`); beyond it what the connection still carries is no longer known as stale. Lot L4, which assigns connection generations, decides whether "older than the current one" closes the second limit | L1, L4 | S0 |
| **Store, limits of the deterministic store** (lot L2, head `35bec750`). It clones its whole durable state per command; the records of terminal sessions stay, bounded by the session counter and not by `max_sessions`; the receipts and fences of epoch 0 are never collected; a recovered limit that does not fit its field, and a route to a session that is not active, are reported as `AllocationFailed`. Stated limits of the deterministic store; the SQLite store states its own | L7 | S0 on SQLite |
| **Store and formats, what is left.** No trace record exists for a `RecipeMismatch` violation: the result is written at neither end, so the chain is absent, not half present. At `store.fence.transit` a fault on a resolution selects every resolution, because the adapter gives a resolution no source: enough while a scenario has one exchange. No semantic rule refuses a fence fault that names a command no earlier fault selected | L3 for the rules; L2 with L4 for the selection | G0 |
| **Store, the read `DurableStore::established`.** The fence point needs a read that writes nothing; the SQLite store realises it and passes the same contract tests | L7 | S0 on SQLite |
| **Oracle, what is left of lot L3** (final head `d86adb0`; the integrator read the corrections of findings 1 and 4 of the review of `3c023bf`, the registry change and the changed test of lot L2, not the rest). No bench plays a scenario: the oracle judged only traces written by hand. The list of contract files is still computed twice, by `manifest.rs` and by `Workspace::open`: both refuse a file of the schema directory that is not a schema, and one list read once needs a change of the shared `manifest.rs` that lot L3 proposed and that is not applied. The Python reader keeps its own copy of the order of recovered facts, on purpose, tied to the Rust reader by twelve shared cases. The registry has no guard on exhausted identities and no session machine. A `ClientBytesReceived` carries one whole packet in v1 | L4 for the bench; L3 with L1 for the guard; integrator for `manifest.rs` | G0 |
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
| The lab binary, the oracle and the generators are identified separately in the manifest (V3-002). The schema `run-manifest` has the two optional members `oracle` and `generators` (lot L3); the runner still writes these identities in its own `campaign.json` and must write them in the manifest | L4 | G0 |
| **One snapshot of the contracts.** The contract digest is computed from one read of the contract files and the schemas and catalogues are then read again; a file that changes in between leaves a digest that names other rules than the ones loaded. Either the bytes that are hashed are the bytes that are interpreted, or the snapshot of a campaign is established as immutable. L3: the readers interpret the bytes they hashed. L4: the snapshot of a campaign. The row stays until both parts are done | L3, L4 | Any qualifying result |

## Effect boxes and transitions (ADR 0006): reception of L1

Lot L1 is not received while a condition it closes is open. The creation
authority is decided by record 0009 and split by lot: no part is closed by
another lot's part.

| Condition | Closed by | Blocks |
| - | - | - |
| **Creation authority, the real path** (ADR 0009). Done for the bench: `Process::start` of the runner opens an epoch, takes the grant, builds the authority with `OwnerAuthority::from_grant`, creates the box and the owner, and `bench_composition.rs` shows through that path that a second box for the active owner is refused, before and after a simulated crash and a new epoch. Left: the same for the start-up of the broker binary, which does not exist yet; the authority of the bench holds one owner | The lot that writes the broker binary | S1 |
| **Creation authority on SQLite** (ADR 0009). The opening of an epoch and its results realised on the SQLite store, with the same contract tests | L7 | S0 on SQLite |

## Pilot proof (DEM-021): reception of L6

| Condition | Closed by | Blocks |
| - | - | - |
| The proof that is received is on `release_if_current` as the product really executes it. Established at the final head of lot L6 (`709c887`): the proof is on the production body, without a copy; the product calls it at one site, `LeaseTable::release` in `crates/mqttiumd-core/src/leases.rs`, and a test of lot L1 counts that site; the job `pilot proof` was read green on that revision by lot L6 and on the integration branch by the integrator. Whether this receives the lot is the owner's decision | Owner of the repository | Reception of L6 |
| The precondition of the proof, one record per held lease, is not proved and the function cannot check it: with two records of one lease the usage is under-counted (harness p12). The lease table of lot L1 carries it, backed by a test that counts no `from_parts` and one `release_if_current` in the product, not by a proof. The proof covers the function alone, neither `LeaseTable` nor recovery, and rests on Kani, CBMC and the nightly compiler Kani brings | Lot that proves the lease table, not planned in this wave | P1 proofs |

## Model M1: reception of L5, then L4

| Condition | Closed by | Blocks |
| - | - | - |
| The sidecar of a model lists as `tests` only files that exist and replay one of its counterexamples. Until a deterministic test replays a counterexample of M1, the list stays empty and the correspondence table names, row by row, the lot that owes the replay | L4 for the replays; the table is written (lot L5, final head `3d6f562`) | G1 |
| **M1, what its campaign does not establish** (closing report of lot L5). Liveness is claimed on one publication only: on two publications no run of the present module completed. M1b is checked by groups of fault kinds: the run with every kind at once was interrupted at 14.1 million distinct states. M1b has no liveness property. The atomicity of `ApplyCommit` is an assumption the SQLite tests discharge. The fence of both models follows record 0015, and their `Refuse` differs from the registry (record 0013, line 12): both records are proposed | Owner of the repository for the two records; L7 for the atomicity; the bounds stay stated limits | G1 |

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
