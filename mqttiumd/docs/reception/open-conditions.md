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
| The collector assigns the origin of a record from its channel and refuses a record that claims another (ADR 0003) | L3, L4 | G0 |
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

## Scenario P1-02b (ADR 0001)

| Condition | Closed by | Blocks |
| - | - | - |
| The scenario format gains the steps P1-02b needs: firing a deadline, releasing a held command, delivering a held result; as a contract change with examples | L3 | P1-02b |
| The registry rows for a replayed PUBLISH while the outcome is unknown, after a reconnect and on a live connection | L3 | P1-02b, G0 |
| `tests/scenarios/p1/P1-02b.json` and its run on the deterministic store | L4 | S0 |

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
