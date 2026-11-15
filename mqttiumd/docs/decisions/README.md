# Decision records

A record states a context, a decision and its consequences. Its status is
one of:

- **Accepted**: decided with the integration owner. The record says where
  it is applied: in lot L0, or by the lot that will apply it.
- **Proposed**: raised and not decided. It binds nobody.

The specification chapters are never edited. A record that departs from a
chapter names the requirement it amends. An identifier introduced by an
accepted record enters `contracts/v1/requirements.json` with that record as
its source; a test fails when the register and the records drift apart.

| Record | Status | Subject | Applied |
| - | - | - | - |
| [0000](0000-repository-and-launch.md) | Accepted | Repository, layout, language, scope of the launch | L0 |
| [0001](0001-live-indeterminate-path.md) | Accepted with amendments | S0 drives the core through an unknown outcome without a crash | L1, L3, L4 |
| [0002](0002-sqlite-variant-topology.md) | Accepted with amendments | Process topology and evidence of the SQLite variant | L0 in part, L4, L9 |
| [0003](0003-bench-input-audit.md) | Accepted with amendments | Provenance of the broker's inputs; detection of DEM-MU-03 | L2, L3, L4 |
| [0004](0004-contract-revision-status.md) | Accepted with amendments | Revision 1 is strict and provisional, identified by content | L0 |
| [0005](0005-chapter-precedence.md) | Accepted with amendments | Precedence between chapters and resolved differences | L0 |
| [0006](0006-plan-then-apply.md) | Accepted with amendments | Shape of a core transition | L0 in part, L1, L6 |
| [0007](0007-pure-crate-boundary.md) | Accepted with amendments | How the purity of the core is enforced | L0 |
| [0008](0008-early-writer-measurement.md) | Accepted with amendments | An early, non-qualifying measurement of the single writer | L7 |
| [0009](0009-owner-authority.md) | Accepted with amendments | Creation authority for owners and effect boxes; a durable fact is not an authorisation to construct | L1, L2, L4, L7 |
| [0010](0010-credits-interface.md) | Proposed | Credits: the order of the guards of `release_if_current`, `Account::reserve`, unchecked constructors, one record per held lease | L1, L6 |
| [0011](0011-epoch-opening.md) | Proposed | The opening of an epoch: one conditional, idempotent command with an identity; amends 0009 | L1, L2, L3, L4, L7 |
| [0012](0012-reservation-refusals.md) | Proposed | Refusals of a reservation and their wake-ups; `StepOutcome::Exhausted`; a fifth refusal of creation; amends 0006, adds to 0009 | L1, L3, L4 |
| [0013](0013-broker-behaviours-left-open.md) | Proposed | Twelve behaviours of the broker that the chapters leave open: replays on a live connection, closures, the refusal of an admission and its reason, what happens without a receipt slot | L1, L3, L4 |
| [0014](0014-recovered-facts-order.md) | Proposed | Routes and limits among the recovered facts; the order of an inventory | L1, L2, L3, L7 |
| [0015](0015-scenario-vocabulary-and-fence-point.md) | Proposed | Scenario steps of P1-02b, what a fault holds, the fence point; corrects step 3 of P1-02b in 0001 | L2, L3, L4, L5 |
| [0016](0016-evidence-formats.md) | Proposed | Trace records of the input audit, what every run owes, the perimeter of a run, the store snapshot | L2, L3, L4, L7 |
| [0017](0017-binary-tied-to-its-source.md) | Proposed | `artifact.built_from` in the run manifest: the binary of a run tied to the source it was rebuilt from; what a qualifying result establishes | L3, L4 |

What each record leaves open is tracked in
[`../reception/open-conditions.md`](../reception/open-conditions.md).
