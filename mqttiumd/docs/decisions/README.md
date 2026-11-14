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
| [0009](0009-owner-authority.md) | Proposed | Creation authority for owners and effect boxes | L1, once decided |

What each record leaves open is tracked in
[`../reception/open-conditions.md`](../reception/open-conditions.md).
