# Decision records

A record states a context, a decision and its consequences. Its status is
one of:

- **Accepted**: decided with the integration owner.
- **Proposed**: an amendment to the specification raised by the critical
  review of the launch. It binds nobody until the integration owner accepts
  it. Where lot L0 already had to choose, the record says so and names what
  changes if the proposal is refused.

The specification chapters are never edited. A record that departs from a
chapter names the requirement it amends.

| Record | Status | Subject |
| - | - | - |
| [0000](0000-repository-and-launch.md) | Accepted | Repository, layout, language, scope of the launch |
| [0001](0001-live-indeterminate-path.md) | Proposed | S0 must drive the core through an unknown outcome without a crash |
| [0002](0002-sqlite-variant-topology.md) | Proposed | Process topology and evidence of the SQLite variant |
| [0003](0003-bench-input-audit.md) | Proposed | How DEM-MU-03 is detected |
| [0004](0004-contract-revision-status.md) | Proposed | Revision 1 is strict and not frozen; deltas made by L0 |
| [0005](0005-chapter-precedence.md) | Proposed | Precedence between chapters and known differences |
| [0006](0006-plan-then-apply.md) | Proposed | Shape of a core transition |
| [0007](0007-pure-crate-boundary.md) | Proposed | How the purity of the core is enforced |
| [0008](0008-early-writer-measurement.md) | Proposed | An early, non-qualifying measurement of the single writer |
