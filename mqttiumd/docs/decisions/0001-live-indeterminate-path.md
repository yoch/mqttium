# 0001 — S0 must drive the core through an unknown outcome without a crash

Status: **Proposed**. Amends DEM-017 and DEM-024 (lot L4), completes DEM-015.

## Context

P1-02 as written in DEM-017 drops the commit result and crashes the broker
immediately afterwards. After the restart the admission is found through
the durable inbound slot (session, Packet Identifier 7), which is the
`AwaitPubrel` row of ARC-011. The path "result lost, process alive" is never
taken by the core:

- `ResolveAndFence` and `Deadline` have no end-to-end witness in S0;
- DEM-MU-05 (a late unknown result replacing a terminal one) has no S0
  scenario to live in;
- QABS-A, B and C exercise the store alone, not the core's reaction.

Two cells of the registry are also undecided by the chapters: what triggers
resolution on a live process, and what a replayed PUBLISH does while the
admission is `AdmissionPending` with unknown outcome.

## Decision

1. Add scenario P1-02b to lot L4 and to the S0 reception: the admission
   result is dropped, the broker does not crash, a deadline token fires, the
   core emits `ResolveAndFence`, receives `Committed`, and PUBREC is observed
   on the same connection. One admission, same obligations.
2. The trigger of resolution is a deadline armed with the `Persist` effect.
   Time stays explicit data.
3. A PUBLISH replayed for the same exchange while the outcome is unknown
   creates no command and no acknowledgement; it is absorbed and the pending
   resolution continues.
4. The oracle role writes these rows into `contracts/v1/p1.transitions.json`
   and removes the corresponding holes.

## Consequences

`P1-02b` is not in the requirement register until this record is accepted.
If it is refused, S0 still passes as specified, and the live reconciliation
path is first exercised by the full P1 corpus (P1-05, P1-10).
