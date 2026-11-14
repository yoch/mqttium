# 0008 — An early, non-qualifying measurement of the single writer

Status: **Proposed**. Completes ARC-025, ARC-028 and DEM-024 (lot L7).

## Context

The dominant risk of architecture R1 is the service rate of one durable
writer with `synchronous=FULL`, on a Raspberry Pi storage in particular.
ARC-028 asks for performance to be measured from the prototypes on, and
ARC-025 rejects R1 when the writer cannot serve the peak load. Chapter 4
measures nothing before gate G4, after P1, P2 and P3 are built, and no
service target is filled in.

## Decision

Lot L7 adds one exploratory measurement of a transaction shaped like
`AdmitPublication` (one publication, N obligations, one inbound slot, one
receipt): commit latency and the effect of grouping, on the hardware
available. Its manifest marks it exploratory.

It qualifies nothing: no threshold, no comparison with another broker, no
claim about a profile. Its only use is to learn early whether R1 is within
an order of magnitude of the intended workloads.

## Consequences

The integration owner supplies rough service targets when available. None
is needed for lots L0 to L6. If the proposal is refused, the first writer
measurement stays at gate G4.
