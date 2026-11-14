# 0008 — An early, non-qualifying measurement of the single writer

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied by lot L7. Completes DEM-024.

## Context

The dominant risk of architecture R1 is the service rate of one durable
writer with `synchronous=FULL`, on Raspberry Pi storage in particular.
Chapters 2 and 3 already ask for it to be measured while the prototypes are
built: ARC-028 measures performance from the prototypes on, ARC-025 rejects
R1 when the writer cannot serve the peak load, and V3-024 defines the
method. The starting lot of chapter 4 schedules no measurement, so without
a decision the first number would arrive at gate G4.

## Decision

Lot L7 measures the writer as soon as the SQLite store exists.

- **The real schema.** The measurement runs the real `AdmitPublication`
  transaction of the store on its real tables, not a stand-in shaped like
  it.
- **The writer apart from the supervision.** The cost of the writer
  (transaction, synchronisation) is reported apart from the cost of the
  bench around it (process control, evidence, tracing). A run through the
  supervised child is not a writer measurement.
- **Exploratory.** Its manifest marks it so. It qualifies nothing: no
  threshold, no comparison with another broker, no claim about a profile.
  Its use is to learn early whether R1 is within an order of magnitude of
  the intended workloads.

## Consequences

The integration owner supplies rough service targets when available. None
is needed for lots L0 to L6. The conditions of V3-024 and V3-025 for a
qualifying measurement are unchanged.
