# 0002 — Process topology and evidence of the SQLite variant

Status: **Proposed**. Completes DEM-013, DEM-016, DEM-017 and DEM-026.

## Context

DEM-017 requires on SQLite the same named injection (`store.result.ready`),
a barrier that suspends the operation, step-driven progress and a read-only
inspection. DEM-013 requires that a supervisor outside the broker process
kills it. The chapters do not say how a killed process is stepped, where
the injection layer sits, or how evidence survives the kill. Trace records
still buffered in the killed process would be lost, and the run would be
inconclusive by construction.

## Decision

- The broker under test is a child process hosting the core, the SQLite
  adapter and the injection layer. The injection layer wraps the adapter; it
  is never inside the core.
- The supervisor drives the child over a control channel, one scheduler
  choice per message. A step of a scenario means one such choice on both
  backends.
- Decisive evidence is write-through: the child sends the record and waits
  for the supervisor's acknowledgement before it continues. `FaultHit` is
  therefore durable in the collector before the barrier holds.
- The inspector opens the database read-only while the child is alive. No
  process opens the database between the kill and the restart: recovery of
  the WAL belongs to the restarted broker alone.
- The controlled transport of the child is carried by the same channel;
  the peer decoder runs in the supervisor.

## Consequences

Lot L0 already encodes two of these points: a scenario in which a step
follows `CrashBroker` before `RestartBroker` is refused
(`step-order-invalid`), and trace records name their origin so that hidden
store truth (`store-evidence`) cannot be stated by the core. If the proposal
is refused, the `step-order-invalid` rule for `InspectDurableState` is the
part to revisit.
