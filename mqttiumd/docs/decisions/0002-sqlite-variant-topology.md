# 0002 — Process topology and evidence of the SQLite variant

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied in lot L0 for the scenario order rule and the record origins; by
lots L4 and L9 for the rest. Completes DEM-013, DEM-016, DEM-017, DEM-026.

## Context

Chapter 4 already requires a collector outside the broker that survives
its crash (DEM-012, DEM-016), and a supervisor outside the broker process
that kills it (DEM-013). What the chapters leave open is how a process that
will be killed is stepped, where the injection layer sits, and how decisive
evidence is known to have reached the collector before the kill.

## Decision

### Child and external supervisor

The broker under test is a child process hosting the core, the SQLite
adapter and the injection layer. The injection layer wraps the adapter and
is never inside the core. The supervisor, the collector, the simulated
clients and the peer decoder live outside the child.

### Macros and elementary choices

A scenario step is a macro: `ClientResumeSession` or `DriveUntil` expands to
several elementary choices. An elementary choice is one action of the
scheduler with a stable identity (DEM-013): deliver one input, release one
held command, deliver one result, confirm some bytes. `max_steps` counts
elementary choices, on both backends. The choice log records elementary
choices only; macros are not replayed, their expansion is.

### Bounded multiplexing

The control channel carries two flows in frames of bounded size:

- actions, from the supervisor to the child, one outstanding at a time;
- evidence, from the child to the supervisor, with a bounded window of
  records not yet acknowledged.

While the supervisor waits for an action to complete it keeps reading
evidence and acknowledging it. While the child waits for its next action it
keeps reading acknowledgements. The child stops only when its window is
full, and a decisive record (`FaultHit`, a store truth record) must be
acknowledged before the child goes past the point it describes. Neither
side ever blocks on a write that the other side must read while it is
itself blocked on a write.

### Inspection

The inspector opens the database read-only while the child is alive. No
process opens the database between the kill and the restart: recovery of
the WAL belongs to the restarted broker alone.

## Consequences

Lot L0 already refuses a scenario in which a step follows `CrashBroker`
before `RestartBroker` (`step-order-invalid`). The trace schema names the
origin of each record; ADR 0003 states why that label is a claim and how it
becomes a fact.
