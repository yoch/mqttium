# 0003 — How DEM-MU-03 is detected

Status: **Proposed**. Completes DEM-012 and DEM-018.

## Context

DEM-MU-03 injects into the bench the fault "re-inject the `OperationId` the
bench remembered during recovery". A broker that receives `op_A` from the
bench and a broker that recovers it from the store behave identically, so
no assertion on packets or durable state can fail for this mutant. DEM-018
requires each mutant to be detected for its own reason.

## Decision

Detection is an audit of the broker's inputs, not of its behaviour. After a
restart the input channel of the broker admits, by type, only client bytes,
control choices and pages read from the store. Every input is recorded with
its origin. The oracle fails the run when an identity appears in a broker
input before the broker itself produced it from a recovery page in the same
epoch. The mutant must violate this audit.

## Consequences

The control channel of ADR 0002 has no message able to carry an identity
towards the broker. The trace format gains an input-audit record with lot
L3. A scenario binding stays a name known to the bench and the oracle only.
